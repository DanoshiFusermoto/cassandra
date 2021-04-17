package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.common.Order;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.executors.ScheduledExecutable;
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.messages.AssociationSearchQueryMessage;
import org.fuserleer.ledger.messages.AssociationSearchResponseMessage;
import org.fuserleer.ledger.messages.StateSearchQueryMessage;
import org.fuserleer.ledger.messages.StateSearchResponseMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.filters.StandardPeerFilter;
import org.fuserleer.serialization.SerializationException;

/**
 * Implementation of ledger search service for Indexables and Identifiers
 * 
 * Currently only allows searches for committed elements. 
 *  
 * @author Dan
 *
 */
class LedgerSearch implements Service, LedgerInterface
{
	private static final Logger searchLog = Logging.getLogger("search");

	private final Context 	context;
	
	private final class StateSearchTask extends ScheduledExecutable
	{
		private final StateSearchQuery query;
		private final CompletableFuture<SearchResult> queryFuture;
		
		public StateSearchTask(StateSearchQuery query, long delay, TimeUnit unit)
		{
			super(delay, 0, unit);
			
			this.query = Objects.requireNonNull(query, "State search query is null");
			this.queryFuture = new CompletableFuture<SearchResult>();
		}
		
		public void response(StateSearchResponse response)
		{
			if (LedgerSearch.this.stateSearchTasks.remove(this.query.getHash(), this) == false)
				return;

			if (this.queryFuture.isDone() == true)
				return;
			
			this.queryFuture.complete(response.getResult());
		}
		
		public StateSearchQuery getQuery()
		{
			return this.query;
		}

		public Future<SearchResult> getQueryFuture()
		{
			return this.queryFuture;
		}

		@Override
		public void execute()
		{
			if (LedgerSearch.this.stateSearchTasks.remove(this.query.getHash(), this) == false)
				return;
			
			if (this.queryFuture.isDone() == true)
				return;
			
			this.queryFuture.completeExceptionally(new TimeoutException("State search query timeout "+this.query));
		}
	}
	
	private final class AssociationSearchTask extends ScheduledExecutable
	{
		private final AssociationSearchQuery query;
		private final Map<Long, AssociationSearchResponse> responses;
		private final CompletableFuture<AssociationSearchResponse> queryFuture;

		public AssociationSearchTask(AssociationSearchQuery query, long delay, TimeUnit unit)
		{
			super(delay, 0, unit);
			
			this.query = Objects.requireNonNull(query, "Association search query is null");
			this.responses = Collections.synchronizedMap(new HashMap<Long, AssociationSearchResponse>());
			this.queryFuture = new CompletableFuture<AssociationSearchResponse>();
		}
		
		public void response(long shardGroup, AssociationSearchResponse response)
		{
			this.responses.put(shardGroup, response);
			if (this.responses.size() < LedgerSearch.this.context.getLedger().numShardGroups())
				return;
			
			build();
		}
		
		public AssociationSearchQuery getQuery()
		{
			return this.query;
		}

		public Future<AssociationSearchResponse> getQueryFuture()
		{
			return this.queryFuture;
		}
		
		@Override
		public void execute()
		{
			if (LedgerSearch.this.associationSearchTasks.remove(this.query.getHash(), this) == false)
				return;
			
			if (this.queryFuture.isDone() == true)
				return;
			
			// TODO return partials?
			this.queryFuture.completeExceptionally(new TimeoutException("Association search query timeout "+this.query));
		}
		
		// FIXME deal with the fact that associations to a primitive can live on multiple shards @ different index heights.
		//		 Index heights are used to define the next offset, which may result in the same primitive being returned from 
		//		 the non-index shard group, or omitting some primitives altogether.
		//		 e.g A is @100 on SG1, A is @90 on SG2 with B @95 on SG2.  Using 100 as next offset may omit B on SG2 if relevant.
		private void build()
		{
			List<SearchResult> sortedResults = new ArrayList<SearchResult>();

			synchronized(this.responses)
			{
				for (AssociationSearchResponse response : this.responses.values())
					sortedResults.addAll(response.getResults());
			}
			
			sortedResults.sort(new Comparator<SearchResult>() 
			{
				@Override
				public int compare(SearchResult arg0, SearchResult arg1)
				{
					if (arg0.getCommit().getIndex() < arg1.getCommit().getIndex())
						return -1;
					
					if (arg0.getCommit().getIndex() > arg1.getCommit().getIndex())
						return 1;
					
					return 0;
				}
			});
			
			long nextOffset = -1;
			Map<Primitive, SearchResult> sortedResultsMap = new LinkedHashMap<Primitive, SearchResult>();
			for (SearchResult searchResult : sortedResults)
			{
				try
				{
					if (sortedResultsMap.containsKey(searchResult.getPrimitive()) == true)
						continue;
					
					if (nextOffset == -1 || 
						(this.query.getOrder().equals(Order.ASCENDING) && searchResult.getCommit().getIndex() > nextOffset) ||
						(this.query.getOrder().equals(Order.DESCENDING) && searchResult.getCommit().getIndex() < nextOffset))
						nextOffset = searchResult.getCommit().getIndex();
					
					sortedResultsMap.put(searchResult.getPrimitive(), searchResult);
				}
				catch (SerializationException ex)
				{
					searchLog.error(LedgerSearch.this.context.getName()+": Failed to deserialize primitive in search result "+searchResult);
				}
			}
			
			AssociationSearchResponse response = new AssociationSearchResponse(this.query, nextOffset, sortedResultsMap.values(), sortedResultsMap.size() < query.getLimit());
			this.queryFuture.complete(response);
		}
	}

	private final Map<Hash, StateSearchTask> stateSearchTasks = Collections.synchronizedMap(new LinkedHashMap<Hash, StateSearchTask>());
	private final Map<Hash, AssociationSearchTask> associationSearchTasks = Collections.synchronizedMap(new LinkedHashMap<Hash, AssociationSearchTask>());
	private final ExecutorService searchExecutor = Executors.newFixedThreadPool(1); // TODO want more than one? Also need a factory 
	
	public LedgerSearch(Context context)
	{
		this.context = Objects.requireNonNull(context);

//		searchLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
		searchLog.setLevels(Logging.ERROR | Logging.FATAL);
	}

	@Override
	public void start() throws StartupException
	{
		this.context.getNetwork().getMessaging().register(StateSearchQueryMessage.class, this.getClass(), new MessageProcessor<StateSearchQueryMessage>()
		{
			@Override
			public void process(final StateSearchQueryMessage stateSearchQueryMessage, final ConnectedPeer peer)
			{
				LedgerSearch.this.searchExecutor.execute(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							StateSearchResponse stateSearchResponse = LedgerSearch.this.doStateSearch(stateSearchQueryMessage.getQuery());
							StateSearchResponseMessage stateSearchResponseMessage = new StateSearchResponseMessage(stateSearchResponse); 
							peer.send(stateSearchResponseMessage);
						}
						catch (Exception ex)
						{
							searchLog.error(LedgerSearch.this.context.getName()+": ledger.messages.search.indexable.request " + peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(StateSearchResponseMessage.class, this.getClass(), new MessageProcessor<StateSearchResponseMessage>()
		{
			@Override
			public void process(final StateSearchResponseMessage stateSearchResponseMessage, final ConnectedPeer peer)
			{
				LedgerSearch.this.searchExecutor.execute(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (searchLog.hasLevel(Logging.DEBUG) == true)
								searchLog.debug(LedgerSearch.this.context.getName()+": Search response for state "+stateSearchResponseMessage.getResponse().getQuery()+" returning "+stateSearchResponseMessage.getResponse().getResult()+" from " + peer);
							
							final StateSearchTask stateSearchTask = LedgerSearch.this.stateSearchTasks.get(stateSearchResponseMessage.getResponse().getQuery().getHash());
							if (stateSearchTask == null)
							{
								searchLog.error(LedgerSearch.this.context.getName()+": Received response for state "+stateSearchResponseMessage.getResponse().getQuery()+" not found or completed from "+peer);
								return;
							}
								
							stateSearchTask.response(stateSearchResponseMessage.getResponse());
						}
						catch (Exception ex)
						{
							searchLog.error(LedgerSearch.this.context.getName()+": ledger.messages.search.indexable.response " + peer, ex);
						}
					}
				});
			}
		});
		
		this.context.getNetwork().getMessaging().register(AssociationSearchQueryMessage.class, this.getClass(), new MessageProcessor<AssociationSearchQueryMessage>()
		{
			@Override
			public void process(final AssociationSearchQueryMessage associationSearchQueryMessage, final ConnectedPeer peer)
			{
				LedgerSearch.this.searchExecutor.execute(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							AssociationSearchResponse associationSearchResponse = LedgerSearch.this.doAssociationSearch(associationSearchQueryMessage.getQuery());
							AssociationSearchResponseMessage associationSearchResponseMessage = new AssociationSearchResponseMessage(associationSearchResponse); 
							peer.send(associationSearchResponseMessage);
						}
						catch (Exception ex)
						{
							searchLog.error(LedgerSearch.this.context.getName()+": ledger.messages.search.query.association " + peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(AssociationSearchResponseMessage.class, this.getClass(), new MessageProcessor<AssociationSearchResponseMessage>()
		{
			@Override
			public void process(final AssociationSearchResponseMessage associationSearchResponseMessage, final ConnectedPeer peer)
			{
				LedgerSearch.this.searchExecutor.execute(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (searchLog.hasLevel(Logging.DEBUG) == true)
								searchLog.debug(LedgerSearch.this.context.getName()+": Search response for association "+associationSearchResponseMessage.getResponse().getQuery()+" returning "+associationSearchResponseMessage.getResponse().getResults().size()+" results from " + peer);
							
							final AssociationSearchTask associationSearchTask = LedgerSearch.this.associationSearchTasks.get(associationSearchResponseMessage.getResponse().getQuery().getHash());
							if (associationSearchTask == null)
							{
								searchLog.error(LedgerSearch.this.context.getName()+": Received response for association "+associationSearchResponseMessage.getResponse().getQuery()+" not found or completed from "+peer);
								return;
							}
							
							associationSearchTask.response(ShardMapper.toShardGroup(peer.getNode().getIdentity(), LedgerSearch.this.context.getLedger().numShardGroups()), associationSearchResponseMessage.getResponse());
						}
						catch (Exception ex)
						{
							searchLog.error(LedgerSearch.this.context.getName()+": ledger.messages.search.response.association " + peer, ex);
						}
					}
				});
			}
		});

	}

	@Override
	public void stop() throws TerminationException
	{
		// TODO Auto-generated method stub
		
	}
	
	StateSearchResponse doStateSearch(final StateSearchQuery query) throws IOException
	{
		if (searchLog.hasLevel(Logging.DEBUG) == true)
			searchLog.debug(LedgerSearch.this.context.getName()+": Search for state "+query.getKey()+" returning "+query.getType());
		
		StateSearchResponse stateSearchResponse;
		Commit commit = LedgerSearch.this.context.getLedger().getLedgerStore().search(query.getKey());
		if (commit == null)
		{
			stateSearchResponse = new StateSearchResponse(query);
		}
		else
		{
			Primitive primitive = doGet(commit, query.getType());
			if (primitive == null)
				stateSearchResponse = new StateSearchResponse(query);
			else
				stateSearchResponse = new StateSearchResponse(query, commit, primitive, query.getType());
		}

		return stateSearchResponse;
	}

	AssociationSearchResponse doAssociationSearch(final AssociationSearchQuery query) throws IOException
	{
		if (searchLog.hasLevel(Logging.DEBUG) == true)
			searchLog.debug(LedgerSearch.this.context.getName()+": Search for Association "+query+" returning "+query.getType());
		
		List<SearchResult> localResults = new ArrayList<SearchResult>();
		Collection<Commit> localCommits = LedgerSearch.this.context.getLedger().getLedgerStore().search(query);
		if (localCommits != null)
		{
			long nextOffset = query.getOffset();
			for (Commit commit : localCommits)
			{
				nextOffset = commit.getIndex();
				Primitive primitive = doGet(commit, query.getType());
				localResults.add(new SearchResult(commit, primitive, query.getType()));
			}

			return new AssociationSearchResponse(query, nextOffset, localResults, localCommits.size() < query.getLimit());
		}
		else
			return new AssociationSearchResponse(query);
	}

	@Override
	public Future<SearchResult> get(final StateSearchQuery query)
	{
		try
		{
			long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups());
			long searchShardGroup = ShardMapper.toShardGroup(query.getKey().get(), this.context.getLedger().numShardGroups());
			if (localShardGroup == searchShardGroup)
			{
				Commit commit = LedgerSearch.this.context.getLedger().getLedgerStore().search(query.getKey());
				if (commit != null)
				{
					Primitive primitive = doGet(commit, query.getType());
					return CompletableFuture.completedFuture(new SearchResult(commit, primitive, query.getType()));
				}
			}
			else	
			{
				synchronized(this.stateSearchTasks)
				{
					StateSearchTask stateSearchTask = this.stateSearchTasks.get(query.getHash());
					if (stateSearchTask == null)
					{					
						StandardPeerFilter standardPeerFilter = StandardPeerFilter.build(this.context).setStates(PeerState.CONNECTED).setShardGroup(searchShardGroup).setSynced(true);
						Collection<ConnectedPeer> connectedStatePeers = this.context.getNetwork().get(standardPeerFilter);
						if (connectedStatePeers.isEmpty() == true)
							throw new IOException(this.context.getName()+": No peers available to query state "+query.getKey()+" @ shard group "+searchShardGroup);
						
						stateSearchTask = new StateSearchTask(query, 5, TimeUnit.SECONDS); 
						this.stateSearchTasks.put(stateSearchTask.getQuery().getHash(), stateSearchTask);
						Executor.getInstance().schedule(stateSearchTask);
					
		    			for (ConnectedPeer connectedPeer : connectedStatePeers)
						{
							try
							{
								StateSearchQueryMessage stateSearchQueryMessage = new StateSearchQueryMessage(query);
								this.context.getNetwork().getMessaging().send(stateSearchQueryMessage, connectedPeer);
								break;
							}
							catch (IOException ex)
							{
								searchLog.error(this.context.getName()+": Unable to send StateSearchQueryMessage for "+query.getKey()+" to " + connectedPeer, ex);
							}
						}
					}
					
					return stateSearchTask.getQueryFuture();
				}
			}
			
			return CompletableFuture.completedFuture(null);
		}
		catch (Throwable t)
		{
			CompletableFuture<SearchResult> future = new CompletableFuture<>();
			future.completeExceptionally(t);
			return future;
		}			
	}
	
	@Override
	public Future<AssociationSearchResponse> get(final AssociationSearchQuery query, final Spin spin)
	{
		long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups());
		try
		{
			synchronized(this.associationSearchTasks)
			{
				AssociationSearchTask associationSearchTask = this.associationSearchTasks.get(query.getHash());
				if (associationSearchTask == null)
				{					
					associationSearchTask = new AssociationSearchTask(query, 5, TimeUnit.SECONDS);
					this.associationSearchTasks.put(associationSearchTask.getQuery().getHash(), associationSearchTask);
					Executor.getInstance().schedule(associationSearchTask);

					// Local search
					List<SearchResult> localResults = new ArrayList<SearchResult>();
					Collection<Commit> localCommits = LedgerSearch.this.context.getLedger().getLedgerStore().search(query);
					if (localCommits != null)
					{
						long nextOffset = query.getOffset();
						for (Commit commit : localCommits)
						{
							nextOffset = commit.getIndex();
							Primitive primitive = doGet(commit, query.getType());
							localResults.add(new SearchResult(commit, primitive, query.getType()));
						}

						associationSearchTask.response(localShardGroup, new AssociationSearchResponse(query, nextOffset, localResults, localCommits.size() < query.getLimit()));
					}
					
					// Remote search
					// TODO brute force search of all shard groups ... inefficient
					for (long searchShardGroup = 0 ; searchShardGroup < this.context.getLedger().numShardGroups() ; searchShardGroup++)
					{
						if (searchShardGroup == localShardGroup)
							continue;
						
						StandardPeerFilter standardPeerFilter = StandardPeerFilter.build(this.context).setStates(PeerState.CONNECTED).setShardGroup(searchShardGroup).setSynced(true);
						Collection<ConnectedPeer> connectedPeers = this.context.getNetwork().get(standardPeerFilter);
						if (connectedPeers.isEmpty() == true)
						{
							searchLog.error(this.context.getName()+": No peers available to query associations "+query+" @ shard group "+searchShardGroup);
							continue;
						}
					
						for (ConnectedPeer connectedPeer : connectedPeers)
						{
							try
							{
								AssociationSearchQueryMessage associationSearchQueryMessage = new AssociationSearchQueryMessage(query);
								this.context.getNetwork().getMessaging().send(associationSearchQueryMessage, connectedPeer);
								break;
							}
							catch (IOException ex)
							{
								searchLog.error(this.context.getName()+": Unable to send AssociationSearchQueryMessage for "+query+" to " + connectedPeer, ex);
							}
						}
					}
				}

				return associationSearchTask.getQueryFuture();
			}
		}
		catch (Throwable t)
		{
			CompletableFuture<AssociationSearchResponse> future = new CompletableFuture<>();
			future.completeExceptionally(t);
			return future;
		}
	}

	@SuppressWarnings("unchecked")
	private <T extends Primitive> T doGet(final Commit commit, Class<? extends Primitive> type) throws IOException
	{
		if (Block.class.isAssignableFrom(type) == true)
		{
			Block block = this.context.getLedger().getLedgerStore().get(commit.getPath().get(Elements.BLOCK), Block.class);
			if (block == null)
				throw new IllegalStateException("Found state commit but unable to locate block");

			return (T) block;
		}
		else if (BlockHeader.class.isAssignableFrom(type) == true)
		{
			BlockHeader blockHeader = this.context.getLedger().getLedgerStore().get(commit.getPath().get(Elements.BLOCK), BlockHeader.class);
			if (blockHeader == null)
				throw new IllegalStateException("Found state commit but unable to locate block header");

			return (T) blockHeader;
		}
		else if (Atom.class.isAssignableFrom(type) == true)
		{
			Atom atom = this.context.getLedger().getLedgerStore().get(commit.getPath().get(Elements.ATOM), Atom.class);
			if (atom == null)
				throw new IllegalStateException("Found state commit but unable to locate atom");

			return (T) atom;
		}
		else if (AtomCertificate.class.isAssignableFrom(type) == true)
		{
			if (commit.getPath().get(Elements.CERTIFICATE) == null)
				return null;
			
			AtomCertificate certificate = this.context.getLedger().getLedgerStore().get(commit.getPath().get(Elements.CERTIFICATE), AtomCertificate.class);
			if (certificate == null)
				throw new IllegalStateException("Found state commit but unable to locate certificate");

			return (T) certificate;
		}
		else if (Particle.class.isAssignableFrom(type) == true)
		{
			Atom atom = this.context.getLedger().getLedgerStore().get(commit.getPath().get(Elements.ATOM), Atom.class);
			if (atom == null)
				throw new IllegalStateException("Found state commit but unable to locate atom");

			return atom.getParticle(commit.getPath().get(Elements.PARTICLE));
		}
		
		return null;
	}
}
