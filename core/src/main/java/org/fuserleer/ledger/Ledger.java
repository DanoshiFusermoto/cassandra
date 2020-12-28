package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.Universe;
import org.fuserleer.common.Match;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.Fields;
import org.fuserleer.database.Identifier;
import org.fuserleer.database.Indexable;
import org.fuserleer.events.EventListener;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.ParticleCertificate;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomCommittedEvent;
import org.fuserleer.ledger.events.AtomDiscardedEvent;
import org.fuserleer.ledger.events.AtomErrorEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomPersistedEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.messages.GetAtomPoolMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.events.PeerConnectedEvent;
import org.fuserleer.node.Node;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;

public final class Ledger implements Service, LedgerInterface
{
	private static final Logger ledgerLog = Logging.getLogger("ledger");

	private final Context 	context;
	
	private final AtomPool 		atomPool;
	private final AtomHandler 	atomHandler;

	private final StatePool 	statePool;
	private final StateHandler 	stateHandler;
	private final StateAccumulator	stateAccumulator;

	private final BlockHandler 	blockHandler;
	private final SyncHandler 	syncHandler;

	private final LedgerStore 	ledgerStore;
	private final VoteRegulator voteRegulator;
	
	private final transient AtomicReference<BlockHeader> head;
	
	private final Map<Hash, AtomFuture> atomFutures = Collections.synchronizedMap(new LinkedHashMap<Hash, AtomFuture>());

	public Ledger(Context context)
	{
		this.context = Objects.requireNonNull(context);

		this.ledgerStore = new LedgerStore(this.context);

		this.voteRegulator = new VoteRegulator(this.context);
		this.blockHandler = new BlockHandler(this.context, this.voteRegulator);
		this.syncHandler = new SyncHandler(this.context);
		this.atomPool = new AtomPool(this.context);
		this.atomHandler = new AtomHandler(this.context);
		this.stateAccumulator = new StateAccumulator(this.context, this.ledgerStore);
		this.stateHandler = new StateHandler(this.context);
		this.statePool = new StatePool(this.context);
		
		this.head = new AtomicReference<BlockHeader>(this.context.getNode().getHead());
//		ledgerLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN | Logging.WARN);
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			this.ledgerStore.start();
			
			integrity();
			
			this.context.getEvents().register(this.syncBlockListener);
			this.context.getEvents().register(this.asyncAtomListener);
			this.context.getEvents().register(this.syncAtomListener);
			this.context.getEvents().register(this.peerListener);

			this.syncHandler.start();
			this.blockHandler.start();
			this.atomPool.start();
			this.atomHandler.start();
			this.stateAccumulator.reset();
			this.statePool.start();
			this.stateHandler.start();
		}
		catch (Exception ex)
		{
			throw new StartupException(ex);
		}			
	}

	@Override
	public void stop() throws TerminationException
	{
		this.context.getEvents().unregister(this.peerListener);
		this.context.getEvents().unregister(this.asyncAtomListener);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.syncBlockListener);

		this.statePool.stop();
		this.stateHandler.stop();
		this.atomHandler.stop();
		this.atomPool.stop();
		this.blockHandler.stop();
		this.syncHandler.stop();
		this.ledgerStore.stop();
	}
	
	public void clean() throws IOException
	{
		this.ledgerStore.clean();
	}
	
	private void integrity() throws IOException, ValidationException
	{
		BlockHeader nodeBlockHeader = this.context.getNode().getHead();
		// Check if this is just a new ledger store and doesn't need integrity or recovery
		if (nodeBlockHeader.equals(Universe.getDefault().getGenesis().getHeader()) == true && this.ledgerStore.has(nodeBlockHeader.getHash()) == false)
		{
			// Store the genesis block primitive
			this.ledgerStore.store(Universe.getDefault().getGenesis());

			// Commit the genesis block
			this.ledgerStore.commit(Universe.getDefault().getGenesis());
			for (Atom atom : Universe.getDefault().getGenesis().getAtoms())
			{
				this.stateAccumulator.lock(Universe.getDefault().getGenesis().getHeader(), atom);
				this.stateAccumulator.precommit(atom);
				this.stateAccumulator.commit(atom);
			}
			
			return;
		}
		else if (this.ledgerStore.has(nodeBlockHeader.getHash()) == false)
		{
			// TODO recover to the best head with committed state
			ledgerLog.error(Ledger.this.context.getName()+": Local node block header "+nodeBlockHeader+" not found in store");
			throw new UnsupportedOperationException("Integrity recovery not implemented");
		}
		else
		{
			// TODO block header is known but is it the strongest head that represents state?
			setHead(nodeBlockHeader);
		}
	}
	
	AtomHandler getAtomHandler()
	{
		return this.atomHandler;
	}

	VoteRegulator getVoteRegulator()
	{
		return this.voteRegulator;
	}

	public BlockHandler getBlockHandler()
	{
		return this.blockHandler;
	}

	public AtomPool getAtomPool()
	{
		return this.atomPool;
	}

	StateHandler getStateHandler()
	{
		return this.stateHandler;
	}

	StatePool getStatePool()
	{
		return this.statePool;
	}

	StateAccumulator getStateAccumulator()
	{
		return this.stateAccumulator;
	}

	LedgerStore getLedgerStore()
	{
		return this.ledgerStore;
	}
	
	@JsonGetter("head")
	public BlockHeader getHead()
	{
		return this.head.get();
	}

	void setHead(BlockHeader head)
	{
		this.head.set(Objects.requireNonNull(head));
	}

	public <T extends Primitive> T get(Hash hash, Class<T> primitive) throws IOException
	{
		return this.ledgerStore.get(hash, primitive);
	}
	
	public Future<AtomCertificate> submit(Atom atom) throws InterruptedException
	{
		Objects.requireNonNull(atom);
		
		synchronized(this.atomFutures)
		{
			AtomFuture atomFuture = this.atomFutures.get(atom.getHash());
			if (atomFuture == null)
			{
				atomFuture = new AtomFuture(atom);
				this.atomFutures.put(atom.getHash(), atomFuture);
				
				try
				{
					// TODO what happens on a false return?
					if (this.atomHandler.submit(atom) == false)
						throw new RejectedExecutionException();
				}
				catch(Throwable t)
				{
					this.atomFutures.remove(atom.getHash());
					atomFuture.completeExceptionally(t);
					throw t;
				}
			}

			return atomFuture;
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends Primitive> SearchResponse<T> get(final SearchQuery query, final Class<T> container, final Spin spin) throws IOException 
	{
		SearchResponse<IndexableCommit> searchResponse = null;
		long nextOffset = query.getOffset();
		List<T> results = new ArrayList<T>();

		try
		{
			do
			{
				SearchQuery nextQuery = new SearchQuery(query.getIdentifiers(), query.getMatchOn(), query.getContainer(), query.getOrder(), nextOffset, query.getLimit());
				searchResponse = this.ledgerStore.search(nextQuery);
				if (searchResponse != null)
				{
					List<Atom> atoms = new ArrayList<Atom>();
					for (IndexableCommit commit : searchResponse.getResults())
					{
						nextOffset = commit.getIndex();
						Atom atom = this.ledgerStore.get(commit.get(IndexableCommit.Path.ATOM), Atom.class);
						if (atom == null)
							ledgerLog.error("Expected atom "+commit.get(IndexableCommit.Path.ATOM)+" was not found");
						else
						{
							Fields fields = this.ledgerStore.get(commit.get(IndexableCommit.Path.ATOM), Fields.class);
							if (fields != null && fields.isEmpty() == false)
								atom.setFields(fields);

							atoms.add(atom);
						}
					}
					
					for (Atom atom : atoms)
					{
						try
						{
							for (Particle particle : atom.getParticles())
							{
								if (query.getContainer().isAssignableFrom(particle.getClass()) == false)
									continue;
								
								if (query.getMatchOn().equals(Match.ALL) == true &&
									particle.getIdentifiers().containsAll(query.getIdentifiers()) == false)
									continue;
		
								if (query.getMatchOn().equals(Match.ANY) == true)
								{
									boolean matched = false;
									for (Identifier identifier : query.getIdentifiers())
									{
										if (particle.getIdentifiers().contains(identifier) == true)
										{
											matched = true;
											break;
										}
									}
									
									if (matched == false)
										continue;
								}
	
								if ( spin.equals(Spin.ANY) == true ||
									(spin.equals(Spin.DOWN) == true && particle.getSpin().equals(Spin.DOWN)) ||
									(spin.equals(Spin.UP) == true && particle.getSpin().equals(Spin.UP)))
								{
									// TODO put a toggle in this so that function can return UP operations even if there is a DOWN
									if (this.ledgerStore.state(Indexable.from(particle.getHash(Spin.DOWN), Particle.class)) == CommitState.NONE)
										continue;
									
									if (Atom.class.isAssignableFrom(container) == true)
										results.add((T) atom);
									else if (container.isAssignableFrom(particle.getClass()) == true)
										results.add((T) particle);
									
									break;
								}
							}
						}
						catch (Exception ex)
						{
							// TODO Catch any throw or just error silently?
							ledgerLog.error(Ledger.this.context.getName()+": Atom for discovered identifier "+query+" threw error", ex);
						}
					}
				}
			}
			while(searchResponse.isEOR() == false && results.size() < query.getLimit());
		}
		catch (IOException ioex)
		{
			throw ioex;
		}
			
		return new SearchResponse<T>(query, nextOffset, results, searchResponse.isEOR());
	}

	@Override
	public CommitState state(Indexable indexable) throws IOException 
	{
		return this.ledgerStore.search(indexable) == null ? CommitState.NONE: CommitState.COMMITTED;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T extends Primitive> T get(final Indexable indexable, final Class<T> container) throws IOException
	{
		final IndexableCommit commit = this.ledgerStore.search(indexable);
		if (commit == null)
			return null;
		
		if (Block.class.isAssignableFrom(container) == true)
		{
			Block block = this.ledgerStore.get(commit.get(IndexableCommit.Path.BLOCK), Block.class);
			if (block == null)
				throw new IllegalStateException("Found indexable commit but unable to locate block");
			
			for (Atom atom : block.getAtoms())
			{
				Fields fields = this.ledgerStore.get(commit.get(IndexableCommit.Path.ATOM), Fields.class);
				if (fields != null && fields.isEmpty() == false)
					atom.setFields(fields);
			}

			return (T) block;
		}
		else if (BlockHeader.class.isAssignableFrom(container) == true)
		{
			BlockHeader blockHeader = this.ledgerStore.get(commit.get(IndexableCommit.Path.BLOCK), BlockHeader.class);
			if (blockHeader == null)
				throw new IllegalStateException("Found indexable commit but unable to locate block header");

			return (T) blockHeader;
		}
		else if (Atom.class.isAssignableFrom(container) == true)
		{
			Atom atom = this.ledgerStore.get(commit.get(IndexableCommit.Path.ATOM), Atom.class);
			if (atom == null)
				throw new IllegalStateException("Found indexable commit but unable to locate atom");

			Fields fields = this.ledgerStore.get(commit.get(IndexableCommit.Path.ATOM), Fields.class);
			if (fields != null && fields.isEmpty() == false)
				atom.setFields(fields);

			return (T) atom;
		}
		else if (Particle.class.isAssignableFrom(container) == true)
		{
			Atom atom = this.ledgerStore.get(commit.get(IndexableCommit.Path.ATOM), Atom.class);
			if (atom == null)
				throw new IllegalStateException("Found indexable commit but unable to locate atom");

			Fields fields = this.ledgerStore.get(commit.get(IndexableCommit.Path.ATOM), Fields.class);
			if (fields != null && fields.isEmpty() == false)
				atom.setFields(fields);

			for (Particle particle : atom.getParticles())
			{
				if (container.isAssignableFrom(particle.getClass()) == false)
					continue;
					
				if (particle.getHash().equals(indexable.getKey()) == true || 
					particle.getIndexables().contains(indexable) == true)
					return (T) particle;
			}
		}
		
		return null;
	}
	
	@VisibleForTesting
	private final AtomicBoolean synced = new AtomicBoolean(false);
	public synchronized boolean isInSync()
	{
		List<ConnectedPeer> syncPeers = this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED); 
		if (syncPeers.isEmpty() == true)
			return false;

		// Out of sync?
		// TODO need to deal with forks that don't converge with local chain due to safety break
		if (this.synced.get() == true && syncPeers.stream().anyMatch(cp -> cp.getNode().isAheadOf(this.context.getNode(), Node.OOS_TRIGGER_LIMIT)) == true)
			this.synced.set(false);
		else if (this.synced.get() == false)
		{
			ConnectedPeer strongestPeer = null;
			for (ConnectedPeer syncPeer : syncPeers)
			{
				if (strongestPeer == null || syncPeer.getNode().getHead().getHeight() > strongestPeer.getNode().getHead().getHeight())
					strongestPeer = syncPeer;
			}
			
			if (strongestPeer != null && 
				(strongestPeer.getNode().isInSyncWith(this.context.getNode(), 0) == true || this.context.getNode().isAheadOf(strongestPeer.getNode(), 0) == true))
				this.synced.set(true);
		}

		return this.synced.get();
	}
	
	// ASYNC ATOM LISTENER //
	private EventListener asyncAtomListener = new EventListener()
	{
		@Subscribe
		@AllowConcurrentEvents
		public void on(AtomCommittedEvent atomCommittedEvent) 
		{
			AtomFuture atomFuture = Ledger.this.atomFutures.remove(atomCommittedEvent.getCertificate().getAtom());
			if (atomFuture != null)
				atomFuture.complete(atomCommittedEvent.getCertificate());
		}

		@Subscribe
		public void on(AtomErrorEvent atomErrorEvent)
		{
			AtomFuture atomFuture = Ledger.this.atomFutures.remove(atomErrorEvent.getAtom().getHash());
			if (atomFuture != null)
				atomFuture.completeExceptionally(atomErrorEvent.getError());
		}
		
		@Subscribe
		public void on(AtomExceptionEvent atomExceptionEvent)
		{
			AtomFuture atomFuture = Ledger.this.atomFutures.remove(atomExceptionEvent.getAtom().getHash());
			if (atomFuture != null)
				atomFuture.completeExceptionally(atomExceptionEvent.getException());
		}
		
		@Subscribe
		public void on(AtomDiscardedEvent atomDiscardedEvent) 
		{
			Exception ex = new ValidationException("Atom "+atomDiscardedEvent.getAtom().getHash()+" was discarded due to: "+atomDiscardedEvent.getMessage());
			ledgerLog.warn(Ledger.this.context.getName()+": "+ex.getMessage());

			AtomFuture atomFuture = Ledger.this.atomFutures.remove(atomDiscardedEvent.getAtom().getHash());
			if (atomFuture != null)
				atomFuture.completeExceptionally(ex);
		}

		@Subscribe
		public void on(AtomCommitTimeoutEvent atomTimeoutEvent)
		{
			AtomFuture atomFuture = Ledger.this.atomFutures.remove(atomTimeoutEvent.getAtom().getHash());
			if (atomFuture != null)
				atomFuture.completeExceptionally(new TimeoutException("Atom "+atomTimeoutEvent.getAtom().getHash()+" timedout"));
		}

		// TODO want to float up to listeners registered here about onTimeout, onVerified etc for application domain?
	};
	
	// SYNCHRONOUS ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomPersistedEvent atomPersistedEvent) 
		{
			if (Ledger.this.atomPool.add(atomPersistedEvent.getAtom()) == false)
				ledgerLog.error(Ledger.this.context.getName()+": Atom "+atomPersistedEvent.getAtom().getHash()+" not added to atom pool");
		}
	};
	
	// SYNCHRONOUS BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		private long lastThroughputUpdate = System.currentTimeMillis();
		private long lastThroughputAtoms = 0;
		private long lastThroughputParticles = 0;

		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent) 
		{
			if (blockCommittedEvent.getBlock().getHeader().getPrevious().equals(Ledger.this.getHead().getHash()) == false)
			{
				ledgerLog.error(Ledger.this.context.getName()+": Committed block "+blockCommittedEvent.getBlock().getHeader()+" does not attach to current head "+Ledger.this.getHead());
				return;
			}
			
			// Clone it to make sure to extract the header
			Ledger.this.setHead(blockCommittedEvent.getBlock().getHeader());
			Ledger.this.context.getNode().setHead(blockCommittedEvent.getBlock().getHeader());
			ledgerLog.info(Ledger.this.context.getName()+": Committed block with "+blockCommittedEvent.getBlock().getHeader().getInventory(InventoryType.ATOMS).size()+" atoms and "+blockCommittedEvent.getBlock().getHeader().getInventory(InventoryType.CERTIFICATES).size()+" certificates "+blockCommittedEvent.getBlock().getHeader());
			Ledger.this.context.getMetaData().increment("ledger.commits.atoms", blockCommittedEvent.getBlock().getHeader().getInventory(InventoryType.ATOMS).size());
			Ledger.this.context.getMetaData().increment("ledger.commits.certificates", blockCommittedEvent.getBlock().getHeader().getInventory(InventoryType.CERTIFICATES).size());
			
			Ledger.this.voteRegulator.addVotePower(blockCommittedEvent.getBlock().getHeader().getOwner(), blockCommittedEvent.getBlock().getHeader().getHeight());
			
			// Lock atom states
			for (Atom atom : blockCommittedEvent.getBlock().getAtoms())
			{
				if (ledgerLog.hasLevel(Logging.DEBUG))
					ledgerLog.debug(Ledger.this.context.getName()+": Pre-committed atom "+atom.getHash()+" in "+blockCommittedEvent.getBlock().getHeader());
				
				Ledger.this.context.getMetaData().increment("ledger.processed.atoms");
				this.lastThroughputAtoms++;

				if (System.currentTimeMillis() - this.lastThroughputUpdate > TimeUnit.SECONDS.toMillis(10))
				{
					int seconds = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - this.lastThroughputUpdate);
					Ledger.this.context.getMetaData().put("ledger.throughput.atoms", (this.lastThroughputAtoms / seconds));
					Ledger.this.context.getMetaData().put("ledger.throughput.particles", (this.lastThroughputParticles / seconds));
					
					this.lastThroughputUpdate = System.currentTimeMillis();
					this.lastThroughputAtoms = this.lastThroughputParticles = 0;
				}

				try
				{
					Ledger.this.atomPool.remove(atom.getHash());
					Ledger.this.stateAccumulator.lock(blockCommittedEvent.getBlock().getHeader(), atom);
				}
				catch (Exception ex)
				{
		    		ledgerLog.fatal(Ledger.this.context.getName()+": Failed to lock atom state for "+atom, ex);
				}
			}

			// Commit atom states
			for (AtomCertificate certificate : blockCommittedEvent.getBlock().getCertificates())
			{
				try
				{
					if (ledgerLog.hasLevel(Logging.DEBUG) == true)
						ledgerLog.debug(Ledger.this.context.getName()+": Committed certificate "+certificate.getHash()+" for atom "+certificate.getAtom());
					
					Atom atom = Ledger.this.getLedgerStore().get(certificate.getAtom(), Atom.class);
					if (certificate.getDecision() == true)
					{
						Ledger.this.stateAccumulator.commit(atom);
						Ledger.this.context.getEvents().post(new AtomCommittedEvent(atom, certificate));
					}
					else
					{
						Ledger.this.stateAccumulator.abort(atom);
						for (ParticleCertificate particleCertificate : certificate.getAll())
						{
							if (particleCertificate.getDecision() == false)
							{
								Ledger.this.context.getEvents().post(new AtomErrorEvent(atom, new ValidationException("Rejection certificate for particle "+atom.getParticle(particleCertificate.getParticle()))));
								break;
							}
						}
					}
				}
				catch (Exception ex)
				{
					// FIXME don't like how this is thrown in an event listener.
					// 		 should be able to send the commit without having to fetch the atom, state processors *should* have it by the time
					//		 we're sending commit certificates to them.
	    			ledgerLog.error(Ledger.this.context.getName()+": Failed to post AtomCommittedEvent for "+certificate.getAtom(), ex);
				}
			}
		}
	};
	
	// PEER LISTENER //
	private EventListener peerListener = new EventListener()
	{
    	@Subscribe
		public void on(PeerConnectedEvent event)
		{
    		try
    		{
    			// TODO needs requesting on connect from synced nodes only
    			if (event.getPeer().getProtocol().equals(Protocol.TCP) == true)
    				Ledger.this.context.getNetwork().getMessaging().send(new GetAtomPoolMessage(), event.getPeer());
    		}
    		catch (IOException ioex)
    		{
    			ledgerLog.error("Failed to request atom pool items from "+event.getPeer(), ioex);
    		}
		}
	};
}
 