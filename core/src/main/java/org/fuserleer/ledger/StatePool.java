package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.collections.MappedBlockingQueue;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignatureBag;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.MerkleProof;
import org.fuserleer.crypto.MerkleProof.Branch;
import org.fuserleer.database.DatabaseException;
import org.fuserleer.events.EventListener;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.events.AtomAcceptedEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomExecutedEvent;
import org.fuserleer.ledger.events.AtomRejectedEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.events.StateCertificateEvent;
import org.fuserleer.ledger.events.SyncStatusChangeEvent;
import org.fuserleer.ledger.messages.SyncAcquiredMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.GossipFetcher;
import org.fuserleer.network.GossipFilter;
import org.fuserleer.network.GossipInventory;
import org.fuserleer.network.GossipReceiver;
import org.fuserleer.network.SyncInventory;
import org.fuserleer.network.messages.BroadcastInventoryMessage;
import org.fuserleer.network.messages.SyncInventoryMessage;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.node.Node;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.UInt256;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public final class StatePool implements Service
{
	private static final Logger statePoolLog = Logging.getLogger("statepool");
	private static final Logger gossipLog = Logging.getLogger("gossip");

	private final Context context;
	
	private class PendingState
	{
		private final Hash	atom;
		private final Hash	block;
		private final StateKey<?, ?> key;
		private final ReentrantLock lock = new ReentrantLock();

		private final Map<ECPublicKey, StateVote> votes;
		private final Map<Hash, Long> weights;
		private StateCertificate certificate;

		public PendingState(StateKey<?, ?> key, Hash atom, Hash block)
		{
			this.key = Objects.requireNonNull(key);
			this.atom = Objects.requireNonNull(atom);
			this.block = Objects.requireNonNull(block);
			this.votes = new HashMap<ECPublicKey, StateVote>();
			this.weights = new HashMap<Hash, Long>();
		}

		public StateKey<?, ?> getKey()
		{
			return this.key;
		}
		
		public long getHeight()
		{
			return Longs.fromByteArray(this.block.toByteArray());
		}

		public Hash getBlock()
		{
			return this.block;
		}
				
		public Hash getAtom()
		{
			return this.atom;
		}
		
		public StateCertificate buildCertificate() throws CryptoException, DatabaseException
		{
			this.lock.lock();
			try
			{
				if (this.certificate != null)
					throw new IllegalStateException("State certificate for "+this+" already exists");
				
				final long shardGroup = ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), StatePool.this.context.getLedger().numShardGroups(getHeight()));
				final long totalVotePower = StatePool.this.context.getLedger().getVotePowerHandler().getTotalVotePower(Math.max(0, getHeight()  - VotePowerHandler.VOTE_POWER_MATURITY), shardGroup);
				final long votePowerThreshold = StatePool.this.context.getLedger().getVotePowerHandler().getVotePowerThreshold(Math.max(0, getHeight() - VotePowerHandler.VOTE_POWER_MATURITY), shardGroup);

				long executionWithMajorityWeight = 0;
				Hash executionWithMajority = null;
				for (Entry<Hash, Long> execution : this.weights.entrySet())
				{
					if (execution.getValue() >= votePowerThreshold)
					{
						executionWithMajority = execution.getKey();
						executionWithMajorityWeight = execution.getValue();
						break;
					}
				}
				
				if (executionWithMajority == null)
					return null;
				
				final ECSignatureBag signatureBag = new ECSignatureBag();
				final List<StateVote> votes = new ArrayList<StateVote>();
				for (StateVote vote : this.votes.values())
				{
					if (vote.getExecution().equals(executionWithMajority) == false)
						continue;
					
					votes.add(vote);
					signatureBag.add(vote.getOwner(), vote.getSignature());
				}
				
				final VotePowerBloom votePowers = StatePool.this.context.getLedger().getVotePowerHandler().getVotePowerBloom(getBlock(), shardGroup);
				// TODO need merkles
				final StateCertificate certificate = new StateCertificate(votes.get(0).getState(), votes.get(0).getAtom(), votes.get(0).getBlock(), 
																		  votes.get(0).getInput(), votes.get(0).getOutput(), votes.get(0).getExecution(), 
																	      Hash.random(), Collections.singletonList(new MerkleProof(Hash.random(), Branch.OLD_ROOT)), votePowers, signatureBag);
				this.certificate = certificate;
				statePoolLog.info(StatePool.this.context.getName()+": State certificate "+certificate.getHash()+" for state "+votes.get(0).getState()+" in atom "+votes.get(0).getAtom()+" has "+votes.get(0).getDecision()+" agreement with "+executionWithMajorityWeight+"/"+totalVotePower);
				
				return this.certificate;
			}
			finally
			{
				this.lock.unlock();
			}
		}
		
		public StateVote vote(ECPublicKey identity)
		{
			this.lock.lock();
			try
			{
				return this.votes.get(identity);
			}
			finally
			{
				this.lock.unlock();
			}
		}
		
		boolean voted(ECPublicKey identity)
		{
			this.lock.lock();
			try
			{
				return this.votes.containsKey(Objects.requireNonNull(identity, "Vote identity is null"));
			}
			finally
			{
				this.lock.unlock();
			}
		}

		public Collection<StateVote> votes()
		{
			this.lock.lock();
			try
			{
				return new ArrayList<StateVote>(this.votes.values());
			}
			finally
			{
				this.lock.unlock();
			}
		}

		boolean vote(final StateVote vote, long weight) throws ValidationException
		{
			this.lock.lock();
			try
			{
				if (vote.getAtom().equals(this.atom) == false || 
					vote.getBlock().equals(this.block) == false || 
					vote.getState().equals(this.key) == false)
					throw new ValidationException("Vote from "+vote.getOwner()+" is not for state "+this.key+" -> "+this.atom+" -> "+this.block);
					
				if (vote.getDecision().equals(StateDecision.NEGATIVE) == true && vote.getExecution().equals(Hash.ZERO) == false)
					throw new ValidationException("Vote from "+vote.getOwner()+" with decision "+vote.getDecision()+" for state "+this.key+" -> "+this.atom+" -> "+this.block+" is not of valid form");

				if (this.votes.containsKey(vote.getOwner()) == false)
				{
					this.votes.put(vote.getOwner(), vote);
					this.weights.compute(vote.getExecution(), (k, v) -> v == null ? weight : v + weight);
					return true;
				}
				else
					statePoolLog.warn(StatePool.this.context.getName()+": "+vote.getOwner()+" has already cast a vote for "+vote.getState());
				
				return false;
			}
			finally
			{
				this.lock.unlock();
			}
		}

		public StateCertificate getCertificate()
		{
			this.lock.lock();
			try
			{
				return this.certificate;
			}
			finally
			{
				this.lock.unlock();
			}
		}

		@Override
		public int hashCode()
		{
			return this.key.hashCode();
		}

		@Override
		public boolean equals(Object obj)
		{
			if (obj == null)
				return false;
			
			if (obj != this)
				return false;
			
			return true;
		}

		@Override
		public String toString()
		{
			return this.key+" @ "+this.atom+":"+getHeight();
		}
	}
	
	private final Semaphore voteProcessorSemaphore = new Semaphore(0);
	private Executable voteProcessor = new Executable()
	{
		@Override
		public void execute()
		{
			try 
			{
				while (this.isTerminated() == false)
				{
					try
					{
						// TODO convert to a wait / notify
						if (StatePool.this.voteProcessorSemaphore.tryAcquire(1, TimeUnit.SECONDS) == false)
							continue;

						List<StateVote> stateVotesToBroadcast = new ArrayList<StateVote>();
						List<StateVote> stateVotesToCount = new ArrayList<StateVote>();
						StatePool.this.votesToCountQueue.drainTo(stateVotesToCount, 64);
						if (stateVotesToCount.isEmpty() == false)
						{
							for (StateVote stateVote : stateVotesToCount)
							{
								try
								{
									if (StatePool.this.context.getLedger().getLedgerStore().store(stateVote).equals(OperationStatus.SUCCESS) == false)
									{
										statePoolLog.warn(StatePool.this.context.getName()+": Received already seen state vote of "+stateVote.getState()+" for "+stateVote.getOwner());
										continue;
									}
									
									if (process(stateVote) == true)
										stateVotesToBroadcast.add(stateVote);
								}
								catch (Exception ex)
								{
									statePoolLog.error(StatePool.this.context.getName()+": Error counting vote for " + stateVote, ex);
								}
							}
						}

						List<PendingState> stateVotesToCast = new ArrayList<PendingState>();
						StatePool.this.votesToCastQueue.drainTo(stateVotesToCast, 64);
						if (stateVotesToCast.isEmpty() == false)
						{
							for (PendingState pendingState : stateVotesToCast)
							{
								StatePool.this.lock.writeLock().lock();
								try
								{
									PendingAtom pendingAtom = StatePool.this.context.getLedger().getAtomHandler().get(pendingState.getAtom());
									if (pendingAtom == null)
									{
										statePoolLog.error(StatePool.this.context.getName()+": Pending atom "+pendingState.getAtom()+" for pending state "+pendingState.getKey()+" not found");
										continue;
									}
									
									// Always vote locally even if no vote power so that can determine the accuracy of local execution
									long localVotePower = StatePool.this.context.getLedger().getVotePowerHandler().getVotePower(Math.max(0, pendingState.getHeight() - VotePowerHandler.VOTE_POWER_MATURITY), StatePool.this.context.getNode().getIdentity());
									if (pendingAtom.thrown() == null && pendingAtom.getExecution() == null)
										throw new IllegalStateException("Can not vote on state "+pendingState.getKey()+" when no decision made");
									
									if (localVotePower > 0)
									{
										Optional<UInt256> input = pendingAtom.getInput(pendingState.getKey());
										Optional<UInt256> output = pendingAtom.getOutput(pendingState.getKey());
										StateVote stateVote = new StateVote(pendingState.getKey(), pendingState.getAtom(), pendingState.getBlock(), 
																  			input == null ? null : input.orElse(null), output == null ? null : output.orElse(null),
																  			pendingAtom.getExecution(), StatePool.this.context.getNode().getIdentity());
										stateVote.sign(StatePool.this.context.getNode().getKey());
										
										if (StatePool.this.context.getLedger().getLedgerStore().store(stateVote).equals(OperationStatus.SUCCESS) == false)
										{
											statePoolLog.error(StatePool.this.context.getName()+": Persistance of local state vote of "+stateVote.getState()+" for "+stateVote.getOwner()+" failed");
											continue;
										}

										if (pendingState.vote(stateVote, localVotePower) == true)
										{
											if (statePoolLog.hasLevel(Logging.DEBUG))
												statePoolLog.debug(StatePool.this.context.getName()+": State vote "+stateVote.getHash()+" on "+pendingState.getKey()+" in atom "+pendingState.getAtom()+" with decision "+stateVote.getDecision());
										
											stateVotesToBroadcast.add(stateVote);
										}
									}
									
									// Don't build certificates from cast votes received until executed locally
									if (pendingState.getCertificate() == null)
									{
										StateCertificate stateCertificate = pendingState.buildCertificate();
										if (stateCertificate != null)
											StatePool.this.context.getEvents().post(new StateCertificateEvent(stateCertificate));
									}
								}
								catch (Exception ex)
								{
									statePoolLog.error(StatePool.this.context.getName()+": Error casting vote for " + pendingState.getKey(), ex);
								}
								finally
								{
									StatePool.this.lock.writeLock().unlock();
								}
							}
						}
						
						try
						{
							if (stateVotesToBroadcast.isEmpty() == false)
							{
								StatePool.this.context.getMetaData().increment("ledger.pool.state.votes", stateVotesToBroadcast.size());
								StatePool.this.context.getNetwork().getGossipHandler().broadcast(StateVote.class, stateVotesToBroadcast);
							}
						}
						catch (Exception ex)
						{
							statePoolLog.error(StatePool.this.context.getName()+": Error broadcasting state votes "+stateVotesToBroadcast, ex);
						}
					} 
					catch (InterruptedException e) 
					{
						// DO NOTHING //
						continue;
					}
				}
			}
			catch (Throwable throwable)
			{
				// TODO want to actually handle this?
				statePoolLog.fatal(StatePool.this.context.getName()+": Error processing state vote queue", throwable);
			}
		}
	};

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	private final BlockingQueue<PendingState> votesToCastQueue;
	private final MappedBlockingQueue<Hash, StateVote> votesToCountQueue;
	private final Map<Hash, PendingState> states = new HashMap<Hash, PendingState>();
	
	// Sync cache
	private final Multimap<Long, Hash> stateVoteSyncCache = Multimaps.synchronizedMultimap(HashMultimap.create());

	StatePool(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		
		this.votesToCastQueue = new LinkedBlockingQueue<PendingState>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.votesToCountQueue = new MappedBlockingQueue<Hash, StateVote>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		
//		statePoolLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
		statePoolLog.setLevels(Logging.ERROR | Logging.FATAL);
	}

	@Override
	public void start() throws StartupException
	{
		// STATE REPRESENTATIONS GOSSIP //
		this.context.getNetwork().getGossipHandler().register(StateVote.class, new GossipFilter(this.context) 
		{
			@Override
			public Set<Long> filter(Primitive stateVote) throws IOException
			{
				return Collections.singleton(ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), StatePool.this.context.getLedger().numShardGroups()));
			}
		});

		this.context.getNetwork().getGossipHandler().register(StateVote.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(Class<? extends Primitive> type, Collection<Hash> items) throws IOException
			{
				if (type.equals(StateVote.class) == false)
				{
					gossipLog.error(StatePool.this.context.getName()+": State vote type expected but got "+type);
					return Collections.emptyList();
				}
				
				StatePool.this.lock.readLock().lock();
				try
				{
					Set<Hash> required = new HashSet<Hash>();
					for (Hash item : items)
					{
						if (StatePool.this.votesToCountQueue.contains(item) == true || 
							StatePool.this.context.getLedger().getLedgerStore().has(item) == true)
							continue;

						required.add(item);
					}
					return required;
				}
				finally
				{
					StatePool.this.lock.readLock().unlock();
				}
			}
		});

		this.context.getNetwork().getGossipHandler().register(StateVote.class, new GossipReceiver() 
		{
			@Override
			public void receive(Primitive object) throws IOException, ValidationException, CryptoException
			{
				StateVote stateVote = (StateVote) object;
				if (statePoolLog.hasLevel(Logging.DEBUG) == true)
					statePoolLog.debug(StatePool.this.context.getName()+": State vote received "+stateVote+" for "+stateVote.getOwner());

				long numShardGroups = StatePool.this.context.getLedger().numShardGroups();
				long localShardGroup = ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), numShardGroups); 
				long stateVoteShardGroup = ShardMapper.toShardGroup(stateVote.getOwner(), numShardGroups);
				if (localShardGroup != stateVoteShardGroup)
				{
					statePoolLog.warn(StatePool.this.context.getName()+": State vote "+stateVote.getHash()+" for "+stateVote.getOwner()+" is for shard group "+stateVoteShardGroup+" but expected local shard group "+localShardGroup);
					// TODO disconnect and ban;
					return;
				}
				
				StatePool.this.votesToCountQueue.put(stateVote.getHash(), stateVote);
				StatePool.this.voteProcessorSemaphore.release();
			}
		});
					
		this.context.getNetwork().getGossipHandler().register(StateVote.class, new GossipFetcher() 
		{
			@Override
			public Collection<StateVote> fetch(Collection<Hash> items) throws IOException
			{
				StatePool.this.lock.readLock().lock();
				try
				{
					Set<StateVote> fetched = new HashSet<StateVote>();
					for (Hash item : items)
					{
						StateVote stateVote = StatePool.this.votesToCountQueue.get(item);
						if (stateVote == null)
							stateVote = StatePool.this.context.getLedger().getLedgerStore().get(item, StateVote.class);
						
						if (stateVote == null)
						{
							gossipLog.error(StatePool.this.context.getName()+": Requested state vote "+item+" not found");
							continue;
						}
						
						fetched.add(stateVote);
					}
					return fetched;
				}
				finally
				{
					StatePool.this.lock.readLock().unlock();
				}
			}
		});
		
		// SYNC //
		this.context.getNetwork().getGossipHandler().register(StateVote.class, new SyncInventory() 
		{
			@Override
			public Collection<Hash> process(Class<? extends Primitive> type, Collection<Hash> items) throws IOException
			{
				if (type.equals(StateVote.class) == false)
				{
					gossipLog.error(StatePool.this.context.getName()+": State vote type expected but got "+type);
					return Collections.emptyList();
				}
				
				StatePool.this.lock.writeLock().lock();
				try
				{
					Set<Hash> required = new HashSet<Hash>();
					for (Hash item : items)
					{
						if (StatePool.this.votesToCountQueue.contains(item) == true)
							continue;
						
						final StateVote stateVote = StatePool.this.context.getLedger().getLedgerStore().get(item, StateVote.class);
						if (stateVote != null)
						{
							try
							{
								StatePool.this.process(stateVote);
							}
							catch(Exception ex)
							{
								statePoolLog.error(StatePool.this.context.getName()+": Failed to process state vote on sync "+stateVote, ex);
							}
						}
						else
							required.add(item);
					}
					return required;
				}
				finally
				{
					StatePool.this.lock.writeLock().unlock();
				}
			}
		});

		this.context.getNetwork().getMessaging().register(SyncAcquiredMessage.class, this.getClass(), new MessageProcessor<SyncAcquiredMessage>()
		{
			@Override
			public void process(final SyncAcquiredMessage syncAcquiredMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						StatePool.this.lock.readLock().lock();
						try
						{
							if (statePoolLog.hasLevel(Logging.DEBUG) == true)
								statePoolLog.debug(StatePool.this.context.getName()+": State pool (votes) inventory request from "+peer);
							
							List<PendingState> pendingStates = new ArrayList<PendingState>(StatePool.this.states.values());
							List<Hash> stateVoteInventory = new ArrayList<Hash>();
							for (PendingState pendingState : pendingStates)
							{
								for (StateVote stateVote : pendingState.votes())
									stateVoteInventory.add(stateVote.getHash());
							}
							
							long height = StatePool.this.context.getLedger().getHead().getHeight();
							while (height >= syncAcquiredMessage.getHead().getHeight())
							{
								stateVoteInventory.addAll(StatePool.this.stateVoteSyncCache.get(height));
								height--;
							}
							
							if (statePoolLog.hasLevel(Logging.DEBUG) == true)
								statePoolLog.debug(StatePool.this.context.getName()+": Broadcasting about "+stateVoteInventory+" pool state votes to "+peer);

							int offset = 0;
							while(offset < stateVoteInventory.size())
							{
								SyncInventoryMessage stateVoteInventoryMessage = new SyncInventoryMessage(stateVoteInventory.subList(offset, Math.min(offset+SyncInventoryMessage.MAX_ITEMS, stateVoteInventory.size())), StateVote.class);
								StatePool.this.context.getNetwork().getMessaging().send(stateVoteInventoryMessage, peer);
								offset += SyncInventoryMessage.MAX_ITEMS; 
							}
						}
						catch (Exception ex)
						{
							statePoolLog.error(StatePool.this.context.getName()+": ledger.messages.state.get.pool " + peer, ex);
						}
						finally
						{
							StatePool.this.lock.readLock().unlock();
						}
					}
				});
			}
		});

		this.context.getEvents().register(this.syncChangeListener);
		this.context.getEvents().register(this.asyncBlockListener);
		this.context.getEvents().register(this.syncAtomListener);
		
		Thread voteProcessorThread = new Thread(this.voteProcessor);
		voteProcessorThread.setDaemon(true);
		voteProcessorThread.setName(this.context.getName()+" State Vote Processor");
		voteProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException
	{
		this.voteProcessor.terminate(true);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.asyncBlockListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}

	public void add(final PendingAtom atom)
	{
		Objects.requireNonNull(atom, "Atom is null");
		
		this.lock.writeLock().lock();
		try
		{
			long numShardGroups = this.context.getLedger().numShardGroups(Longs.fromByteArray(atom.getBlock().toByteArray()));
			long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			for (StateKey<?, ?> stateKey : atom.getStateKeys())
			{
				long stateShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
				if (stateShardGroup != localShardGroup)
					continue;
				
				Hash stateAtomBlockHash = Hash.from(stateKey.get(), atom.getHash(), atom.getBlock());
				PendingState pendingState = this.states.get(stateAtomBlockHash);
				if (pendingState == null)
				{
					this.states.put(stateAtomBlockHash,  new PendingState(stateKey, atom.getHash(), atom.getBlock()));
					StatePool.this.context.getMetaData().increment("ledger.pool.state.added");
				}
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public void add(final PendingState state)
	{
		Objects.requireNonNull(state, "State is null");
		
		this.lock.writeLock().lock();
		try
		{
			long numShardGroups = this.context.getLedger().numShardGroups(Longs.fromByteArray(state.getBlock().toByteArray()));
			long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			long stateShardGroup = ShardMapper.toShardGroup(state.getKey().get(), numShardGroups);
			if (stateShardGroup != localShardGroup)
				throw new IllegalArgumentException(StatePool.this.context.getName()+": Pending state "+state+" is for shard group "+stateShardGroup+" expected local shard group "+localShardGroup);
				
			Hash stateAtomBlockHash = Hash.from(state.getKey().get(), state.getAtom(), state.getBlock());
			PendingState pendingState = this.states.get(stateAtomBlockHash);
			if (pendingState != null)
				throw new IllegalStateException(this.context.getName()+": Pending state "+state.getKey()+" already exists");

			this.states.put(stateAtomBlockHash, state);
			StatePool.this.context.getMetaData().increment("ledger.pool.state.added");
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public void committed(final PendingAtom atom)
	{
		Objects.requireNonNull(atom, "Atom is null");
		
		this.lock.writeLock().lock();
		try
		{
			long numShardGroups = this.context.getLedger().numShardGroups(Longs.fromByteArray(atom.getBlock().toByteArray()));
			long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			for (StateKey<?, ?> stateKey : atom.getStateKeys())
			{
				long stateShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
				if (stateShardGroup != localShardGroup)
					continue;
				
				Hash stateAtomBlockHash = Hash.from(stateKey.get(), atom.getHash(), atom.getBlock());
				PendingState pendingState = this.states.remove(stateAtomBlockHash);
				if (pendingState == null)
					throw new IllegalStateException(this.context.getName()+": Expected pending state "+stateKey+" not found");

				this.stateVoteSyncCache.putAll(this.context.getLedger().getHead().getHeight(), pendingState.votes().stream().map(sv -> sv.getHash()).collect(Collectors.toList()));

				StatePool.this.context.getMetaData().increment("ledger.pool.state.removed");
				StatePool.this.context.getMetaData().increment("ledger.pool.state.committed");
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public void aborted(final PendingAtom atom)
	{
		Objects.requireNonNull(atom, "Atom is null");
		
		this.lock.writeLock().lock();
		try
		{
			long numShardGroups = this.context.getLedger().numShardGroups(Longs.fromByteArray(atom.getBlock().toByteArray()));
			long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			for (StateKey<?, ?> stateKey : atom.getStateKeys())
			{
				long stateShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
				if (stateShardGroup != localShardGroup)
					continue;
				
				Hash stateAtomBlockHash = Hash.from(stateKey.get(), atom.getHash(), atom.getBlock());
				PendingState pendingState = this.states.remove(stateAtomBlockHash);
				if (pendingState == null)
					throw new IllegalStateException(this.context.getName()+": Expected pending state "+stateKey+" not found");
				
				this.stateVoteSyncCache.putAll(this.context.getLedger().getHead().getHeight(), pendingState.votes().stream().map(sv -> sv.getHash()).collect(Collectors.toList()));
				
				StatePool.this.context.getMetaData().increment("ledger.pool.state.removed");
				StatePool.this.context.getMetaData().increment("ledger.pool.state.aborted");
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	void queue(final PendingAtom atom)
	{
		Objects.requireNonNull(atom);
		
		this.lock.writeLock().lock();
		try
		{
			long numShardGroups = this.context.getLedger().numShardGroups(Longs.fromByteArray(atom.getBlock().toByteArray()));
			long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			for (StateKey<?, ?> stateKey : atom.getStateKeys())
			{
				long stateShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
				if (stateShardGroup != localShardGroup)
					continue;
				
				Hash stateAtomBlockHash = Hash.from(stateKey.get(), atom.getHash(), atom.getBlock());
				PendingState pendingState = this.states.get(stateAtomBlockHash);
				if (pendingState == null)
					throw new IllegalStateException(this.context.getName()+": Expected pending state "+stateKey+" not found");

				this.votesToCastQueue.add(pendingState);
				StatePool.this.voteProcessorSemaphore.release();
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	private boolean process(final StateVote stateVote) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(stateVote, "State vote is null");
		
		if (stateVote.verify(stateVote.getOwner()) == false)
		{
			statePoolLog.error(StatePool.this.context.getName()+": State vote failed verification for "+stateVote.getOwner());
			return false;
		}

		StatePool.this.lock.writeLock().lock();
		PendingAtom pendingAtom;
		PendingState pendingState;
		try
		{
			Hash stateAtomBlockHash = Hash.from(stateVote.getState().get(), stateVote.getAtom(), stateVote.getBlock());
			pendingState = StatePool.this.states.get(stateAtomBlockHash);
			// Creating pending state objects from vote if particle not seen or committed
			if (pendingState == null)
			{
				pendingAtom = StatePool.this.context.getLedger().getAtomHandler().get(stateVote.getAtom());
				if (pendingAtom == null)
				{
					statePoolLog.warn(StatePool.this.context.getName()+": Pending atom not found or completed for received state vote of "+stateVote.getState()+" for "+stateVote.getOwner());
					return false;
				}

				pendingState = new PendingState(stateVote.getState(), stateVote.getAtom(), stateVote.getBlock());
				add(pendingState);
			}
			else
			{
				pendingAtom = StatePool.this.context.getLedger().getAtomHandler().get(pendingState.getAtom());
				if (pendingAtom == null)
					throw new IllegalStateException("Pending atom "+pendingState.getAtom()+" for pending state "+pendingState.getKey()+" not found");
			}

			if (pendingState.getBlock().equals(stateVote.getBlock()) == false || 
				pendingState.getAtom().equals(stateVote.getAtom()) == false)
				throw new IllegalStateException("State vote "+stateVote.getState()+" block or atom dependencies not as expected for "+stateVote.getOwner());
					
			long votePower = StatePool.this.context.getLedger().getVotePowerHandler().getVotePower(Math.max(0, pendingState.getHeight() - VotePowerHandler.VOTE_POWER_MATURITY), stateVote.getOwner());
			if (votePower > 0 && pendingState.vote(stateVote, votePower) == true)
			{
				if (pendingState.getCertificate() == null && pendingAtom.getStatus().greaterThan(CommitStatus.PROVISIONED) == true)
				{
					StateCertificate stateCertificate = pendingState.buildCertificate();
					if (stateCertificate != null)
						StatePool.this.context.getEvents().post(new StateCertificateEvent(stateCertificate));
				}
				
				return true;
			}
			
			return false;
		}
		finally
		{
			StatePool.this.lock.writeLock().unlock();
		}
	}
	
	public int size()
	{
		this.lock.writeLock().lock();
		try
		{
			return this.states.size();
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	// ASYNC BLOCK LISTENER //
	private EventListener asyncBlockListener = new EventListener()
	{
		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent)
		{
			StatePool.this.lock.writeLock().lock();
			try
			{
				long trimTo = blockCommittedEvent.getBlock().getHeader().getHeight() - Node.OOS_TRIGGER_LIMIT;
				if (trimTo > 0)
				{
					Iterator<Long> stateVoteSyncCacheKeyIterator = StatePool.this.stateVoteSyncCache.keySet().iterator();
					while(stateVoteSyncCacheKeyIterator.hasNext() == true)
					{
						if (stateVoteSyncCacheKeyIterator.next() < trimTo)
							stateVoteSyncCacheKeyIterator.remove();
					}
				}
			}
			finally
			{
				StatePool.this.lock.writeLock().unlock();
			}
		}
	};
	
	// SYNCHRONOUS ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomExecutedEvent event) 
		{
			add(event.getPendingAtom());
			queue(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event) 
		{
//			remove(atomCommitTimeoutEvent.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomAcceptedEvent event) 
		{
			committed(event.getPendingAtom());
		}
		
		@Subscribe
		public void on(final AtomRejectedEvent event) 
		{
			if (event.getPendingAtom().getStatus().equals(CommitStatus.EXECUTED) == false)
				return;

			committed(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomExceptionEvent event) 
		{
			if (event.getPendingAtom().getStatus().equals(CommitStatus.EXECUTED) == false)
				return;

			if (event.getException() instanceof StateLockedException)
				return;
				
			aborted(event.getPendingAtom());
		}
	};
	
	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncStatusChangeEvent event) 
		{
			if (event.isSynced() == true)
				return;

			StatePool.this.lock.writeLock().lock();
			try
			{
				statePoolLog.info(StatePool.this.context.getName()+": Sync status changed to "+event.isSynced()+", flushing state pool");
				StatePool.this.voteProcessorSemaphore.drainPermits();
				StatePool.this.states.clear();
				StatePool.this.votesToCastQueue.clear();
				StatePool.this.votesToCountQueue.clear();
			}
			finally
			{
				StatePool.this.lock.writeLock().unlock();
			}
		}
	};
}
