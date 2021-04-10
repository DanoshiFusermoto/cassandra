package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.collections.Bloom;
import org.fuserleer.collections.MappedBlockingQueue;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.crypto.BLSSignature;
import org.fuserleer.crypto.CryptoException;
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
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.atoms.Atom;
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
import org.fuserleer.network.messages.GetInventoryItemsMessage;
import org.fuserleer.network.messages.SyncInventoryMessage;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.node.Node;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.UInt256;

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

		private final Map<BLSPublicKey, StateVote> votes;
		private final Map<Hash, Long> weights;
		
		private Bloom signers;
		private StateVote majorityStateVote;
		private BLSPublicKey aggregatePublicKey;
		private BLSSignature aggregateSignature;
		private StateCertificate certificate;
		private boolean verified = false;
		private long agreedVotePower, totalVotePower;

		public PendingState(StateKey<?, ?> key, Hash atom, Hash block)
		{
			this.key = Objects.requireNonNull(key);
			this.atom = Objects.requireNonNull(atom);
			this.block = Objects.requireNonNull(block);
			this.votes = new HashMap<BLSPublicKey, StateVote>();
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
		
		public boolean preverify() throws DatabaseException
		{
			this.lock.lock();
			try
			{
				if (this.majorityStateVote != null)
					throw new IllegalStateException("Pending state "+this+" is already preverified");
				
				final long shardGroup = ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), StatePool.this.context.getLedger().numShardGroups(getHeight()));
				final long votePowerThreshold = StatePool.this.context.getLedger().getValidatorHandler().getVotePowerThreshold(Math.max(0, getHeight() - ValidatorHandler.VOTE_POWER_MATURITY), shardGroup);
				final long totalVotePower = StatePool.this.context.getLedger().getValidatorHandler().getTotalVotePower(Math.max(0, getHeight() - ValidatorHandler.VOTE_POWER_MATURITY), shardGroup);

				Entry<Hash, Long> executionWithMajority = null;
				for (Entry<Hash, Long> execution : this.weights.entrySet())
				{
					if (execution.getValue() >= votePowerThreshold)
					{
						executionWithMajority = execution;
						break;
					}
				}
				
				if (executionWithMajority == null)
					return false;
				
				// Aggregate and verify
				// TODO failures
				StateVote majorityStateVote = null;
				BLSPublicKey aggregatedPublicKey = null;
				BLSSignature aggregatedSignature = null;
				final Bloom signers = new Bloom(0.000001, this.votes.size());
				for (StateVote vote : this.votes.values())
				{
					if (vote.getExecution().equals(executionWithMajority.getKey()) == false)
						continue;
					
					if (majorityStateVote == null)
						majorityStateVote = new StateVote(vote.getState(), vote.getAtom(), vote.getBlock(), vote.getInput(), vote.getOutput(), vote.getExecution());
					
					if (aggregatedPublicKey == null)
					{
						aggregatedPublicKey = vote.getOwner();
						aggregatedSignature = vote.getSignature();
					}
					else
					{
						aggregatedPublicKey = aggregatedPublicKey.combine(vote.getOwner());
						aggregatedSignature = aggregatedSignature.combine(vote.getSignature());
					}

					signers.add(vote.getOwner().toByteArray());
				}
				
				this.agreedVotePower = executionWithMajority.getValue();
				this.totalVotePower = totalVotePower;
				this.majorityStateVote = majorityStateVote;
				this.aggregatePublicKey = aggregatedPublicKey;
				this.aggregateSignature = aggregatedSignature;
				this.signers = signers;
				
				return true;
			}
			finally
			{
				this.lock.unlock();
			}
		}
		
		public boolean isPreverified()
		{
			this.lock.lock();
			try
			{
				return this.majorityStateVote != null;
			}
			finally
			{
				this.lock.unlock();
			}
		}
		
		public boolean verify() throws CryptoException
		{
			this.lock.lock();
			try
			{
				if (this.verified == true)
					throw new IllegalStateException("Pending state "+this+" majority is already verified");

				if (this.majorityStateVote == null)
					throw new IllegalStateException("Pending state "+this+" has not met threshold majority or is not prepared");
				
				if (this.aggregatePublicKey.verify(this.majorityStateVote.getTarget(), this.aggregateSignature) == false)
				{
					statePoolLog.error(StatePool.this.context.getName()+": State pool votes failed verification for "+this.majorityStateVote.getTarget()+" of "+this.signers.count()+" aggregated signatures");
					this.majorityStateVote = null;
					this.verified = false;
					return false;
				}
				
				this.verified = true;
				return true;
			}
			finally
			{
				this.lock.unlock();
			}
		}
		
		public boolean isVerified()
		{
			this.lock.lock();
			try
			{
				return this.verified;
			}
			finally
			{
				this.lock.unlock();
			}
		}
		
		public StateCertificate buildCertificate() throws CryptoException, DatabaseException
		{
			this.lock.lock();
			try
			{
				if (this.certificate != null)
					throw new IllegalStateException("State certificate for "+this+" already exists");
				
				if (this.verified == false)
					throw new IllegalStateException("Pending state "+this+" is not verified");
				
				final long shardGroup = ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), StatePool.this.context.getLedger().numShardGroups(getHeight()));
				final VotePowerBloom votePowers = StatePool.this.context.getLedger().getValidatorHandler().getVotePowerBloom(getBlock(), shardGroup);
				// TODO need merkles
				final StateCertificate certificate = new StateCertificate(this.majorityStateVote.getState(), this.majorityStateVote.getAtom(), this.majorityStateVote.getBlock(), 
																		  this.majorityStateVote.getInput(), this.majorityStateVote.getOutput(), this.majorityStateVote.getExecution(), 
																	      Hash.random(), Collections.singletonList(new MerkleProof(Hash.random(), Branch.OLD_ROOT)), votePowers, this.signers, this.aggregateSignature);
				this.certificate = certificate;
				statePoolLog.info(StatePool.this.context.getName()+": State certificate "+certificate.getHash()+" for state "+this.majorityStateVote.getState()+" in atom "+this.majorityStateVote.getAtom()+" has "+this.majorityStateVote.getDecision()+" agreement with "+this.agreedVotePower+"/"+this.totalVotePower);
				
				return this.certificate;
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
						synchronized(StatePool.this.voteProcessor)
						{
							StatePool.this.voteProcessor.wait(TimeUnit.SECONDS.toMillis(1));
						}

						if (StatePool.this.context.getLedger().isSynced() == false)
							continue;

						if (StatePool.this.votesToSyncQueue.isEmpty() == false)
						{
							Entry<Hash, StateVote> stateVote;
							while((stateVote = StatePool.this.votesToSyncQueue.peek()) != null)
							{
								try
								{
									if (process(stateVote.getValue()) == false)
										statePoolLog.warn(StatePool.this.context.getName()+": Syncing state vote "+stateVote.getValue().getState()+" returned false for atom "+stateVote.getValue().getAtom()+" in block "+stateVote.getValue().getBlock()+" by "+stateVote.getValue().getOwner());
								}
								catch (Exception ex)
								{
									statePoolLog.error(StatePool.this.context.getName()+": Error syncing vote for " + stateVote.getValue(), ex);
								}
								finally
								{
									if (StatePool.this.votesToSyncQueue.remove(stateVote.getKey(), stateVote.getValue()) == false)
										throw new IllegalStateException("State pool vote peek/remove failed for "+stateVote.getValue());
								}
							}
						}

						List<StateVote> stateVotesToBroadcast = new ArrayList<StateVote>();
						if (StatePool.this.votesToCountQueue.isEmpty() == false)
						{
							Entry<Hash, StateVote> stateVote;
							while((stateVote = StatePool.this.votesToCountQueue.peek()) != null)
							{
								try
								{
									if (StatePool.this.context.getLedger().getLedgerStore().store(stateVote.getValue()).equals(OperationStatus.SUCCESS) == false)
									{
										statePoolLog.warn(StatePool.this.context.getName()+": Received already seen state vote of "+stateVote.getValue().getState()+" for atom "+stateVote.getValue().getAtom()+" in block "+stateVote.getValue().getBlock()+" by "+stateVote.getValue().getOwner());
										continue;
									}
									
									if (process(stateVote.getValue()) == true)
										stateVotesToBroadcast.add(stateVote.getValue());
									else
										statePoolLog.warn(StatePool.this.context.getName()+": Processing of state vote "+stateVote.getValue().getState()+" returned false for atom "+stateVote.getValue().getAtom()+" in block "+stateVote.getValue().getBlock()+" by "+stateVote.getValue().getOwner());
								}
								catch (Exception ex)
								{
									statePoolLog.error(StatePool.this.context.getName()+": Error counting vote for " + stateVote.getValue(), ex);
								}
								finally
								{
									if (StatePool.this.votesToCountQueue.remove(stateVote.getKey(), stateVote.getValue()) == false)
										throw new IllegalStateException("State pool vote peek/remove failed for "+stateVote.getValue());
								}
							}
						}

						if (StatePool.this.votesToCastQueue.isEmpty() == false)
						{
							PendingState pendingState;
							while((pendingState = StatePool.this.votesToCastQueue.peek()) != null)
							{
								try
								{
									PendingAtom pendingAtom = StatePool.this.context.getLedger().getAtomHandler().get(pendingState.getAtom());
									if (pendingAtom == null)
									{
										statePoolLog.error(StatePool.this.context.getName()+": Pending atom "+pendingState.getAtom()+" for pending state "+pendingState.getKey()+" not found");
										continue;
									}
									
									long localVotePower = StatePool.this.context.getLedger().getValidatorHandler().getVotePower(Math.max(0, pendingState.getHeight() - ValidatorHandler.VOTE_POWER_MATURITY), StatePool.this.context.getNode().getIdentity());
									if (pendingAtom.thrown() == null && pendingAtom.getExecution() == null)
										throw new IllegalStateException("Can not vote on state "+pendingState.getKey()+" when no decision made");
									
									if (localVotePower > 0)
									{
										Optional<UInt256> input = pendingAtom.getInput(pendingState.getKey());
										Optional<UInt256> output = pendingAtom.getOutput(pendingState.getKey());
										StateVote stateVote = new StateVote(pendingState.getKey(), pendingState.getAtom(), pendingState.getBlock(), 
																  			input == null ? null : input.orElse(null), output == null ? null : output.orElse(null),
																  			pendingAtom.getExecution(), StatePool.this.context.getNode().getIdentity());
										stateVote.sign(StatePool.this.context.getNode().getKeyPair());
										
										if (StatePool.this.context.getLedger().getLedgerStore().store(stateVote).equals(OperationStatus.SUCCESS) == true)
										{
											if (process(stateVote) == true)
											{
												if (statePoolLog.hasLevel(Logging.DEBUG))
													statePoolLog.debug(StatePool.this.context.getName()+": State vote "+stateVote.getHash()+" on "+pendingState.getKey()+" in atom "+pendingState.getAtom()+" with decision "+stateVote.getDecision());
																					
												stateVotesToBroadcast.add(stateVote);
											}
											
//											if (pendingState.vote(stateVote, localVotePower) == true)
//											{
//												if (statePoolLog.hasLevel(Logging.DEBUG))
//													statePoolLog.debug(StatePool.this.context.getName()+": State vote "+stateVote.getHash()+" on "+pendingState.getKey()+" in atom "+pendingState.getAtom()+" with decision "+stateVote.getDecision());
//										
//												stateVotesToBroadcast.add(stateVote);
//											}
										}
										else // FIXME happens usually on sync after state reconstruction as local validator doesn't know its voted already
											statePoolLog.error(StatePool.this.context.getName()+": Persistance of local state vote of "+stateVote.getState()+" for "+stateVote.getOwner()+" failed");
									}
								}
								catch (Exception ex)
								{
									statePoolLog.error(StatePool.this.context.getName()+": Error casting vote for " + pendingState.getKey(), ex);
								}
								finally
								{
									if (pendingState.equals(StatePool.this.votesToCastQueue.poll()) == false)
										throw new IllegalStateException("State pool vote cast peek/pool failed for "+pendingState.getKey());
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
	private final Map<Hash, StateVote> votesToCountDelayed;
	private final BlockingQueue<PendingState> votesToCastQueue;
	private final MappedBlockingQueue<Hash, StateVote> votesToSyncQueue;
	private final MappedBlockingQueue<Hash, StateVote> votesToCountQueue;
	private final Map<Hash, PendingState> states = new HashMap<Hash, PendingState>();
	
	StatePool(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		
		this.votesToCastQueue = new LinkedBlockingQueue<PendingState>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.votesToSyncQueue = new MappedBlockingQueue<Hash, StateVote>(this.context.getConfiguration().get("ledger.state.queue", 1<<20));
		this.votesToCountQueue = new MappedBlockingQueue<Hash, StateVote>(this.context.getConfiguration().get("ledger.state.queue", 1<<20));
		this.votesToCountDelayed = Collections.synchronizedMap(new HashMap<Hash, StateVote>());
		
//		statePoolLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
		statePoolLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
//		statePoolLog.setLevels(Logging.ERROR | Logging.FATAL);
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
			public int requestLimit()
			{
				return GetInventoryItemsMessage.MAX_ITEMS;
			}

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
						if (StatePool.this.votesToSyncQueue.contains(item) == true || 
							StatePool.this.votesToCountQueue.contains(item) == true || 
							StatePool.this.votesToCountDelayed.containsKey(item) == true ||
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
					statePoolLog.debug(StatePool.this.context.getName()+": State vote "+stateVote.getHash()+" received for "+stateVote.getObject()+":"+stateVote.getAtom()+" by "+stateVote.getOwner());

				long numShardGroups = StatePool.this.context.getLedger().numShardGroups();
				long localShardGroup = ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), numShardGroups); 
				long stateVoteShardGroup = ShardMapper.toShardGroup(stateVote.getOwner(), numShardGroups);
				if (localShardGroup != stateVoteShardGroup)
				{
					statePoolLog.warn(StatePool.this.context.getName()+": State vote "+stateVote.getHash()+" for "+stateVote.getOwner()+" is for shard group "+stateVoteShardGroup+" but expected local shard group "+localShardGroup);
					// TODO disconnect and ban;
					return;
				}
				
				if (stateVote.getHeight() <= StatePool.this.context.getLedger().getHead().getHeight())
				{
					StatePool.this.votesToCountQueue.put(stateVote.getHash(), stateVote);
					synchronized(StatePool.this.voteProcessor)
					{
						StatePool.this.voteProcessor.notify();
					}
				}
				else
					StatePool.this.votesToCountDelayed.put(stateVote.getHash(), stateVote);
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
							stateVote = StatePool.this.votesToCountDelayed.get(item);
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
							
							final Set<PendingState> pendingStates = new HashSet<PendingState>(StatePool.this.states.values());
							final Set<Hash> stateVoteInventory = new LinkedHashSet<Hash>();
							for (PendingState pendingState : pendingStates)
							{
								for (StateVote stateVote : pendingState.votes())
									stateVoteInventory.add(stateVote.getHash());
							}
							
							long height = StatePool.this.context.getLedger().getHead().getHeight();
							while (height >= Math.max(0, syncAcquiredMessage.getHead().getHeight() - Node.OOS_RESOLVED_LIMIT))
							{
								stateVoteInventory.addAll(StatePool.this.context.getLedger().getLedgerStore().getSyncInventory(height, StateVote.class));
								height--;
							}
							
							if (statePoolLog.hasLevel(Logging.DEBUG) == true)
								statePoolLog.debug(StatePool.this.context.getName()+": Broadcasting about "+stateVoteInventory+" pool state votes to "+peer);

							while(stateVoteInventory.isEmpty() == false)
							{
								SyncInventoryMessage stateVoteInventoryMessage = new SyncInventoryMessage(stateVoteInventory, 0, Math.min(SyncInventoryMessage.MAX_ITEMS, stateVoteInventory.size()), StateVote.class);
								StatePool.this.context.getNetwork().getMessaging().send(stateVoteInventoryMessage, peer);
								stateVoteInventory.removeAll(stateVoteInventoryMessage.getItems());
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
				
				StatePool.this.context.getMetaData().increment("ledger.pool.state.removed");
				StatePool.this.context.getMetaData().increment("ledger.pool.state.aborted");
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	void remove(final PendingAtom atom)
	{
		Objects.requireNonNull(atom, "Atom is null");
		
		this.lock.writeLock().lock();
		try
		{
			long numShardGroups = this.context.getLedger().numShardGroups(Longs.fromByteArray(atom.getBlock().toByteArray()));
			long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			final Collection<StateKey<?, ?>> stateKeys = atom.getStateKeys();
			for (StateKey<?, ?> stateKey : stateKeys)
			{
				long stateShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
				if (stateShardGroup != localShardGroup)
					continue;
				
				Hash stateAtomBlockHash = Hash.from(stateKey.get(), atom.getHash(), atom.getBlock());
				PendingState pendingState = this.states.remove(stateAtomBlockHash);
				if (pendingState == null)
					throw new IllegalStateException(this.context.getName()+": Expected pending state "+stateKey+" not found");

				this.votesToCastQueue.remove(pendingState);
				StatePool.this.context.getMetaData().increment("ledger.pool.state.removed");
			}
			
			final List<Hash> removals = new ArrayList<Hash>();
			this.votesToCountQueue.forEach((h,sv) -> {
				if (stateKeys.contains(sv.getObject()) == true)
					removals.add(h);
			});
			this.votesToCountDelayed.forEach((h,sv) -> {
				if (stateKeys.contains(sv.getObject()) == true)
					removals.add(h);
			});
			this.votesToSyncQueue.forEach((h,sv) -> {
				if (stateKeys.contains(sv.getObject()) == true)
					removals.add(h);
			});
			
			this.votesToCountQueue.removeAll(removals);
			this.votesToSyncQueue.removeAll(removals);
			synchronized(this.votesToCountDelayed) { this.votesToCountDelayed.keySet().removeAll(removals); }
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
				synchronized(StatePool.this.voteProcessor)
				{
					StatePool.this.voteProcessor.notify();
				}
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
		
		StatePool.this.lock.writeLock().lock();
		boolean response = false;
		PendingState pendingState = null;
		PendingAtom pendingAtom = null;
		try
		{
			Hash stateAtomBlockHash = Hash.from(stateVote.getState().get(), stateVote.getAtom(), stateVote.getBlock());
			pendingAtom = StatePool.this.context.getLedger().getAtomHandler().get(stateVote.getAtom());
			pendingState = StatePool.this.states.get(stateAtomBlockHash);
			// Creating pending state objects from vote if particle not seen or committed
			if (pendingState == null)
			{
				// Pending state and atom null is likely the atom has already been committed
				// Check for a commit status, and if true silently accept
				if (pendingAtom == null)
				{
					Commit commit = StatePool.this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, stateVote.getAtom()));
					if (commit == null)
						throw new IllegalStateException("Pending state "+stateVote.getState()+" and atom "+stateVote.getAtom()+" not found, no commit present");
					
					if (commit != null && commit.getPath().get(Elements.CERTIFICATE) == null)
						throw new IllegalStateException("Pending state "+stateVote.getState()+" and atom "+stateVote.getAtom()+" not found, commit lacking certificate");

					if (stateVote.getBlock().equals(commit.getPath().get(Elements.BLOCK)) == false || 
						stateVote.getAtom().equals(commit.getPath().get(Elements.ATOM)) == false)
						throw new IllegalStateException("State vote "+stateVote.getState()+" block or atom dependencies not as expected for "+stateVote.getOwner());
					
					return true;
				}

				// Atom is known, pending state is not yet created
				pendingState = new PendingState(stateVote.getState(), stateVote.getAtom(), stateVote.getBlock());
				add(pendingState);
			}
			else if (pendingAtom == null)
				throw new IllegalStateException("Pending atom "+pendingState.getAtom()+" for pending state "+pendingState.getKey()+" not found");

			long votePower = StatePool.this.context.getLedger().getValidatorHandler().getVotePower(Math.max(0, pendingState.getHeight() - ValidatorHandler.VOTE_POWER_MATURITY), stateVote.getOwner());
			if (votePower > 0 && pendingState.vote(stateVote, votePower) == true)
				response = true;
		}
		finally
		{
			StatePool.this.lock.writeLock().unlock();
		}
		
		// See if threshold vote power is met, verify aggregate signatures and attempt to build a state certificate
		if (response == true)
		{
			// Don't build certificates from cast votes received until executed locally
			if (pendingAtom.getStatus().greaterThan(CommitStatus.PROVISIONED) == false)
				return response;

			if (pendingState.isPreverified() == true)
				return response;
			
			if (pendingState.preverify() == false)
				return response;
			
			if (pendingState.isVerified() == true)
				return response;
			
			if (pendingState.verify() == false)
				return response;

			if (pendingAtom.getCertificate() != null)
				return response;

			StateCertificate stateCertificate = pendingState.buildCertificate();
			if (stateCertificate != null)
			{
				if (statePoolLog.hasLevel(Logging.DEBUG) == true)
					statePoolLog.debug(StatePool.this.context.getName()+": State vote "+stateVote.getHash()+" processed for "+stateVote.getObject()+":"+stateVote.getAtom()+" by "+stateVote.getOwner()+" (CERTIFICATE)");

				StatePool.this.context.getEvents().post(new StateCertificateEvent(stateCertificate));
			}
		}
		
		return response;
	}
	
	public int size()
	{
		this.lock.readLock().lock();
		try
		{
			return this.states.size();
		}
		finally
		{
			this.lock.readLock().unlock();
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
				final List<Hash> removals = new ArrayList<Hash>();
				StatePool.this.votesToCountDelayed.forEach((h,sv) -> {
					if (sv.getHeight() <= blockCommittedEvent.getBlock().getHeader().getHeight())
					{
						StatePool.this.votesToCountQueue.put(sv.getHash(), sv);
						removals.add(h);
					}
				});
				synchronized(StatePool.this.votesToCountDelayed) { StatePool.this.votesToCountDelayed.keySet().removeAll(removals); }
				
				synchronized(StatePool.this.voteProcessor)
				{
					StatePool.this.voteProcessor.notify();
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
			// TODO disable to witness state pool saturation
			// 		In a liveness resolve situation vote power is being taken for pending state votes
			//		from when the liveness break was active.  
			//		The missing vote power needed for a threshold never voted, as that caused the liveness issue.
			//		The absence of that same vote power can cause state vote issues AFTER the liveness issue 
			//		is resolved, causing accepted atoms to fail en-mass.
			remove(event.getPendingAtom());
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
			StatePool.this.lock.writeLock().lock();
			try
			{
				if (event.isSynced() == true)
				{
					statePoolLog.info(StatePool.this.context.getName()+": Sync status changed to "+event.isSynced()+", loading known state pool state");
					for (long height = Math.max(0, StatePool.this.context.getLedger().getHead().getHeight() - Node.OOS_TRIGGER_LIMIT) ; height < StatePool.this.context.getLedger().getHead().getHeight() ; height++)
					{
						try
						{
							Collection<Hash> items = StatePool.this.context.getLedger().getLedgerStore().getSyncInventory(height, StateVote.class);
							for (Hash item : items)
							{
								StateVote stateVote = StatePool.this.context.getLedger().getLedgerStore().get(item, StateVote.class);
								Commit commit = StatePool.this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, stateVote.getAtom()));
								if (commit != null && commit.getPath().get(Elements.CERTIFICATE) != null)
									continue;
									
								PendingAtom pendingAtom = StatePool.this.context.getLedger().getAtomHandler().get(stateVote.getAtom());
								if (pendingAtom == null)
								{
									Atom atom = StatePool.this.context.getLedger().getLedgerStore().get(stateVote.getAtom(), Atom.class);
									StatePool.this.context.getLedger().getAtomHandler().submit(atom);
								}
								
								if (stateVote.getHeight() > StatePool.this.context.getLedger().getHead().getHeight())
									StatePool.this.votesToCountDelayed.put(stateVote.getHash(), stateVote);
								else
									StatePool.this.votesToSyncQueue.put(stateVote.getHash(), stateVote);
							}
						}
						catch (Exception ex)
						{
							statePoolLog.error(StatePool.this.context.getName()+": Failed to load state for state pool at height "+height, ex);
						}

						synchronized(StatePool.this.voteProcessor)
						{
							StatePool.this.voteProcessor.notify();
						}
					}					
				}
				else
				{
					statePoolLog.info(StatePool.this.context.getName()+": Sync status changed to "+event.isSynced()+", flushing state pool");
					StatePool.this.states.clear();
					StatePool.this.votesToCastQueue.clear();
					StatePool.this.votesToSyncQueue.clear();
					StatePool.this.votesToCountQueue.clear();
					StatePool.this.votesToCountDelayed.clear();
				}
			}
			finally
			{
				StatePool.this.lock.writeLock().unlock();
			}
		}
	};
}
