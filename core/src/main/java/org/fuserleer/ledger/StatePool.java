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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.collections.MappedBlockingQueue;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.LedgerStore.SyncInventoryType;
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.events.AtomPositiveCommitEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomExecutedEvent;
import org.fuserleer.ledger.events.AtomNegativeCommitEvent;
import org.fuserleer.ledger.events.StateCertificateEvent;
import org.fuserleer.ledger.events.SyncStatusChangeEvent;
import org.fuserleer.ledger.messages.SyncAcquiredMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.GossipFetcher;
import org.fuserleer.network.GossipFilter;
import org.fuserleer.network.GossipInventory;
import org.fuserleer.network.GossipReceiver;
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
	
	static 
	{
//		statePoolLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
		statePoolLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
//		statePoolLog.setLevels(Logging.ERROR | Logging.FATAL);
	}

	private enum StateVoteStatus
	{
		SUCCESS, FAILED, SKIPPED;
	}

	private final Context context;
	
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

						List<StateVote> stateVotesToBroadcast = new ArrayList<StateVote>();
						if (StatePool.this.votesToCountQueue.isEmpty() == false)
						{
							Entry<Hash, StateVote> stateVote;
							while((stateVote = StatePool.this.votesToCountQueue.peek()) != null)
							{
								try
								{
									if (StatePool.this.context.getLedger().getLedgerStore().store(StatePool.this.context.getLedger().getHead().getHeight(), stateVote.getValue()).equals(OperationStatus.SUCCESS) == false)
									{
										statePoolLog.warn(StatePool.this.context.getName()+": Already seen state vote of "+stateVote.getValue().getState()+" for atom "+stateVote.getValue().getAtom()+" in block "+stateVote.getValue().getBlock()+" by "+stateVote.getValue().getOwner());
										continue;
									}
									
									StateVoteStatus status = process(stateVote.getValue());
									if (status == StateVoteStatus.SUCCESS)
									{
										if (statePoolLog.hasLevel(Logging.DEBUG) == true)
											statePoolLog.debug(StatePool.this.context.getName()+": Processed state vote "+stateVote.getValue().getHash()+" for atom "+stateVote.getValue().getAtom()+" in block "+stateVote.getValue().getBlock()+" by "+stateVote.getValue().getOwner());

										stateVotesToBroadcast.add(stateVote.getValue());
									}
									else if (status == StateVoteStatus.SKIPPED)
									{
										if (statePoolLog.hasLevel(Logging.DEBUG) == true)
											statePoolLog.debug(StatePool.this.context.getName()+": Processing of state vote "+stateVote.getValue().getHash()+" was skipped for atom "+stateVote.getValue().getAtom()+" in block "+stateVote.getValue().getBlock()+" by "+stateVote.getValue().getOwner());
									}
									else
										statePoolLog.warn(StatePool.this.context.getName()+": Processing of state vote "+stateVote.getValue().getHash()+" returned false for atom "+stateVote.getValue().getAtom()+" in block "+stateVote.getValue().getBlock()+" by "+stateVote.getValue().getOwner());
								}
								catch (Exception ex)
								{
									statePoolLog.error(StatePool.this.context.getName()+": Error counting vote for " + stateVote.getValue(), ex);
								}
								finally
								{
									if (StatePool.this.votesToCountQueue.remove(stateVote.getKey(), stateVote.getValue()) == false)
										// FIXME sync state can initially flip/flop between ... annoying, so just throw these as warns for now (theres are in all queue handlers!)
										statePoolLog.warn(StatePool.this.context.getName()+": State pool vote peek/remove failed for "+stateVote.getValue());
//										throw new IllegalStateException("State pool vote peek/remove failed for "+stateVote.getValue());
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
									
									if (pendingState.voted(StatePool.this.context.getNode().getIdentity()) == true)
									{
										statePoolLog.warn(StatePool.this.context.getName()+": Already voted on pending state "+pendingState);
										StatePool.this.tryFinalize(pendingAtom, pendingState);
									}
									else
									{
										long localVotePower = StatePool.this.context.getLedger().getValidatorHandler().getVotePower(Math.max(0, pendingState.getHeight() - ValidatorHandler.VOTE_POWER_MATURITY), StatePool.this.context.getNode().getIdentity());
										if (pendingAtom.thrown() == null && pendingAtom.getExecution() == null)
											throw new IllegalStateException("Can not vote on state "+pendingState.getKey()+" when no decision made");
										
										if (localVotePower > 0)
										{
											Optional<UInt256> input = pendingAtom.getInput(pendingState.getKey());
											Optional<UInt256> output = pendingAtom.getOutput(pendingState.getKey());
											StateVote stateVote = new StateVote(pendingState.getKey(), pendingState.getAtom(), 
																				pendingState.getBlock(), StatePool.this.context.getLedger().getLedgerStore().get(pendingState.getBlock(), BlockHeader.class).getOwner(),
																	  			input == null ? null : input.orElse(null), output == null ? null : output.orElse(null),
																	  			pendingAtom.getExecution(), StatePool.this.context.getNode().getIdentity());
											stateVote.sign(StatePool.this.context.getNode().getKeyPair());
											
											if (StatePool.this.context.getLedger().getLedgerStore().store(StatePool.this.context.getLedger().getHead().getHeight(), stateVote).equals(OperationStatus.SUCCESS) == true)
											{
												StateVoteStatus status = process(stateVote);
												if (status == StateVoteStatus.SUCCESS)
												{
													if (statePoolLog.hasLevel(Logging.DEBUG))
														statePoolLog.debug(StatePool.this.context.getName()+": State vote "+stateVote.getHash()+" on "+pendingState.getKey()+" in atom "+pendingState.getAtom()+" with decision "+stateVote.getDecision());
																						
													stateVotesToBroadcast.add(stateVote);
												}
												else if (status == StateVoteStatus.SKIPPED)
													statePoolLog.error(StatePool.this.context.getName()+": State vote "+stateVote.getHash()+" on "+pendingState.getKey()+" in atom "+pendingState.getAtom()+" with decision "+stateVote.getDecision()+" was skipped");
												else
													statePoolLog.error(StatePool.this.context.getName()+": State vote "+stateVote.getHash()+" on "+pendingState.getKey()+" in atom "+pendingState.getAtom()+" with decision "+stateVote.getDecision()+" return false");
	
											}
											else // FIXME happens usually on sync after state reconstruction as local validator doesn't know its voted already
												statePoolLog.error(StatePool.this.context.getName()+": Local state vote of "+stateVote.getState()+" for "+stateVote.getOwner()+" failed");
										}
										else
											StatePool.this.tryFinalize(pendingAtom, pendingState);
									}
								}
								catch (Exception ex)
								{
									statePoolLog.error(StatePool.this.context.getName()+": Error casting vote for " + pendingState.getKey(), ex);
								}
								finally
								{
									if (pendingState.equals(StatePool.this.votesToCastQueue.poll()) == false)
										// FIXME sync state can initially flip/flop between ... annoying, so just throw these as warns for now (theres are in all queue handlers!)
										statePoolLog.warn(StatePool.this.context.getName()+": State pool vote cast peek/remove failed for "+pendingState.getKey());
//										throw new IllegalStateException("State pool vote cast peek/pool failed for "+pendingState.getKey());
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
						
						// Delayed state votes
						final List<Hash> voteRemovals = new ArrayList<Hash>();
						StatePool.this.votesToCountDelayed.forEach((h,sv) -> 
						{
							PendingAtom pendingAtom;
							try
							{
								pendingAtom = StatePool.this.context.getLedger().getAtomHandler().get(sv.getAtom());
								if (sv.getHeight() <= StatePool.this.context.getLedger().getHead().getHeight() && 
									pendingAtom.getStatus().greaterThan(CommitStatus.PREPARED) == true)
								{
									StatePool.this.votesToCountQueue.put(sv.getHash(), sv);
									voteRemovals.add(h);
								}
							}
							catch (IOException ioex)
							{
								statePoolLog.error(StatePool.this.context.getName()+": Maintenence of delayed state votes failed for pending atom "+sv.getAtom(), ioex);
							}
						});
						synchronized(StatePool.this.votesToCountDelayed) { StatePool.this.votesToCountDelayed.keySet().removeAll(voteRemovals); }
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
	private final MappedBlockingQueue<Hash, StateVote> votesToCountQueue;
	
	StatePool(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		
		this.votesToCastQueue = new LinkedBlockingQueue<PendingState>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.votesToCountQueue = new MappedBlockingQueue<Hash, StateVote>(this.context.getConfiguration().get("ledger.state.queue", 1<<20));
		this.votesToCountDelayed = Collections.synchronizedMap(new HashMap<Hash, StateVote>());
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
				
				// Check existence of StateVote ... primary cause of this evaluating to true is that 
				// the received StateVote is the local nodes.
				// Syncing from a clean slate may result in the local node voting for a state in 
				// the pool, not knowing it already voted previously until it receives the vote from
				// a sync peer.  The duplicate will get caught in the votesToCountQueue processor
				// outputting a lot of warnings which is undesirable.
				if (StatePool.this.votesToCountDelayed.containsKey(stateVote.getHash()) == true || 
					StatePool.this.votesToCountQueue.contains(stateVote.getHash()) == true || 
					StatePool.this.context.getLedger().getLedgerStore().has(stateVote.getHash()) == true)
					return;

				PendingAtom pendingAtom = StatePool.this.context.getLedger().getAtomHandler().get(stateVote.getAtom());
				if (stateVote.getHeight() <= StatePool.this.context.getLedger().getHead().getHeight() && 
					pendingAtom.getStatus().greaterThan(CommitStatus.PREPARED) == true)
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
						long numShardGroups = StatePool.this.context.getLedger().numShardGroups();
						long localShardGroup = ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), numShardGroups);
						long remoteShardGroup = ShardMapper.toShardGroup(peer.getNode().getIdentity(), numShardGroups);

						if (remoteShardGroup != localShardGroup)
						{
							statePoolLog.error(StatePool.this.context.getName()+": Received SyncAcquiredMessage from "+peer+" in shard group "+remoteShardGroup+" but local is "+localShardGroup);
							// Disconnect and ban?
							return;
						}
						
						StatePool.this.lock.readLock().lock();
						try
						{
							if (statePoolLog.hasLevel(Logging.DEBUG) == true)
								statePoolLog.debug(StatePool.this.context.getName()+": State pool (votes) inventory request from "+peer);
							
							final Set<PendingState> pendingStates = new HashSet<PendingState>(StatePool.this.context.getLedger().getStateHandler().getAll());
							final Set<Hash> stateVoteInventory = new LinkedHashSet<Hash>();
							StatePool.this.votesToCountQueue.forEach((h, v) -> stateVoteInventory.add(h));
							StatePool.this.votesToCountDelayed.forEach((h, v) -> stateVoteInventory.add(h));
							for (PendingState pendingState : pendingStates)
							{
								for (StateVote stateVote : pendingState.votes())
									stateVoteInventory.add(stateVote.getHash());
							}
							
							long height = StatePool.this.context.getLedger().getHead().getHeight();
							while (height >= Math.max(0, syncAcquiredMessage.getHead().getHeight() -1))
							{
								stateVoteInventory.addAll(StatePool.this.context.getLedger().getLedgerStore().getSyncInventory(height, StateVote.class, SyncInventoryType.COMMIT));
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
							statePoolLog.error(StatePool.this.context.getName()+":  ledger.messages.sync.acquired " + peer, ex);
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
		this.context.getEvents().unregister(this.syncChangeListener);
		
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	void vote(final PendingAtom atom) throws IOException
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
				
				PendingState pendingState = this.context.getLedger().getStateHandler().get(atom.getBlock(), atom.getHash(), stateKey);
				if (pendingState == null)
					throw new IllegalStateException("Expected pending state "+stateKey+" in atom "+atom.getHash()+" not found");
				
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
	
	private void committed(final PendingAtom pendingAtom) throws IOException
	{
		remove(pendingAtom);
	}

	private void aborted(final PendingAtom pendingAtom) throws IOException
	{
		remove(pendingAtom);
	}
	
	private void remove(final PendingAtom pendingAtom) throws IOException
	{
		Objects.requireNonNull(pendingAtom, "Atom is null");
		
		this.lock.writeLock().lock();
		try
		{
			long numShardGroups = this.context.getLedger().numShardGroups(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()));
			long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			final Collection<StateKey<?, ?>> stateKeys = pendingAtom.getStateKeys();
			for (StateKey<?, ?> stateKey : stateKeys)
			{
				long stateShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
				if (stateShardGroup != localShardGroup)
					continue;
				
				PendingState pendingState = this.context.getLedger().getStateHandler().get(pendingAtom.getBlock(), pendingAtom.getHash(), stateKey);
				if (pendingState != null)
				{
					for (StateVote stateVote : pendingState.votes())
						this.context.getLedger().getLedgerStore().storeSyncInventory(this.context.getLedger().getHead().getHeight(), stateVote.getHash(), StateVote.class, SyncInventoryType.COMMIT);

					this.votesToCastQueue.remove(pendingState);

					if (statePoolLog.hasLevel(Logging.DEBUG) == true)
						statePoolLog.debug(this.context.getName()+": Removed state "+stateKey+" for "+pendingAtom+" in block "+pendingAtom.getBlock());
				}
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

			this.votesToCountQueue.removeAll(removals);
			synchronized(this.votesToCountDelayed) { this.votesToCountDelayed.keySet().removeAll(removals); }
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	
	private StateVoteStatus process(final StateVote stateVote) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(stateVote, "State vote is null");
		
		StatePool.this.lock.writeLock().lock();
		StateVoteStatus response = StateVoteStatus.FAILED;
		PendingState pendingState = null;
		PendingAtom pendingAtom = null;
		try
		{
			pendingAtom = StatePool.this.context.getLedger().getAtomHandler().get(stateVote.getAtom());
			if (pendingAtom == null)
			{
				Commit commit = StatePool.this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, stateVote.getAtom()));
				if (commit != null && commit.getPath().get(Elements.CERTIFICATE) != null)
					return StateVoteStatus.SKIPPED;

				throw new IllegalStateException("Pending atom "+stateVote.getAtom()+" for pending state "+stateVote.getObject()+" not found");
			}
			
			pendingState = this.context.getLedger().getStateHandler().get(pendingAtom.getBlock(), pendingAtom.getHash(), stateVote.getState());
			// Creating pending state objects from vote if particle not seen or committed
			if (pendingState == null)
			{
				// Atom is known, pending state is not yet created
				pendingState = this.context.getLedger().getStateHandler().get(stateVote.getBlock(), stateVote.getAtom(), stateVote.getState());
				if (pendingState == null)
					throw new IllegalStateException("Pending state "+stateVote.getState()+" for atom "+stateVote.getAtom()+" in block "+stateVote.getBlock()+" not found or is committed / timedout");
			}

			long votePower = StatePool.this.context.getLedger().getValidatorHandler().getVotePower(Math.max(0, pendingState.getHeight() - ValidatorHandler.VOTE_POWER_MATURITY), stateVote.getOwner());
			if (votePower > 0 && pendingState.vote(stateVote, votePower) == true)
				response = StateVoteStatus.SUCCESS;
		}
		finally
		{
			StatePool.this.lock.writeLock().unlock();
		}
		
		tryFinalize(pendingAtom, pendingState);
		return response;
	}
	
	private boolean tryFinalize(final PendingAtom pendingAtom, final PendingState pendingState) throws IOException, CryptoException
	{
		// Don't build certificates from cast votes received until executed locally
		if (pendingAtom.getStatus().greaterThan(CommitStatus.PROVISIONED) == false)
			return false;

		if (pendingState.isPreverified() == true)
			return false;
		
		if (pendingState.preverify() == false)
			return false;
		
		if (pendingState.isVerified() == true)
			return false;
		
		if (pendingState.verify() == false)
			return false;

		if (pendingAtom.getCertificate() != null)
			return false;

		StateCertificate stateCertificate = pendingState.buildCertificate();
		if (stateCertificate != null)
		{
			if (statePoolLog.hasLevel(Logging.DEBUG) == true)
				statePoolLog.debug(StatePool.this.context.getName()+": State certificate "+stateCertificate.getHash()+" constructed for "+stateCertificate.getObject()+":"+stateCertificate.getAtom());

			StatePool.this.context.getEvents().post(new StateCertificateEvent(stateCertificate));
			return true;
		}
		
		return false;
	}
	
	// SYNCHRONOUS ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomExecutedEvent event) throws IOException 
		{
			vote(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event) throws IOException
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
		public void on(final AtomPositiveCommitEvent event) throws IOException 
		{
			committed(event.getPendingAtom());
		}
		
		@Subscribe
		public void on(final AtomNegativeCommitEvent event) throws IOException 
		{
			if (event.getPendingAtom().getStatus().equals(CommitStatus.EXECUTED) == false)
				return;

			committed(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomExceptionEvent event) throws IOException 
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
					for (long height = Math.max(0, StatePool.this.context.getLedger().getHead().getHeight() - Node.OOS_TRIGGER_LIMIT) ; height <= StatePool.this.context.getLedger().getHead().getHeight() ; height++)
					{
						try
						{
							Collection<Hash> items = StatePool.this.context.getLedger().getLedgerStore().getSyncInventory(height, StateVote.class, SyncInventoryType.SEEN);
							for (Hash item : items)
							{
								StateVote stateVote = StatePool.this.context.getLedger().getLedgerStore().get(item, StateVote.class);
								PendingAtom pendingAtom = StatePool.this.context.getLedger().getAtomHandler().load(stateVote.getAtom());
								if (pendingAtom == null)
									continue;
								
								if (stateVote.getHeight() > StatePool.this.context.getLedger().getHead().getHeight())
									StatePool.this.votesToCountDelayed.put(stateVote.getHash(), stateVote);
								else
								{
									StateVoteStatus status = process(stateVote);
									if (status == StateVoteStatus.SKIPPED)
									{
										if (statePoolLog.hasLevel(Logging.DEBUG) == true)
											statePoolLog.debug(StatePool.this.context.getName()+": Syncing of state vote "+stateVote.getHash()+" was skipped for atom "+stateVote.getAtom()+" in block "+stateVote.getBlock()+" by "+stateVote.getOwner());
									}
									else if (status == StateVoteStatus.FAILED)
										statePoolLog.warn(StatePool.this.context.getName()+": Syncing of state vote "+stateVote.getHash()+" failed for atom "+stateVote.getAtom()+" in block "+stateVote.getBlock()+" by "+stateVote.getOwner());
								}
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
					StatePool.this.votesToCastQueue.clear();
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
