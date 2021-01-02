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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.common.Direction;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.database.Indexable;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomCommittedEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.events.StateCertificateEvent;
import org.fuserleer.ledger.messages.GetStateVoteMessage;
import org.fuserleer.ledger.messages.InventoryMessage;
import org.fuserleer.ledger.messages.StateCertificateMessage;
import org.fuserleer.ledger.messages.StateVoteInventoryMessage;
import org.fuserleer.ledger.messages.StateVoteMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.discovery.RemoteShardDiscovery;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.filters.OutboundUDPPeerFilter;
import org.fuserleer.node.Node;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.UInt128;
import org.fuserleer.utils.UInt256;

import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public final class StatePool implements Service
{
	private static final Logger cerbyLog = Logging.getLogger("cerby");

	private final Context context;
	
	private class PendingState implements Hashable
	{
		private final Hash	hash;
		private final Hash	block;
		private	final Hash 	atom;
		private Set<StateOp>	stateOps;
		private StateCertificate certificate;

		private long		positiveVoteWeight;
		private long		negativeVoteWeight;
		private final Map<ECPublicKey, StateVote> votes;

		public PendingState(Hash particle, Hash atom, Hash block)
		{
			this.hash = Objects.requireNonNull(particle);
			this.atom = Objects.requireNonNull(atom);
			this.block = Objects.requireNonNull(block);
			this.positiveVoteWeight = 0l;
			this.negativeVoteWeight = 0l;
			this.votes = Collections.synchronizedMap(new HashMap<ECPublicKey, StateVote>());
		}

		@Override
		public Hash getHash()
		{
			return this.hash;
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
		
		public Set<StateOp> getStateOps()
		{
			return this.stateOps;
		}

		public StateCertificate getCertificate()
		{
			return this.certificate;
		}

		void setStateOps(Set<StateOp> stateOps)
		{
			Objects.requireNonNull(stateOps);
			if (stateOps.stream().allMatch(sop -> sop.key().equals(this.hash)) == false)
				throw new IllegalArgumentException("State ops does not match state hash "+this.hash+" "+stateOps);
			
			this.stateOps = stateOps;
		}

		void setCertificate(StateCertificate certificate)
		{
			if (Objects.requireNonNull(certificate).getState().equals(this.hash) == false)
				throw new IllegalArgumentException("Certificate does not match state hash "+this.hash+" "+certificate);
			
			this.certificate = certificate;
		}

		@Override
		public int hashCode()
		{
			return this.hash.hashCode();
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
			return this.hash+" @ "+Longs.fromByteArray(this.block.toByteArray());
		}
		
		public boolean voted(ECPublicKey identity)
		{
			return this.votes.containsKey(Objects.requireNonNull(identity, "Vote identity is null"));
		}

		public boolean vote(final StateVote vote, final long weight) throws ValidationException
		{
			synchronized(this.votes)
			{
				if (this.votes.containsKey(Objects.requireNonNull(vote, "Vote is null").getOwner()) == false)
				{
					if (vote.getAtom().equals(this.atom) == false || 
						vote.getBlock().equals(this.block) == false || 
						vote.getState().equals(this.hash) == false)
						throw new ValidationException("Vote from "+vote.getOwner()+" is not for state "+this.getHash()+" -> "+this.atom+" -> "+this.block);
					
					this.votes.put(vote.getOwner(), vote);
					
					if (vote.getDecision() == true)
						this.positiveVoteWeight += weight;
					else
						this.negativeVoteWeight += weight;
					
					return true;
				}
				else
				{
					cerbyLog.warn(StatePool.this.context.getName()+": "+vote.getOwner()+" has already cast a vote for "+this.hash);
					return false;
				}
			}
		}
		
		public Collection<StateVote> votes(boolean decision)
		{
			return this.votes.values().stream().filter(v -> v.getDecision() == decision).collect(Collectors.toList());
		}

		public long power(boolean decision)
		{
			if (decision == true)
				return this.positiveVoteWeight;
			
			return this.negativeVoteWeight;
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
						PendingState pendingState = StatePool.this.voteQueue.poll(1, TimeUnit.SECONDS);
						if (pendingState != null && pendingState.getStateOps() != null)
						{
							if (cerbyLog.hasLevel(Logging.DEBUG))
								cerbyLog.debug(StatePool.this.context.getName()+": Voting on state "+pendingState.getHash());

							try
							{
								Atom atom = StatePool.this.context.getLedger().getLedgerStore().get(pendingState.getAtom(), Atom.class);
								if (atom == null)
								{
									cerbyLog.warn(StatePool.this.context.getName()+": Atom "+pendingState.getAtom()+" required to vote on state "+pendingState.getHash()+" not found");
									continue;
								}
								
								// Dont vote if we have no power!
								long localVotePower = StatePool.this.context.getLedger().getVoteRegulator().getVotePower(StatePool.this.context.getNode().getIdentity());
								if (localVotePower > 0)
								{
									// Determine the decision via state machine
									boolean decision = false;
									try
									{
										StateMachine stateMachine = new StateMachine(StatePool.this.context, StatePool.this.context.getLedger().getHead(), atom, StatePool.this.context.getLedger().getStateAccumulator());
										stateMachine.precommit();
										decision = true;
									}
									catch (ValidationException vex)
									{
										cerbyLog.error(StatePool.this.context.getName()+": Validation of pending state "+pendingState.getHash()+" failed", vex);
									}
									
									StateVote stateVote = new StateVote(pendingState.getHash(), pendingState.getAtom(), pendingState.getBlock(), decision, StatePool.this.context.getNode().getIdentity());
									stateVote.sign(StatePool.this.context.getNode().getKey());
									StatePool.this.context.getLedger().getLedgerStore().store(stateVote);

									pendingState.vote(stateVote, localVotePower);
									
									StateVoteMessage stateVoteMessage = new StateVoteMessage(stateVote);
									// TODO only broadcast this to validators in same shard!
									for (ConnectedPeer connectedPeer : StatePool.this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED))
									{
										if (StatePool.this.context.getNode().isInSyncWith(connectedPeer.getNode(), Node.OOS_TRIGGER_LIMIT) == false)
											continue;
										
										try
										{
											StatePool.this.context.getNetwork().getMessaging().send(stateVoteMessage, connectedPeer);
										}
										catch (IOException ex)
										{
											cerbyLog.error(StatePool.this.context.getName()+": Unable to send StateVoteMessage for "+pendingState.getHash()+" to "+connectedPeer, ex);
										}
									}

									buildCertificate(pendingState);
								}
							}
							catch (Exception ex)
							{
								cerbyLog.error(StatePool.this.context.getName()+": Error processing vote for " + pendingState.getHash(), ex);
							}
						}
						
						pendingState = StatePool.this.certificateQueue.poll(1, TimeUnit.SECONDS);
						if (pendingState != null && pendingState.getStateOps() != null && pendingState.getCertificate() != null)
						{
							if (cerbyLog.hasLevel(Logging.DEBUG))
								cerbyLog.debug(StatePool.this.context.getName()+": Broadcasting state certificate "+pendingState.getHash());
							
							try
							{
								Atom atom = StatePool.this.context.getLedger().getLedgerStore().get(pendingState.getAtom(), Atom.class);
								if (atom == null)
								{
									cerbyLog.warn(StatePool.this.context.getName()+": Atom "+pendingState.getAtom()+" required to broadcast state certificate "+pendingState.getHash()+" not found");
									continue;
								}
	
								// TODO inventorize this
								for (UInt256 shard : atom.getShards())
			                	{
			                		UInt128 shardGroup = StatePool.this.context.getLedger().getShardGroup(shard);
			                		if (shardGroup.compareTo(StatePool.this.context.getLedger().getShardGroup(StatePool.this.context.getNode().getIdentity())) == 0)
			                			continue;
			                		
			                		StateCertificateMessage stateCertificateMessage = new StateCertificateMessage(pendingState.getCertificate());
			        				OutboundUDPPeerFilter outboundUDPPeerFilter = new OutboundUDPPeerFilter(StatePool.this.context, Collections.singleton(shardGroup));
		        					Collection<Peer> preferred = new RemoteShardDiscovery(StatePool.this.context).discover(outboundUDPPeerFilter);
		        					for (Peer preferredPeer : preferred)
		        					{
		        						try
		        						{
		        							ConnectedPeer connectedPeer = StatePool.this.context.getNetwork().connect(preferredPeer.getURI(), Direction.OUTBOUND, Protocol.UDP);
		        							StatePool.this.context.getNetwork().getMessaging().send(stateCertificateMessage, connectedPeer);
		        						}
		        						catch (IOException ex)
		        						{
		        							cerbyLog.error(StatePool.this.context.getName()+": Unable to send StateCertificateMessage for "+pendingState.getHash()+" to " + preferredPeer, ex);
		        						}
		        					}
			                	}
							}
							catch (Exception ex)
							{
								cerbyLog.error(StatePool.this.context.getName()+": Error broadcasting certificate for " + pendingState.getHash(), ex);
							}
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
				cerbyLog.fatal(StatePool.this.context.getName()+": Error processing particle vote queue", throwable);
			}
		}
	};

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Map<Hash, PendingState> pending = new HashMap<Hash, PendingState>();
	private final BlockingQueue<PendingState> voteQueue;
	private final BlockingQueue<PendingState> certificateQueue;
	
	// Atom vote broadcast batching
	private final BlockingQueue<Hash> stateVoteInventory = new LinkedBlockingQueue<Hash>();
	private final Executable stateVoteInventoryProcessor = new Executable() 
	{
		@Override
		public void execute()
		{
			while(isTerminated() == false)
			{
				try
				{
					Hash hash = StatePool.this.stateVoteInventory.poll(1, TimeUnit.SECONDS);
					if (hash == null)
						continue;
					
					List<Hash> stateVoteInventory = new ArrayList<Hash>();
					stateVoteInventory.add(hash);
					StatePool.this.stateVoteInventory.drainTo(stateVoteInventory, InventoryMessage.MAX_INVENTORY-1);
	
					if (StatePool.this.context.getLedger().isSynced() == true)
					{
						StateVoteInventoryMessage stateVoteInventoryMessage = new StateVoteInventoryMessage(stateVoteInventory);
						for (ConnectedPeer connectedPeer : StatePool.this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED))
						{
							try
							{
								StatePool.this.context.getNetwork().getMessaging().send(stateVoteInventoryMessage, connectedPeer);
							}
							catch (IOException ex)
							{
								cerbyLog.error(StatePool.this.context.getName()+": Unable to send StateVoteInventoryMessage of "+stateVoteInventoryMessage.getInventory().size()+" state votes to "+connectedPeer, ex);
							}
						}
					}
				}
				catch (Exception ex)
				{
					cerbyLog.error(StatePool.this.context.getName()+": Processing of state vote inventory failed", ex);
				}
			}
		}
	};


	StatePool(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		
		this.voteQueue = new LinkedBlockingQueue<PendingState>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.certificateQueue = new LinkedBlockingQueue<PendingState>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
	}

	@Override
	public void start() throws StartupException
	{
		this.context.getNetwork().getMessaging().register(StateVoteInventoryMessage.class, this.getClass(), new MessageProcessor<StateVoteInventoryMessage>()
		{
			@Override
			public void process(final StateVoteInventoryMessage stateVoteInventoryMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						final List<Hash> stateVoteInventoryRequired = new ArrayList<Hash>();

						try
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StatePool.this.context.getName()+": State votes inventory for "+stateVoteInventoryMessage.getInventory().size()+" votes from " + peer);

							if (stateVoteInventoryMessage.getInventory().size() == 0)
							{
								cerbyLog.error(StatePool.this.context.getName()+": Received empty state votes inventory from " + peer);
								// TODO disconnect and ban
								return;
							}

							// TODO proper request process that doesn't produce duplicate requests see AtomHandler
							for (Hash stateVoteHash : stateVoteInventoryMessage.getInventory())
							{
								if (StatePool.this.context.getLedger().getLedgerStore().has(stateVoteHash) == true)
									continue;
								
								stateVoteInventoryRequired.add(stateVoteHash);
							}
							
							if (stateVoteInventoryRequired.isEmpty() == true)
								return;
							
							StatePool.this.context.getNetwork().getMessaging().send(new GetStateVoteMessage(stateVoteInventoryRequired), peer);
						}
						catch (Exception ex)
						{
							cerbyLog.error(StatePool.this.context.getName()+": Unable to send ledger.messages.state.vote.get for "+stateVoteInventoryRequired.size()+" state votes to "+peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(GetStateVoteMessage.class, this.getClass(), new MessageProcessor<GetStateVoteMessage>()
		{
			@Override
			public void process(final GetStateVoteMessage getStateVoteMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StatePool.this.context.getName()+": State votes request for "+getStateVoteMessage.getInventory().size()+" votes from " + peer);
							
							if (getStateVoteMessage.getInventory().size() == 0)
							{
								cerbyLog.error(StatePool.this.context.getName()+": Received empty state votes request from " + peer);
								// TODO disconnect and ban
								return;
							}
							
							for (Hash stateVoteHash : getStateVoteMessage.getInventory())
							{
								StateVote stateVote = StatePool.this.context.getLedger().getLedgerStore().get(stateVoteHash, StateVote.class);
								if (stateVote == null)
								{
									if (cerbyLog.hasLevel(Logging.DEBUG) == true)
										cerbyLog.debug(StatePool.this.context.getName()+": Requested state vote not found "+stateVoteHash+" for " + peer);
									
									continue;
								}

								try
								{
									StatePool.this.context.getNetwork().getMessaging().send(new StateVoteMessage(stateVote), peer);
								}
								catch (IOException ex)
								{
									cerbyLog.error(StatePool.this.context.getName()+": Unable to send StateVoteMessage for "+stateVote+" to "+peer, ex);
								}
							}
						}
						catch (Exception ex)
						{
							cerbyLog.error(StatePool.this.context.getName()+": ledger.messages.state.vote.get " + peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(StateVoteMessage.class, this.getClass(), new MessageProcessor<StateVoteMessage>()
		{
			@Override
			public void process(final StateVoteMessage stateVoteMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StatePool.this.context.getName()+": State vote "+stateVoteMessage.getVote().getObject()+" for "+stateVoteMessage.getVote().getOwner()+" from " + peer);
							
							if (StatePool.this.context.getLedger().getShardGroup(stateVoteMessage.getVote().getState()).compareTo(StatePool.this.context.getLedger().getShardGroup(StatePool.this.context.getNode().getIdentity())) != 0)
							{
								cerbyLog.error(StatePool.this.context.getName()+": Received state vote "+stateVoteMessage.getVote().getObject()+" not for local shard from " + peer);
								// Disconnected and ban
								return;
							}

							if (stateVoteMessage.getVote().verify(stateVoteMessage.getVote().getOwner()) == false)
							{
								cerbyLog.error(StatePool.this.context.getName()+": State vote failed verification for "+stateVoteMessage.getVote().getOwner()+" from " + peer);
								return;
							}
							
							if (OperationStatus.KEYEXIST.equals(StatePool.this.context.getLedger().getLedgerStore().store(stateVoteMessage.getVote())) == false)
							{
								// Creating pending state objects from vote if particle not seen
								StatePool.this.lock.writeLock().lock();
								try
								{
									PendingState pendingState = StatePool.this.pending.get(stateVoteMessage.getVote().getState());
									CommitState commitState = StatePool.this.context.getLedger().getStateAccumulator().has(Indexable.from(stateVoteMessage.getVote().getState(), Particle.class));
									if (pendingState == null && commitState.index() < CommitState.COMMITTED.index())
									{
										pendingState = new PendingState(stateVoteMessage.getVote().getState(), stateVoteMessage.getVote().getAtom(), stateVoteMessage.getVote().getBlock());
										add(pendingState);
									}

									if (pendingState == null)
										return;
									
									if (commitState.index() == CommitState.COMMITTED.index())
									{
										cerbyLog.warn(StatePool.this.context.getName()+": State "+stateVoteMessage.getVote().getState()+" is already committed");
										remove(StatePool.this.context.getLedger().getLedgerStore().get(pendingState.getAtom(), Atom.class));
										return;
									}
									
									if (pendingState.getBlock().equals(stateVoteMessage.getVote().getBlock()) == false || 
										pendingState.getAtom().equals(stateVoteMessage.getVote().getAtom()) == false)
									{
										cerbyLog.error(StatePool.this.context.getName()+": State vote "+stateVoteMessage.getVote().getState()+" block or atom dependencies not as expected for "+stateVoteMessage.getVote().getOwner()+" from " + peer);
										return;
									}
											
									if (pendingState.voted(stateVoteMessage.getVote().getOwner()) == true)
										return;
										
									pendingState.vote(stateVoteMessage.getVote(), StatePool.this.context.getLedger().getVoteRegulator().getVotePower(stateVoteMessage.getVote().getOwner()));
									if (pendingState.getCertificate() != null)
										return;
									
									buildCertificate(pendingState);
								}
								finally
								{
									StatePool.this.lock.writeLock().unlock();
								}
								
								// Independent so doesn't need to sit in a lock
								StatePool.this.stateVoteInventory.add(stateVoteMessage.getVote().getHash());
							}
							else
								cerbyLog.warn(StatePool.this.context.getName()+": Received already seen state vote of "+stateVoteMessage.getVote().getObject()+" for "+stateVoteMessage.getVote().getOwner()+" from " + peer);
						}
						catch (Exception ex)
						{
							cerbyLog.error(StatePool.this.context.getName()+": ledger.messages.state.vote " + peer, ex);
						}
					}
				});
			}
		});
		

		this.context.getEvents().register(this.syncBlockListener);
		this.context.getEvents().register(this.syncAtomListener);
		
		Thread stateVoteInventoryProcessorThread = new Thread(this.stateVoteInventoryProcessor);
		stateVoteInventoryProcessorThread.setDaemon(true);
		stateVoteInventoryProcessorThread.setName(this.context.getName()+" State Vote Inventory Processor");
		stateVoteInventoryProcessorThread.start();
		
		Thread voteProcessorThread = new Thread(this.voteProcessor);
		voteProcessorThread.setDaemon(true);
		voteProcessorThread.setName(this.context.getName()+" State Vote Processor");
		voteProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException
	{
		this.stateVoteInventoryProcessor.terminate(true);
		this.voteProcessor.terminate(true);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	private void add(Atom atom, Hash block)
	{
		Objects.requireNonNull(atom);

		this.lock.writeLock().lock();
		try
		{
			// TODO shard filtering
			for (Hash state : atom.getStates())
			{
				if (StatePool.this.context.getLedger().getShardGroup(state).compareTo(StatePool.this.context.getLedger().getShardGroup(StatePool.this.context.getNode().getIdentity())) != 0)
					continue;
				
				PendingState pendingState = this.pending.get(state);
				if (pendingState == null || pendingState.getStateOps() == null)
				{
					if (pendingState == null)
					{
						pendingState = new PendingState(state, atom.getHash(), block);
						add(pendingState);
					}
					
					if (pendingState.getStateOps() == null)
					{
						Set<StateOp> stateOps = new LinkedHashSet<StateOp>();
						atom.getStateOps().forEach(sop -> {
							if (sop.key().equals(state) == true)
								stateOps.add(sop);
						});
						pendingState.setStateOps(stateOps);
						
						this.voteQueue.add(pendingState);
					}
				}
			}			
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	private void add(final PendingState pendingState)
	{
		Objects.requireNonNull(pendingState);

		this.lock.writeLock().lock();
		try
		{
			this.pending.computeIfAbsent(pendingState.getHash(), ps -> 
			{
				StatePool.this.context.getMetaData().increment("ledger.pool.state.added");

				if (cerbyLog.hasLevel(Logging.DEBUG) == true) 
					cerbyLog.debug(StatePool.this.context.getName()+": "+pendingState+" added to pending pool, size is now "+this.pending.size());
				
				return pendingState;
			});
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	private void remove(final Atom atom)
	{
		Objects.requireNonNull(atom);

		this.lock.writeLock().lock();
		try
		{
			for (Hash state : atom.getStates())
				this.pending.remove(state);
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	private void buildCertificate(PendingState pendingState) throws CryptoException
	{
		Objects.requireNonNull(pendingState);
		if (pendingState.getCertificate() != null)
			return;

		this.lock.writeLock().lock();
		try
		{
			StateCertificate certificate = null;
			UInt128 shardGroup = StatePool.this.context.getLedger().getShardGroup(StatePool.this.context.getNode().getIdentity());
			long voteThresold = StatePool.this.context.getLedger().getVoteRegulator().getVotePowerThreshold(Collections.singleton(shardGroup));
			if (pendingState.power(true) >= voteThresold)
			{
				cerbyLog.info(StatePool.this.context.getName()+": State "+pendingState.getHash()+" has positive agreement with "+pendingState.power(true)+"/"+StatePool.this.context.getLedger().getVoteRegulator().getTotalVotePower(Collections.singleton(shardGroup)));
				certificate = new StateCertificate(pendingState.getHash(), pendingState.getAtom(), pendingState.getBlock(), true, 
													  StatePool.this.context.getLedger().getVoteRegulator().getVotePowerBloom(shardGroup),
													  pendingState.votes(true));
			}
			else if (pendingState.power(false) >= voteThresold)
			{
				cerbyLog.info(StatePool.this.context.getName()+": State "+pendingState.getHash()+" has negative agreement with "+pendingState.power(false)+"/"+StatePool.this.context.getLedger().getVoteRegulator().getTotalVotePower(Collections.singleton(shardGroup)));
				certificate = new StateCertificate(pendingState.getHash(), pendingState.getAtom(), pendingState.getBlock(), false, 
													  StatePool.this.context.getLedger().getVoteRegulator().getVotePowerBloom(shardGroup),
													  pendingState.votes(false));
			}
			
			if (certificate != null)
			{
				pendingState.setCertificate(certificate);
				this.certificateQueue.add(pendingState);
				StatePool.this.context.getEvents().post(new StateCertificateEvent(certificate));
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	// SYNCHRONOUS ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomCommitTimeoutEvent atomCommitTimeoutEvent) 
		{
			remove(atomCommitTimeoutEvent.getAtom());
		}

		@Subscribe
		public void on(final AtomCommittedEvent atomCommittedEvent) 
		{
			remove(atomCommittedEvent.getAtom());
		}
	};

	// SYNCHRONOUS ATOM LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final BlockCommittedEvent blockCommittedEvent) 
		{
			for (Atom atom : blockCommittedEvent.getBlock().getAtoms())
				add(atom, blockCommittedEvent.getBlock().getHeader().getHash());
		}
	};
}
