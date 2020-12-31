package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.fuserleer.ledger.atoms.ParticleCertificate;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomCommittedEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.events.ParticleCertificateEvent;
import org.fuserleer.ledger.messages.GetParticleVoteMessage;
import org.fuserleer.ledger.messages.InventoryMessage;
import org.fuserleer.ledger.messages.ParticleCertificateMessage;
import org.fuserleer.ledger.messages.ParticleVoteInventoryMessage;
import org.fuserleer.ledger.messages.ParticleVoteMessage;
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
		private Particle	particle;
		private ParticleCertificate	certificate;

		private long		positiveVoteWeight;
		private long		negativeVoteWeight;
		private final Map<ECPublicKey, ParticleVote> votes;

		public PendingState(Hash particle, Hash atom, Hash block)
		{
			this.hash = Objects.requireNonNull(particle);
			this.atom = Objects.requireNonNull(atom);
			this.block = Objects.requireNonNull(block);
			this.positiveVoteWeight = 0l;
			this.negativeVoteWeight = 0l;
			this.votes = Collections.synchronizedMap(new HashMap<ECPublicKey, ParticleVote>());
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
		
		public Particle getParticle()
		{
			return this.particle;
		}

		public ParticleCertificate getCertificate()
		{
			return this.certificate;
		}

		void setParticle(Particle particle)
		{
			if (Objects.requireNonNull(particle).getHash().equals(this.hash) == false)
				throw new IllegalArgumentException("Particle does not match hash "+this.hash+" "+particle);
			
			this.particle = particle;
		}

		void setCertificate(ParticleCertificate certificate)
		{
			if (Objects.requireNonNull(certificate).getParticle().equals(this.hash) == false)
				throw new IllegalArgumentException("Certificate does not match hash "+this.hash+" "+particle);
			
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

		public boolean vote(final ParticleVote vote, final long weight) throws ValidationException
		{
			synchronized(this.votes)
			{
				if (this.votes.containsKey(Objects.requireNonNull(vote, "Vote is null").getOwner()) == false)
				{
					if (vote.getAtom().equals(this.atom) == false || 
						vote.getBlock().equals(this.block) == false || 
						vote.getParticle().equals(this.hash) == false)
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
		
		public Collection<ParticleVote> votes(boolean decision)
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
						if (pendingState != null && pendingState.getParticle() != null)
						{
							if (cerbyLog.hasLevel(Logging.DEBUG))
								cerbyLog.debug(StatePool.this.context.getName()+": Voting on particle "+pendingState.getHash());

							try
							{
								Atom atom = StatePool.this.context.getLedger().getLedgerStore().get(pendingState.getAtom(), Atom.class);
								if (atom == null)
								{
									cerbyLog.warn(StatePool.this.context.getName()+": Atom "+pendingState.getAtom()+" required to vote on particle "+pendingState.getHash()+" not found");
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
									
									ParticleVote particleVote = new ParticleVote(pendingState.getHash(), pendingState.getAtom(), pendingState.getBlock(), decision, StatePool.this.context.getNode().getIdentity());
									particleVote.sign(StatePool.this.context.getNode().getKey());
									StatePool.this.context.getLedger().getLedgerStore().store(particleVote);

									pendingState.vote(particleVote, localVotePower);
									
									ParticleVoteMessage particleVoteMessage = new ParticleVoteMessage(particleVote);
									// TODO only broadcast this to validators in same shard!
									for (ConnectedPeer connectedPeer : StatePool.this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED))
									{
										if (StatePool.this.context.getNode().isInSyncWith(connectedPeer.getNode(), Node.OOS_TRIGGER_LIMIT) == false)
											continue;
										
										try
										{
											StatePool.this.context.getNetwork().getMessaging().send(particleVoteMessage, connectedPeer);
										}
										catch (IOException ex)
										{
											cerbyLog.error(StatePool.this.context.getName()+": Unable to send ParticleVoteMessage for "+pendingState.getHash()+" to "+connectedPeer, ex);
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
						if (pendingState != null && pendingState.getParticle() != null && pendingState.getCertificate() != null)
						{
							if (cerbyLog.hasLevel(Logging.DEBUG))
								cerbyLog.debug(StatePool.this.context.getName()+": Broadcasting particle certificate "+pendingState.getHash());
							
							try
							{
								Atom atom = StatePool.this.context.getLedger().getLedgerStore().get(pendingState.getAtom(), Atom.class);
								if (atom == null)
								{
									cerbyLog.warn(StatePool.this.context.getName()+": Atom "+pendingState.getAtom()+" required to broadcast particle certificate "+pendingState.getHash()+" not found");
									continue;
								}
	
								// TODO inventorize this
								for (UInt256 shard : atom.getShards())
			                	{
			                		UInt128 shardGroup = StatePool.this.context.getLedger().getShardGroup(shard);
			                		if (shardGroup.compareTo(StatePool.this.context.getLedger().getShardGroup(StatePool.this.context.getNode().getIdentity())) == 0)
			                			continue;
			                		
			                		ParticleCertificateMessage particleCertificateMessage = new ParticleCertificateMessage(pendingState.getCertificate());
			        				OutboundUDPPeerFilter outboundUDPPeerFilter = new OutboundUDPPeerFilter(StatePool.this.context, Collections.singleton(shardGroup));
		        					Collection<Peer> preferred = new RemoteShardDiscovery(StatePool.this.context).discover(outboundUDPPeerFilter);
		        					for (Peer preferredPeer : preferred)
		        					{
		        						try
		        						{
		        							ConnectedPeer connectedPeer = StatePool.this.context.getNetwork().connect(preferredPeer.getURI(), Direction.OUTBOUND, Protocol.UDP);
		        							StatePool.this.context.getNetwork().getMessaging().send(particleCertificateMessage, connectedPeer);
		        						}
		        						catch (IOException ex)
		        						{
		        							cerbyLog.error(StatePool.this.context.getName()+": Unable to send ParticleCertificateMessage for "+pendingState.getHash()+" to " + preferredPeer, ex);
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
	private final Map<Hash, Hash> indexables = new HashMap<Hash, Hash>();
	private final BlockingQueue<PendingState> voteQueue;
	private final BlockingQueue<PendingState> certificateQueue;
	
	// Atom vote broadcast batching
	private final BlockingQueue<Hash> particleVoteInventory = new LinkedBlockingQueue<Hash>();
	private final Executable particleVoteInventoryProcessor = new Executable() 
	{
		@Override
		public void execute()
		{
			while(isTerminated() == false)
			{
				try
				{
					Hash hash = StatePool.this.particleVoteInventory.poll(1, TimeUnit.SECONDS);
					if (hash == null)
						continue;
					
					List<Hash> particleVoteInventory = new ArrayList<Hash>();
					particleVoteInventory.add(hash);
					StatePool.this.particleVoteInventory.drainTo(particleVoteInventory, InventoryMessage.MAX_INVENTORY-1);
	
					if (StatePool.this.context.getLedger().isSynced() == true)
					{
						ParticleVoteInventoryMessage particleVoteInventoryMessage = new ParticleVoteInventoryMessage(particleVoteInventory);
						for (ConnectedPeer connectedPeer : StatePool.this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED))
						{
							try
							{
								StatePool.this.context.getNetwork().getMessaging().send(particleVoteInventoryMessage, connectedPeer);
							}
							catch (IOException ex)
							{
								cerbyLog.error(StatePool.this.context.getName()+": Unable to send ParticleVoteInventoryMessage of "+particleVoteInventoryMessage.getInventory().size()+" particle votes to "+connectedPeer, ex);
							}
						}
					}
				}
				catch (Exception ex)
				{
					cerbyLog.error(StatePool.this.context.getName()+": Processing of particle vote inventory failed", ex);
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
		this.context.getNetwork().getMessaging().register(ParticleVoteInventoryMessage.class, this.getClass(), new MessageProcessor<ParticleVoteInventoryMessage>()
		{
			@Override
			public void process(final ParticleVoteInventoryMessage particleVoteInventoryMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						final List<Hash> particleVoteInventoryRequired = new ArrayList<Hash>();

						try
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StatePool.this.context.getName()+": Particle votes inventory for "+particleVoteInventoryMessage.getInventory().size()+" votes from " + peer);

							if (particleVoteInventoryMessage.getInventory().size() == 0)
							{
								cerbyLog.error(StatePool.this.context.getName()+": Received empty particle votes inventory from " + peer);
								// TODO disconnect and ban
								return;
							}

							// TODO proper request process that doesn't produce duplicate requests see AtomHandler
							for (Hash particleVoteHash : particleVoteInventoryMessage.getInventory())
							{
								if (StatePool.this.context.getLedger().getLedgerStore().has(particleVoteHash) == true)
									continue;
								
								particleVoteInventoryRequired.add(particleVoteHash);
							}
							
							if (particleVoteInventoryRequired.isEmpty() == true)
								return;
							
							StatePool.this.context.getNetwork().getMessaging().send(new GetParticleVoteMessage(particleVoteInventoryRequired), peer);
						}
						catch (Exception ex)
						{
							cerbyLog.error(StatePool.this.context.getName()+": Unable to send ledger.messages.particle.vote.get for "+particleVoteInventoryRequired.size()+" particle votes to "+peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(GetParticleVoteMessage.class, this.getClass(), new MessageProcessor<GetParticleVoteMessage>()
		{
			@Override
			public void process(final GetParticleVoteMessage getParticleVoteMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StatePool.this.context.getName()+": Particle votes request for "+getParticleVoteMessage.getInventory().size()+" votes from " + peer);
							
							if (getParticleVoteMessage.getInventory().size() == 0)
							{
								cerbyLog.error(StatePool.this.context.getName()+": Received empty particle votes request from " + peer);
								// TODO disconnect and ban
								return;
							}
							
							for (Hash particleVoteHash : getParticleVoteMessage.getInventory())
							{
								ParticleVote particleVote = StatePool.this.context.getLedger().getLedgerStore().get(particleVoteHash, ParticleVote.class);
								if (particleVote == null)
								{
									if (cerbyLog.hasLevel(Logging.DEBUG) == true)
										cerbyLog.debug(StatePool.this.context.getName()+": Requested particle vote not found "+particleVoteHash+" for " + peer);
									
									continue;
								}

								try
								{
									StatePool.this.context.getNetwork().getMessaging().send(new ParticleVoteMessage(particleVote), peer);
								}
								catch (IOException ex)
								{
									cerbyLog.error(StatePool.this.context.getName()+": Unable to send ParticleVoteMessage for "+particleVote+" to "+peer, ex);
								}
							}
						}
						catch (Exception ex)
						{
							cerbyLog.error(StatePool.this.context.getName()+": ledger.messages.particle.vote.get " + peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(ParticleVoteMessage.class, this.getClass(), new MessageProcessor<ParticleVoteMessage>()
		{
			@Override
			public void process(final ParticleVoteMessage particleVoteMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StatePool.this.context.getName()+": Particle vote "+particleVoteMessage.getVote().getObject()+" for "+particleVoteMessage.getVote().getOwner()+" from " + peer);
							
							if (particleVoteMessage.getVote().verify(particleVoteMessage.getVote().getOwner()) == false)
							{
								cerbyLog.error(StatePool.this.context.getName()+": Particle vote failed verification for "+particleVoteMessage.getVote().getOwner()+" from " + peer);
								return;
							}
							
							if (OperationStatus.KEYEXIST.equals(StatePool.this.context.getLedger().getLedgerStore().store(particleVoteMessage.getVote())) == false)
							{
								// Creating pending state objects from vote if particle not seen
								StatePool.this.lock.writeLock().lock();
								try
								{
									PendingState pendingState = StatePool.this.pending.get(particleVoteMessage.getVote().getParticle());
									CommitState commitState = StatePool.this.context.getLedger().getStateAccumulator().state(Indexable.from(particleVoteMessage.getVote().getParticle(), Particle.class));
									if (pendingState == null && commitState.index() < CommitState.COMMITTED.index())
									{
										pendingState = new PendingState(particleVoteMessage.getVote().getParticle(), particleVoteMessage.getVote().getAtom(), particleVoteMessage.getVote().getBlock());
										add(pendingState);
									}

									if (pendingState == null)
										return;
									
									if (commitState.index() == CommitState.COMMITTED.index())
									{
										cerbyLog.warn(StatePool.this.context.getName()+": Particle "+particleVoteMessage.getVote().getParticle()+" is already committed");
										remove(StatePool.this.context.getLedger().getLedgerStore().get(pendingState.getAtom(), Atom.class));
										return;
									}
									
									if (pendingState.getBlock().equals(particleVoteMessage.getVote().getBlock()) == false || 
										pendingState.getAtom().equals(particleVoteMessage.getVote().getAtom()) == false)
									{
										cerbyLog.error(StatePool.this.context.getName()+": Particle vote "+particleVoteMessage.getVote().getParticle()+" block or atom dependencies not as expected for "+particleVoteMessage.getVote().getOwner()+" from " + peer);
										return;
									}
											
									if (pendingState.voted(particleVoteMessage.getVote().getOwner()) == true)
										return;
										
									pendingState.vote(particleVoteMessage.getVote(), StatePool.this.context.getLedger().getVoteRegulator().getVotePower(particleVoteMessage.getVote().getOwner()));
									if (pendingState.getCertificate() != null)
										return;
									
									buildCertificate(pendingState);
								}
								finally
								{
									StatePool.this.lock.writeLock().unlock();
								}
								
								// Independent so doesn't need to sit in a lock
								StatePool.this.particleVoteInventory.add(particleVoteMessage.getVote().getHash());
							}
							else
								cerbyLog.warn(StatePool.this.context.getName()+": Received already seen particle vote of "+particleVoteMessage.getVote().getObject()+" for "+particleVoteMessage.getVote().getOwner()+" from " + peer);
						}
						catch (Exception ex)
						{
							cerbyLog.error(StatePool.this.context.getName()+": ledger.messages.particle.vote " + peer, ex);
						}
					}
				});
			}
		});
		

		this.context.getEvents().register(this.syncBlockListener);
		this.context.getEvents().register(this.syncAtomListener);
		
		Thread particleVoteInventoryProcessorThread = new Thread(this.particleVoteInventoryProcessor);
		particleVoteInventoryProcessorThread.setDaemon(true);
		particleVoteInventoryProcessorThread.setName(this.context.getName()+" Particle Vote Inventory Processor");
		particleVoteInventoryProcessorThread.start();
		
		Thread voteProcessorThread = new Thread(this.voteProcessor);
		voteProcessorThread.setDaemon(true);
		voteProcessorThread.setName(this.context.getName()+" Particle Vote Processor");
		voteProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException
	{
		this.particleVoteInventoryProcessor.terminate(true);
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
			for (Particle particle : atom.getParticles())
			{
				PendingState pendingState = this.pending.get(particle.getHash());
				if (pendingState == null || pendingState.getParticle() == null)
				{
					if (pendingState == null)
					{
						pendingState = new PendingState(particle.getHash(), atom.getHash(), block);
						add(pendingState);
					}
					
					if (pendingState.getParticle() == null)
					{
						pendingState.setParticle(particle);
	
						Set<Indexable> indexables = new HashSet<Indexable>(particle.getIndexables());
						indexables.add(Indexable.from(particle.getHash(), Particle.class));
						indexables.add(Indexable.from(particle.getHash(), particle.getClass()));
						
						// TODO want to allow multiple indexable definitions in pool?
						// TODO indexable management here is disabled
						for (Indexable indexable : indexables)
						{
							if (this.indexables.containsKey(indexable.getHash()) == true)
								cerbyLog.debug("Indexable "+indexable+" defined by "+particle.getHash()+" already defined in pending state");
							else
								this.indexables.put(indexable.getHash(), particle.getHash());
						}
	
						if (StatePool.this.context.getLedger().getShardGroups(particle.getShards()).contains(StatePool.this.context.getLedger().getShardGroup(StatePool.this.context.getNode().getIdentity())) == true)
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
			for (Particle particle : atom.getParticles())
			{
				this.pending.remove(particle.getHash());
				Set<Indexable> indexables = new HashSet<Indexable>(particle.getIndexables());
				indexables.add(Indexable.from(particle.getHash(), Particle.class));
				indexables.add(Indexable.from(particle.getHash(), particle.getClass()));
						
				for (Indexable indexable : indexables)
					this.indexables.remove(indexable.getHash());
			}			
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
			ParticleCertificate certificate = null;
			UInt128 shardGroup = StatePool.this.context.getLedger().getShardGroup(StatePool.this.context.getNode().getIdentity());
			long voteThresold = StatePool.this.context.getLedger().getVoteRegulator().getVotePowerThreshold(Collections.singleton(shardGroup));
			if (pendingState.power(true) >= voteThresold)
			{
				cerbyLog.info(StatePool.this.context.getName()+": Particle "+pendingState.getHash()+" has positive agreement with "+pendingState.power(true)+"/"+StatePool.this.context.getLedger().getVoteRegulator().getTotalVotePower(Collections.singleton(shardGroup)));
				certificate = new ParticleCertificate(pendingState.getHash(), pendingState.getAtom(), pendingState.getBlock(), true, 
													  StatePool.this.context.getLedger().getVoteRegulator().getVotePowerBloom(shardGroup),
													  pendingState.votes(true));
			}
			else if (pendingState.power(false) >= voteThresold)
			{
				cerbyLog.info(StatePool.this.context.getName()+": Particle "+pendingState.getHash()+" has negative agreement with "+pendingState.power(false)+"/"+StatePool.this.context.getLedger().getVoteRegulator().getTotalVotePower(Collections.singleton(shardGroup)));
				certificate = new ParticleCertificate(pendingState.getHash(), pendingState.getAtom(), pendingState.getBlock(), false, 
													  StatePool.this.context.getLedger().getVoteRegulator().getVotePowerBloom(shardGroup),
													  pendingState.votes(false));
			}
			
			if (certificate != null)
			{
				pendingState.setCertificate(certificate);
				this.certificateQueue.add(pendingState);
				StatePool.this.context.getEvents().post(new ParticleCertificateEvent(certificate));
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
