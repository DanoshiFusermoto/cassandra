package org.fuserleer.ledger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.collections.MappedBlockingQueue;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Certificate;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.EventListener;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.AtomAcceptedEvent;
import org.fuserleer.ledger.events.AtomCertificateEvent;
import org.fuserleer.ledger.events.AtomCommitEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomExecutedEvent;
import org.fuserleer.ledger.events.AtomPersistedEvent;
import org.fuserleer.ledger.events.AtomRejectedEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.events.StateCertificateEvent;
import org.fuserleer.ledger.events.SyncStatusChangeEvent;
import org.fuserleer.ledger.messages.GetStateMessage;
import org.fuserleer.ledger.messages.StateMessage;
import org.fuserleer.ledger.messages.SyncAcquiredMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.GossipFetcher;
import org.fuserleer.network.GossipFilter;
import org.fuserleer.network.GossipInventory;
import org.fuserleer.network.GossipReceiver;
import org.fuserleer.network.SyncInventory;
import org.fuserleer.network.messages.BroadcastInventoryMessage;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.PeerTask;
import org.fuserleer.network.peers.filters.StandardPeerFilter;
import org.fuserleer.node.Node;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.UInt256;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public final class StateHandler implements Service
{
	private static final Logger stateLog = Logging.getLogger("state");
	private static final Logger cerbyLog = Logging.getLogger("cerby");
	
	private final Context context;
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	private final Map<Hash, PendingAtom> atoms = new HashMap<Hash, PendingAtom>();
	private final Map<StateKey<?, ?>, PendingAtom> states = new HashMap<StateKey<?, ?>, PendingAtom>();

	private final BlockingQueue<PendingAtom> executionQueue;
	private final BlockingQueue<StateMessage> provisionResponses;
	private final MappedBlockingQueue<StateKey<?, ?>, PendingAtom> provisionQueue;
	private final Multimap<StateKey<?, ?>, Entry<Hash, ConnectedPeer>> inboundProvisionRequests = Multimaps.synchronizedMultimap(HashMultimap.create());

	private final MappedBlockingQueue<Hash, StateCertificate> certificatesToSyncQueue;
	private final MappedBlockingQueue<Hash, StateCertificate> certificatesToProcessQueue;

	// FIXME Need to remember the local state inputs for an atom/statekey pair to serve state requests to remote validators.
	//       ideally would want to embed the state inputs into the atom / state certificates for future reference and
	//		 to read any latent state requests from them.
	//		 A compromise solution is to persist them for a period of time as with the atom and state votes and prune 
	//		 periodically, as the state input information is generally transient.
	//		 Complexity required to do either is beyond immediate Cassandra scope, so for now just cheat and keep a largish cache.
	private final Cache<Hash, Optional<UInt256>> stateInputs = CacheBuilder.newBuilder().maximumSize(1<<18).build();
	
	// Sync cache
	private final Multimap<Long, Hash> stateCertificateSyncCache = Multimaps.synchronizedMultimap(HashMultimap.create());
	
	// TODO clean this up, used to track responses (or lack of) for state provisioning requests to remote validators and trigger peer tasks
	private final Set<Hash> outboundProvisionRequests = Collections.synchronizedSet(new HashSet<Hash>());
	
	private Executable provisioningProcessor = new Executable()
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
						List<StateCertificate> certificatesToSync = new ArrayList<StateCertificate>();
						StateHandler.this.certificatesToSyncQueue.drainTo(certificatesToSync, 64);
						if (certificatesToSync.isEmpty() == false)
						{
							for (StateCertificate stateCertificate : certificatesToSync)
							{
								try
								{
									process(stateCertificate);
								}
								catch (Exception ex)
								{
									stateLog.error(StateHandler.this.context.getName()+": Error syncing certificate for " + stateCertificate, ex);
								}
							}
						}

						List<StateCertificate> certificatesToProcess = new ArrayList<StateCertificate>();
						StateHandler.this.certificatesToProcessQueue.drainTo(certificatesToProcess, 64);
						if (certificatesToProcess.isEmpty() == false)
						{
							for (StateCertificate stateCertificate : certificatesToProcess)
							{
								try
								{
									if (OperationStatus.KEYEXIST.equals(StateHandler.this.context.getLedger().getLedgerStore().store(stateCertificate)) == false)
										StateHandler.this.process(stateCertificate);
									else
										cerbyLog.warn(StateHandler.this.context.getName()+": State certificate "+stateCertificate.getHash()+" of "+stateCertificate.getState()+" already seen for "+stateCertificate.getAtom());
								}
								catch (Exception ex)
								{
									stateLog.error(StateHandler.this.context.getName()+": Error processing certificate for " + stateCertificate, ex);
								}
							}
						}

						Entry<StateKey<?, ?>, PendingAtom> stateToProvision = StateHandler.this.provisionQueue.poll(1, TimeUnit.SECONDS);
						if (stateToProvision != null)
						{
							StateKey<?, ?> stateKey = stateToProvision.getKey();
							PendingAtom pendingAtom = stateToProvision.getValue();
							try
							{
								if (cerbyLog.hasLevel(Logging.DEBUG))
									cerbyLog.debug(StateHandler.this.context.getName()+": Provisioning state "+stateKey+" for atom "+pendingAtom.getHash());

								long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()));
								long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
								long provisionShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
			                	if (provisionShardGroup == localShardGroup)
			                	{
			            			UInt256 value = StateHandler.this.context.getLedger().getLedgerStore().get(stateKey);
			                		if (cerbyLog.hasLevel(Logging.DEBUG))
										cerbyLog.debug(StateHandler.this.context.getName()+": State "+stateKey+" in atom "+pendingAtom.getHash()+" was provisioned locally");
			                		
			                		processRequests(pendingAtom.getHash(), stateKey, value);

			                		// Process any state certificates received early in case of early out possibility 
			            			if (pendingAtom.getCertificate() == null && pendingAtom.buildCertificate() == null)
			            			{
				                		// No certificate possible with early received state certificates so provision local state
			            				if (pendingAtom.thrown() != null)
										{
											if (cerbyLog.hasLevel(Logging.DEBUG))
												cerbyLog.debug(StateHandler.this.context.getName()+": Aborting state provisioning "+stateKey+" for atom "+pendingAtom.getHash()+" as execution exception thrown");
											
											continue;
										}

										provision(pendingAtom, stateKey, value);
			            			}
			                	}
			                	else if (pendingAtom.getCertificate() == null)
			                	{
									if (pendingAtom.thrown() != null)
									{
										if (cerbyLog.hasLevel(Logging.DEBUG))
											cerbyLog.debug(StateHandler.this.context.getName()+": Aborting state provisioning "+stateKey+" for atom "+pendingAtom.getHash()+" as execution exception thrown");
										
										continue;
									}

									final GetStateMessage getStateMessage = new GetStateMessage(pendingAtom.getHash(), stateKey);
									final List<ConnectedPeer> provisionPeers = StateHandler.this.context.getNetwork().get(StandardPeerFilter.build(StateHandler.this.context).setStates(PeerState.CONNECTED).setShardGroup(provisionShardGroup));
									if (provisionPeers.isEmpty() == true)
									{
										cerbyLog.error(StateHandler.this.context.getName()+": No provisioning peers available to satisfy request for "+stateKey+" in atom "+pendingAtom.getHash());
										continue;
									}
										
			        				for (ConnectedPeer connectedPeer : provisionPeers)
			        				{
			        					Hash requestHash = Hash.from(getStateMessage.getKey().get(), getStateMessage.getAtom());
			        					try
			        					{
			        						StateHandler.this.outboundProvisionRequests.add(requestHash);
		        							StateHandler.this.context.getNetwork().getMessaging().send(getStateMessage, connectedPeer);
											Executor.getInstance().schedule(new PeerTask(connectedPeer, 120, TimeUnit.SECONDS) 
											{
												final GetStateMessage request = getStateMessage;
												
												@Override
												public void execute()
												{
													if (StateHandler.this.outboundProvisionRequests.remove(Hash.from(this.request.getKey().get(), this.request.getAtom())) == true)
													{
														if (getPeer().getState().equals(PeerState.CONNECTED) || getPeer().getState().equals(PeerState.CONNECTING))
															cerbyLog.error(StateHandler.this.context.getName()+": "+getPeer()+" did not respond to request for state "+request.getKey()+" in atom "+request.getAtom());
														
														// TODO retrys
													}
												}
											});
		        							break; // Only want to ask a single remote node
		        						}
		        						catch (IOException ex)
		        						{
			        						StateHandler.this.outboundProvisionRequests.remove(requestHash);
		        							cerbyLog.error(StateHandler.this.context.getName()+": Unable to send GetStateMessage of "+stateKey+" in atom "+pendingAtom.getHash()+" to "+connectedPeer, ex);
		        						}
			        				}
		                		}
							}
							catch (Exception ex)
							{
								cerbyLog.error(StateHandler.this.context.getName()+": Error processing provisioning for "+stateKey+" in atom "+pendingAtom.getHash(), ex);
								StateHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
							}
						}
						
						// Deal with responses
						List<StateMessage> provisionResponses = new ArrayList<StateMessage>();
						StateHandler.this.provisionResponses.drainTo(provisionResponses);
						for (StateMessage provisionResponse : provisionResponses)
						{
							StateKey<?, ?> stateKey = provisionResponse.getKey();
							PendingAtom pendingAtom = StateHandler.this.atoms.get(provisionResponse.getAtom());
							UInt256 value = provisionResponse.getValue();
							
							if (pendingAtom == null)
							{
								cerbyLog.warn(StateHandler.this.context.getName()+": Pending atom "+provisionResponse.getHash()+" for state provision response "+stateKey+" not found");
								continue;
							}
							
							try
							{
								if (pendingAtom.thrown() != null)
								{
									if (cerbyLog.hasLevel(Logging.DEBUG))
										cerbyLog.debug(StateHandler.this.context.getName()+": Aborting state provisioning "+stateKey+" for atom "+pendingAtom.getHash()+" as execution exception thrown");
									
									continue;
								}

								if (cerbyLog.hasLevel(Logging.DEBUG))
									cerbyLog.debug(StateHandler.this.context.getName()+": Provisioning state "+stateKey+" for atom "+pendingAtom.getHash());

		                		provision(pendingAtom, stateKey, value);
							}
							catch (Exception ex)
							{
								cerbyLog.error(StateHandler.this.context.getName()+": Error processing provisioning for "+stateKey+" in atom "+pendingAtom.getHash(), ex);
								StateHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
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
				cerbyLog.fatal(StateHandler.this.context.getName()+": Error processing provisioning queue", throwable);
			}
		}
	};
	
	private Executable executionProcessor = new Executable()
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
						PendingAtom pendingAtom = StateHandler.this.executionQueue.poll(1, TimeUnit.SECONDS);
						if (pendingAtom != null)
						{
							try
							{
								if (cerbyLog.hasLevel(Logging.DEBUG))
									cerbyLog.debug(StateHandler.this.context.getName()+": Executing atom "+pendingAtom.getHash());
								
								pendingAtom.execute();
								StateHandler.this.context.getEvents().post(new AtomExecutedEvent(pendingAtom));
							}
							catch (ValidationException vex)
							{
								// Let validation exceptions make it into the state pool as they represent a "no" vote for commit
								cerbyLog.error(StateHandler.this.context.getName()+": State machine throw validation exception on "+pendingAtom.getHash(), vex);
								StateHandler.this.context.getEvents().post(new AtomExecutedEvent(pendingAtom));
							}
							catch (Exception ex)
							{
								cerbyLog.error(StateHandler.this.context.getName()+": Error executing "+pendingAtom.getHash(), ex);
								StateHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
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
				cerbyLog.fatal(StateHandler.this.context.getName()+": Error processing execution queue", throwable);
			}
		}
	};

	StateHandler(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.provisionQueue = new MappedBlockingQueue<StateKey<?, ?>, PendingAtom>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.provisionResponses = new LinkedBlockingQueue<StateMessage>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.executionQueue = new LinkedBlockingQueue<PendingAtom>(this.context.getConfiguration().get("ledger.atom.queue", 1<<16));
		this.certificatesToSyncQueue = new MappedBlockingQueue<Hash, StateCertificate>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.certificatesToProcessQueue = new MappedBlockingQueue<Hash, StateCertificate>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));

//		cerbyLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
//		stateLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
		cerbyLog.setLevels(Logging.ERROR | Logging.FATAL);
		stateLog.setLevels(Logging.ERROR | Logging.FATAL);
	}
	
	private void provision(final PendingAtom pendingAtom, final StateKey<?, ?> stateKey, final UInt256 value) throws InterruptedException
	{
		try
		{
			if (pendingAtom.provision(stateKey, value) == true)
			{
	    		if (cerbyLog.hasLevel(Logging.DEBUG))
					cerbyLog.debug(StateHandler.this.context.getName()+": Queuing pending atom "+pendingAtom.getHash()+" for execution");
				
	    		StateHandler.this.executionQueue.put(pendingAtom);
			}
		}
		catch (ValidationException vex)
		{
			// Let provisioning validation exceptions make it into the state pool as they represent a "no" vote for commit
			cerbyLog.error(StateHandler.this.context.getName()+": State machine throw validation exception processing provisioning for "+stateKey+" in atom "+pendingAtom.getHash(), vex);
			StateHandler.this.context.getEvents().post(new AtomExecutedEvent(pendingAtom, vex));
		}
	}

	@Override
	public void start() throws StartupException
	{
		// STATE CERTIFICATE GOSSIP //
		this.context.getNetwork().getGossipHandler().register(StateCertificate.class, new GossipFilter(this.context) 
		{
			@Override
			public Set<Long> filter(Primitive stateCertificate) throws IOException
			{
				// TODO this may be redundant as presenting the broadcast with the shard groups included.
				//		reason being that if the broadcast is latent, the pending atom is gone from all sources due to a commit accept/reject
				StateHandler.this.lock.readLock().lock();
				try
				{
					PendingAtom pendingAtom = StateHandler.this.atoms.get(((StateCertificate)stateCertificate).getAtom());
					if (pendingAtom == null)
					{
						cerbyLog.error(StateHandler.this.context.getName()+": Pending atom "+((StateCertificate)stateCertificate).getAtom()+" not found for inventory filter");
						return Collections.emptySet();
					}
					
					long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(((StateCertificate)stateCertificate).getHeight());
					Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
					shardGroups.remove(ShardMapper.toShardGroup(((StateCertificate)stateCertificate).getState().get(), numShardGroups));
					if (shardGroups.isEmpty() == true)
						return Collections.emptySet();
				
					return shardGroups;
				}
				finally
				{
					StateHandler.this.lock.readLock().unlock();
				}
			}
		});

		this.context.getNetwork().getGossipHandler().register(StateCertificate.class, new GossipInventory() 
		{
			@Override
			public int requestLimit()
			{
				return 8;
			}

			@Override
			public Collection<Hash> required(Class<? extends Primitive> type, Collection<Hash> items) throws IOException
			{
				if (type.equals(StateCertificate.class) == false)
				{
					cerbyLog.error(StateHandler.this.context.getName()+": State certificate type expected but got "+type);
					return Collections.emptyList();
				}
					
				StateHandler.this.lock.readLock().lock();
				try
				{
					Set<Hash> required = new HashSet<Hash>();
					for (Hash item : items)
					{
						if (StateHandler.this.certificatesToSyncQueue.contains(item) == true || 
							StateHandler.this.certificatesToProcessQueue.contains(item) == true || 
							StateHandler.this.context.getLedger().getLedgerStore().has(item) == true)
							continue;
					
						if (cerbyLog.hasLevel(Logging.DEBUG) == true)
							cerbyLog.debug(StateHandler.this.context.getName()+": Added request for state certificate "+item);
						
						required.add(item);
					}
					return required;
				}
				finally
				{
					StateHandler.this.lock.readLock().unlock();
				}
			}
		});

		this.context.getNetwork().getGossipHandler().register(StateCertificate.class, new GossipReceiver() 
		{
			@Override
			public void receive(Primitive object) throws IOException, ValidationException, CryptoException
			{
				StateCertificate stateCertificate = (StateCertificate)object;
				long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(stateCertificate.getHeight());
				long stateShardGroup = ShardMapper.toShardGroup(stateCertificate.getState().get(), numShardGroups);
				long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
				if (stateShardGroup == localShardGroup)
				{
					cerbyLog.error(StateHandler.this.context.getName()+": Received state certificate "+stateCertificate.getState()+" for local shard");
					// 	Disconnected and ban
					return;
				}

				StateHandler.this.certificatesToProcessQueue.put(stateCertificate.getHash(), stateCertificate);
			}
		});
					
		this.context.getNetwork().getGossipHandler().register(StateCertificate.class, new GossipFetcher() 
		{
			@Override
			public Collection<StateCertificate> fetch(Collection<Hash> items) throws IOException
			{
				StateHandler.this.lock.readLock().lock();
				try
				{
					Set<StateCertificate> fetched = new HashSet<StateCertificate>();
					for (Hash item : items)
					{
						StateCertificate stateCertificate = getCertificate(item, StateCertificate.class);
						if (stateCertificate == null)
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StateHandler.this.context.getName()+": Requested state certificate "+item+" not found");
							continue;
						}
						
						fetched.add(stateCertificate);
					}
					return fetched;
				}
				finally
				{
					StateHandler.this.lock.readLock().unlock();
				}
			}
		});

		this.context.getNetwork().getMessaging().register(GetStateMessage.class, this.getClass(), new MessageProcessor<GetStateMessage>()
		{
			@Override
			public void process(final GetStateMessage getStateMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StateHandler.this.context.getName()+": State request "+getStateMessage.getKey()+" for atom "+getStateMessage.getAtom()+" from " + peer);
							
							PendingAtom pendingAtom;
							Optional<UInt256> value = null;
							CommitStatus status = CommitStatus.NONE;
							StateHandler.this.lock.readLock().lock();
							try
							{
								// Look for provisioned state in pending corresponding to the atom in question
								pendingAtom = StateHandler.this.states.get(getStateMessage.getKey());
								if (pendingAtom != null && pendingAtom.getStatus().greaterThan(CommitStatus.LOCKED) == true && 
									pendingAtom.getHash().equals(getStateMessage.getAtom()) == true)
								{
									value = pendingAtom.getInput(getStateMessage.getKey());
									if (value != null)
									{
										status = pendingAtom.getStatus();
	
										if (cerbyLog.hasLevel(Logging.DEBUG) == true)
											cerbyLog.debug(StateHandler.this.context.getName()+": State request "+getStateMessage.getKey()+" in atom "+getStateMessage.getAtom()+" served from provisioned atom inputs for " + peer);
										
										StateHandler.this.context.getNetwork().getMessaging().send(new StateMessage(getStateMessage.getAtom(), getStateMessage.getKey(), value.orElse(null), status), peer);
										return;
									}
								}

								// Look for state certificate in a pending atom
								pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(getStateMessage.getAtom());
								if (pendingAtom != null && pendingAtom.getStatus().greaterThan(CommitStatus.LOCKED) == true)
								{
									value = pendingAtom.getInput(getStateMessage.getKey());
									if (value == null)
									{
										StateCertificate certificate = pendingAtom.getCertificate(getStateMessage.getKey());
										if (certificate != null)
											value = Optional.ofNullable(certificate.getInput());
									}
									
									if (value != null)
									{
										status = pendingAtom.getStatus();
	
										if (cerbyLog.hasLevel(Logging.DEBUG) == true)
											cerbyLog.debug(StateHandler.this.context.getName()+": State request "+getStateMessage.getKey()+" in atom "+getStateMessage.getAtom()+" served from state certificate for " + peer);
										
										StateHandler.this.context.getNetwork().getMessaging().send(new StateMessage(getStateMessage.getAtom(), getStateMessage.getKey(), value.orElse(null), status), peer);
										return;
									}
								}

								// Look for a committed atom certificate 
								Commit atomCommit = StateHandler.this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, getStateMessage.getAtom()));
								if (atomCommit != null)
								{
									if (atomCommit.getPath().get(Elements.CERTIFICATE) != null)
									{
										AtomCertificate certificate = StateHandler.this.context.getLedger().getLedgerStore().get(atomCommit.getPath().get(Elements.CERTIFICATE), AtomCertificate.class);
										StateCertificate stateCertificate  = certificate.get(getStateMessage.getKey());
										if (stateCertificate != null)
										{
											value = Optional.ofNullable(stateCertificate.getInput());
											status = CommitStatus.COMMITTED;
	
											if (cerbyLog.hasLevel(Logging.DEBUG) == true)
												cerbyLog.debug(StateHandler.this.context.getName()+": State request "+getStateMessage.getKey()+" in atom "+getStateMessage.getAtom()+" served from committed atom certificate for " + peer);

											StateHandler.this.context.getNetwork().getMessaging().send(new StateMessage(getStateMessage.getAtom(), getStateMessage.getKey(), value.orElse(null), status), peer);
											return;
										}
									}
								}
								
								value = StateHandler.this.stateInputs.getIfPresent(Hash.from(getStateMessage.getKey().get(), getStateMessage.getAtom()));
								if (value != null)
								{
									status = CommitStatus.PROVISIONED;
									
									if (cerbyLog.hasLevel(Logging.DEBUG) == true)
										cerbyLog.debug(StateHandler.this.context.getName()+": State request "+getStateMessage.getKey()+" in atom "+getStateMessage.getAtom()+" served from state inputs cache for " + peer);

									StateHandler.this.context.getNetwork().getMessaging().send(new StateMessage(getStateMessage.getAtom(), getStateMessage.getKey(), value.orElse(null), status), peer);
									return;
								}
								
								// Pending atom is not found in any pool or commit. Need to set a future response
								if (pendingAtom == null)
									cerbyLog.warn(StateHandler.this.context.getName()+": Pending atom "+getStateMessage.getAtom()+" was null for state request "+getStateMessage.getKey()+" for " + peer);

								// Pending atom was found but is not in a suitable state to serve the request. Need to set a future response
								if (pendingAtom != null && pendingAtom.getStatus().greaterThan(CommitStatus.LOCKED) == false)
									cerbyLog.warn(StateHandler.this.context.getName()+": Pending atom "+getStateMessage.getAtom()+" status is "+pendingAtom.getStatus()+" for state request "+getStateMessage.getKey()+" for " + peer);
									
								StateHandler.this.inboundProvisionRequests.put(getStateMessage.getKey(), new AbstractMap.SimpleEntry<>(getStateMessage.getAtom(), peer));
							}
							finally
							{
								StateHandler.this.lock.readLock().unlock();
							}
						}
						catch (Exception ex)
						{
							cerbyLog.error(StateHandler.this.context.getName()+": ledger.messages.state.get" + peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(StateMessage.class, this.getClass(), new MessageProcessor<StateMessage>()
		{
			@Override
			public void process(final StateMessage stateMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
    						if (StateHandler.this.outboundProvisionRequests.remove(Hash.from(stateMessage.getKey().get(), stateMessage.getAtom())) == false)
								cerbyLog.warn(StateHandler.this.context.getName()+": Received unexpected state response "+stateMessage.getKey()+":"+stateMessage.getValue()+" with status "+stateMessage.getStatus()+" for atom "+stateMessage.getAtom()+" from " + peer);
    						else if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StateHandler.this.context.getName()+": State response "+stateMessage.getKey()+":"+stateMessage.getValue()+" with status "+stateMessage.getStatus()+" for atom "+stateMessage.getAtom()+" from " + peer);

    						StateHandler.this.provisionResponses.add(stateMessage);
						}
						catch (Exception ex)
						{
							cerbyLog.error(StateHandler.this.context.getName()+": ledger.messages.state" + peer, ex);
						}
					}
				});
			}
		});
		
		// SYNC //
		this.context.getNetwork().getGossipHandler().register(StateCertificate.class, new SyncInventory() 
		{
			@Override
			public Collection<Hash> process(Class<? extends Primitive> type, Collection<Hash> items) throws IOException
			{
				if (type.equals(StateCertificate.class) == false)
				{
					cerbyLog.error(StateHandler.this.context.getName()+": State certificate type expected but got "+type);
					return Collections.emptyList();
				}
				
				StateHandler.this.lock.writeLock().lock();
				try
				{
					Set<Hash> required = new HashSet<Hash>();
					for (Hash item : items)
					{
						if (StateHandler.this.certificatesToSyncQueue.contains(item) == true || 
							StateHandler.this.certificatesToProcessQueue.contains(item) == true)
							continue;
						
						final StateCertificate stateCertificate = StateHandler.this.context.getLedger().getLedgerStore().get(item, StateCertificate.class);
						if (stateCertificate != null)
							StateHandler.this.certificatesToSyncQueue.put(item, stateCertificate);
						else
							required.add(item);
					}
					return required;
				}
				finally
				{
					StateHandler.this.lock.writeLock().unlock();
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
						StateHandler.this.lock.readLock().lock();
						try
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StateHandler.this.context.getName()+": State pool (certificates) inventory request from "+peer);
							
							final List<PendingAtom> pendingAtoms = new ArrayList<PendingAtom>(StateHandler.this.atoms.values());
							final List<Hash> stateCertificateInventory = new ArrayList<Hash>();
							for (PendingAtom pendingAtom : pendingAtoms)
							{
								for (StateCertificate stateCertificate : pendingAtom.getCertificates())
								{
									long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(stateCertificate.getHeight());
									long stateShardGroup = ShardMapper.toShardGroup(stateCertificate.getState().get(), numShardGroups);
									long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
									
									if (localShardGroup == stateShardGroup)
										// Handled by sending state votes from state pool 
										continue;
									
									stateCertificateInventory.add(stateCertificate.getHash());
								}
							}
							
							long height = StateHandler.this.context.getLedger().getHead().getHeight();
							while (height >= syncAcquiredMessage.getHead().getHeight())
							{
								stateCertificateInventory.addAll(StateHandler.this.stateCertificateSyncCache.get(height));
								height--;
							}
							
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StateHandler.this.context.getName()+": Broadcasting about "+stateCertificateInventory+" pool state certificates to "+peer);

							int offset = 0;
							while(offset < stateCertificateInventory.size())
							{
								BroadcastInventoryMessage stateCertificateInventoryMessage = new BroadcastInventoryMessage(stateCertificateInventory.subList(offset, Math.min(offset+BroadcastInventoryMessage.MAX_ITEMS, stateCertificateInventory.size())), StateCertificate.class);
								StateHandler.this.context.getNetwork().getMessaging().send(stateCertificateInventoryMessage, peer);
								offset += BroadcastInventoryMessage.MAX_ITEMS; 
							}
						}
						catch (Exception ex)
						{
							cerbyLog.error(StateHandler.this.context.getName()+": ledger.messages.sync.acquired " + peer, ex);
						}
						finally
						{
							StateHandler.this.lock.readLock().unlock();
						}
					}
				});
			}
		});
		
		this.context.getEvents().register(this.syncChangeListener);
		this.context.getEvents().register(this.syncBlockListener);
		this.context.getEvents().register(this.asyncBlockListener);
		this.context.getEvents().register(this.syncAtomListener);
		this.context.getEvents().register(this.certificateListener);
		
		Thread provisioningProcessorThread = new Thread(this.provisioningProcessor);
		provisioningProcessorThread.setDaemon(true);
		provisioningProcessorThread.setName(this.context.getName()+" Provisioning Processor");
		provisioningProcessorThread.start();

		Thread executionProcessorThread = new Thread(this.executionProcessor);
		executionProcessorThread.setDaemon(true);
		executionProcessorThread.setName(this.context.getName()+" Execution Processor");
		executionProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException
	{
		this.executionProcessor.terminate(true);
		this.provisioningProcessor.terminate(true);
		
		this.context.getEvents().unregister(this.certificateListener);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.asyncBlockListener);
		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	<T extends Certificate> T getCertificate(Hash hash, Class<? extends Certificate> type) throws IOException
	{
		this.lock.readLock().lock();
		try
		{
			return (T) this.context.getLedger().getLedgerStore().get(hash, type);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public List<AtomCertificate> get(final int limit, final Collection<Hash> exclusions)
	{
		final List<AtomCertificate> certificates = new ArrayList<AtomCertificate>();
		final Predicate<PendingAtom> filter = new Predicate<PendingAtom>()
		{
			@Override
			public boolean test(PendingAtom pa)
			{
				if (pa.getAtom() == null)
					return false;

				if (pa.getCertificate() == null)
					return false;

				if (exclusions.contains(pa.getCertificate().getHash()) == true)
					return false;
					
				return true;
			}
		};

		this.lock.readLock().lock();
		try
		{
			for (PendingAtom pendingAtom : this.atoms.values())
			{
				if (filter.test(pendingAtom) == false)
					continue;
				
				certificates.add(pendingAtom.getCertificate());
				
				if (certificates.size() == limit)
					break;
			}
		}
		finally
		{
			this.lock.readLock().unlock();
		}
		
		return certificates;
	}
	
	private void remove(final PendingAtom pendingAtom)
	{
		StateHandler.this.lock.writeLock().lock();
		try
		{
			StateHandler.this.atoms.remove(pendingAtom.getHash());
			pendingAtom.getStateKeys().forEach(sk -> {
				long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()));
				long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
				long provisionShardGroup = ShardMapper.toShardGroup(sk.get(), numShardGroups);
	        	if (provisionShardGroup != localShardGroup)
	        		this.stateCertificateSyncCache.put(this.context.getLedger().getHead().getHeight(), pendingAtom.getCertificate(sk).getHash());

				StateHandler.this.states.remove(sk, pendingAtom);
				StateHandler.this.outboundProvisionRequests.remove(Hash.from(sk.get(), pendingAtom.getHash()));
			});
			pendingAtom.getStateKeys().forEach(sk -> StateHandler.this.provisionQueue.remove(sk, pendingAtom));
		}
		finally
		{
			StateHandler.this.lock.writeLock().unlock();
		}
	}
	
	private void processRequests(final Hash atom, final StateKey<?, ?> stateKey, final UInt256 value)
	{
		StateHandler.this.lock.writeLock().lock();
		try
		{
    		this.stateInputs.put(Hash.from(stateKey.get(), atom), Optional.ofNullable(value));
			
       		// Any pending requests for this atom / state combo
       		// TODO needs timeouts
       		Iterator<Entry<Hash, ConnectedPeer>> requests = StateHandler.this.inboundProvisionRequests.get(stateKey).iterator();
       		while(requests.hasNext())
       		{
       			Entry<Hash, ConnectedPeer> request = requests.next();
   				if (request.getValue().getState().equals(PeerState.CONNECTED) == false)
   				{
   					requests.remove();
   					continue;
   				}
            				
   				if (atom.equals(request.getKey()) == false)
   				{
   					cerbyLog.warn(StateHandler.this.context.getName()+": Requested state "+stateKey+" is locked by "+atom+" in home shard group but "+request.getKey()+" at remote");
   					continue;
   				}
            				
				cerbyLog.debug(StateHandler.this.context.getName()+": State request "+stateKey+" in atom "+atom+" processed for " + request.getValue());

				try
   				{
   					StateHandler.this.context.getNetwork().getMessaging().send(new StateMessage(request.getKey(), stateKey, value, CommitStatus.PROVISIONING), request.getValue());
   				}
   				catch (IOException ex)
   				{
   					cerbyLog.error(this.context.getName()+": Delivery of state response failed for "+stateKey+" locked by "+atom+" with value "+value+" to "+request.getValue(), ex);
   				}

   				requests.remove();
       		}
		}
		finally
		{
			StateHandler.this.lock.writeLock().unlock();
		}
	}
	
	private boolean process(final StateCertificate certificate) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(certificate, "State certificate is null");
		
		StateHandler.this.lock.writeLock().lock();
		try
		{
			CommitStatus commitStatus = this.context.getLedger().getStateAccumulator().has(new StateAddress(AtomCertificate.class, certificate.getAtom()));
			if (commitStatus.equals(CommitStatus.COMMITTED) == true)
			{
				cerbyLog.warn(this.context.getName()+": Already have a certificate for "+certificate.getAtom());
				PendingAtom pendingAtom = this.atoms.remove(certificate.getAtom());
				if (pendingAtom != null)
				{
					for (StateKey<?, ?> stateKey : pendingAtom.getStateKeys())
						this.states.remove(stateKey, pendingAtom);
				}
				return false;
			}

			PendingAtom pendingAtom = StateHandler.this.atoms.get(certificate.getAtom());
			if (pendingAtom == null)
			{
				cerbyLog.warn(this.context.getName()+": Heard about Atom "+certificate.getAtom()+" via StateCertificate before BlockCommittedEvent");
				pendingAtom = this.context.getLedger().getAtomHandler().get(certificate.getAtom());
				if (pendingAtom == null)
				{
					cerbyLog.warn(this.context.getName()+": AtomHandler return null for Atom "+certificate.getAtom()+".  Possibly committed");
					return false;
				}
				
				this.atoms.put(certificate.getAtom(), pendingAtom);
			}

			if (cerbyLog.hasLevel(Logging.DEBUG) == true)
				cerbyLog.debug(this.context.getName()+": State certificate "+certificate.getState()+" for "+certificate.getAtom());
		
			if (pendingAtom.addCertificate(certificate) == false)
				return false;
				
			long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(certificate.getHeight());
			Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
			shardGroups.remove(ShardMapper.toShardGroup(certificate.getState().get(), numShardGroups));
			if (shardGroups.isEmpty() == false)
			{
				this.context.getNetwork().getGossipHandler().broadcast(certificate, shardGroups);
				cerbyLog.info(StateHandler.this.context.getName()+": Broadcasted state certificate "+certificate);
			}
			
			pendingAtom.buildCertificate();
			return true;
		}
		finally
		{
			StateHandler.this.lock.writeLock().unlock();
		}
	}
	
	void push(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom for push is null");
		
		for (StateKey<?, ?> stateKey : pendingAtom.getStateKeys())
		{
			if (StateHandler.this.states.putIfAbsent(stateKey, pendingAtom) != null)
				cerbyLog.warn(StateHandler.this.context.getName()+": State "+stateKey+" should be absent for "+pendingAtom.getHash());
				
			if (StateHandler.this.provisionQueue.putIfAbsent(stateKey, pendingAtom) != null)
				cerbyLog.warn(StateHandler.this.context.getName()+": State "+stateKey+" should be absent for "+pendingAtom.getHash());
		}
	}

	// PARTICLE CERTIFICATE LISTENER //
	private EventListener certificateListener = new EventListener()
	{
		@Subscribe
		public void on(final StateCertificateEvent stateCertificateEvent) 
		{
			if (cerbyLog.hasLevel(Logging.DEBUG) == true)
				cerbyLog.debug(StateHandler.this.context.getName()+": State certificate "+stateCertificateEvent.getCertificate().getState()+" from local");

			try
			{
				StateHandler.this.process(stateCertificateEvent.getCertificate());
			}
			catch (Exception ex)
			{
				cerbyLog.fatal(StateHandler.class.getName()+": Failed to process state certificate for "+stateCertificateEvent.getCertificate().getState()+" when processing StateCertificateEvent", ex);
				return;
			}
		}
		
		@Subscribe
		public void on(final AtomCertificateEvent atomCertificateEvent) throws IOException 
		{
			if (cerbyLog.hasLevel(Logging.DEBUG) == true)
				cerbyLog.debug(StateHandler.this.context.getName()+": Atom certificate "+atomCertificateEvent.getCertificate().getAtom()+" from local");

			StateHandler.this.lock.writeLock().lock();
			try
			{
				if (StateHandler.this.atoms.containsKey(atomCertificateEvent.getCertificate().getAtom()) == false)
					throw new IllegalStateException(StateHandler.this.context.getName()+": Pending atom "+atomCertificateEvent.getCertificate().getAtom()+" in certificate not found");
				
				StateHandler.this.context.getLedger().getLedgerStore().store(atomCertificateEvent.getCertificate());
			}
			finally
			{
				StateHandler.this.lock.writeLock().unlock();
			}
		}
	};
		
	// SYNC ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomPersistedEvent event) 
		{
			StateHandler.this.lock.writeLock().lock();
			try
			{
				if (StateHandler.this.context.getLedger().getAtomPool().add(event.getPendingAtom()) == false)
					cerbyLog.error(StateHandler.this.context.getName()+": Atom "+event.getAtom().getHash()+" not added to atom pool");
			}
			finally
			{
				StateHandler.this.lock.writeLock().unlock();
			}
		}
		
		@Subscribe
		public void on(final AtomAcceptedEvent event) 
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomRejectedEvent event) 
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event) 
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomExceptionEvent event) 
		{
			if (event.getException() instanceof StateLockedException)
				return;
				
			remove(event.getPendingAtom());
		}
	};

	// ASYNC BLOCK LISTENER //
	private EventListener asyncBlockListener = new EventListener()
	{
		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent)
		{
			StateHandler.this.lock.writeLock().lock();
			try
			{
				long trimTo = blockCommittedEvent.getBlock().getHeader().getHeight() - Node.OOS_TRIGGER_LIMIT;
				if (trimTo > 0)
				{
					Iterator<Long> stateCertificateSyncCacheKeyIterator = StateHandler.this.stateCertificateSyncCache.keySet().iterator();
					while(stateCertificateSyncCacheKeyIterator.hasNext() == true)
					{
						if (stateCertificateSyncCacheKeyIterator.next() < trimTo)
							stateCertificateSyncCacheKeyIterator.remove();
					}
				}
			}
			finally
			{
				StateHandler.this.lock.writeLock().unlock();
			}
		}
	};

	// SYNC BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final BlockCommittedEvent event) 
		{
			// Creating pending atom from precommitted event if not seen // This is the most likely place for a pending atom object to be created
			StateHandler.this.lock.writeLock().lock();
			try
			{
				try
				{
					Set<PendingAtom> pendingAtoms = new HashSet<PendingAtom>();
					for (Atom atom : event.getBlock().getAtoms())
					{
						PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomPool().remove(atom.getHash());
						if (pendingAtom == null)
							pendingAtom = StateHandler.this.atoms.get(atom.getHash());
						
						if (pendingAtom == null)
							pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(atom.getHash());
						
						if (pendingAtom == null)
						{
							cerbyLog.warn(StateHandler.this.context.getName()+": Pending atom "+atom.getHash()+" state appears invalid.");
							continue;
						}
						
						StateHandler.this.atoms.putIfAbsent(atom.getHash(), pendingAtom);
						pendingAtoms.add(pendingAtom);
					}

					for (PendingAtom pendingAtom : pendingAtoms)
					{
						CommitStatus commitStatus = StateHandler.this.context.getLedger().getStateAccumulator().has(new StateAddress(AtomCertificate.class, pendingAtom.getHash()));
						if (commitStatus.equals(CommitStatus.COMMITTED) == true)
						{
							cerbyLog.warn(StateHandler.this.context.getName()+": Already have a certificate for "+pendingAtom.getHash());
							if (StateHandler.this.atoms.remove(pendingAtom.getHash()) != null)
								pendingAtom.getStateKeys().forEach(sk -> StateHandler.this.states.remove(sk, pendingAtom));

							continue;
						}
						
						StateHandler.this.context.getLedger().getStateAccumulator().lock(pendingAtom);
						
                		if (cerbyLog.hasLevel(Logging.DEBUG))
							cerbyLog.debug(StateHandler.this.context.getName()+": Queuing pending atom "+pendingAtom.getHash()+" for provisioning");

                		// TODO provisioning queue should only have ONE entry for a state address due to locking.  Verify!
						Set<StateKey<?, ?>> stateKeys = StateHandler.this.context.getLedger().getStateAccumulator().provision(event.getBlock().getHeader(), pendingAtom);
						push(pendingAtom);
					}
				}
				catch (Exception ex)
				{
					cerbyLog.fatal(StateHandler.class.getName()+": Failed to create PendingAtom set for "+event.getBlock().getHeader()+" when processing BlockCommittedEvent", ex);
					return;
				}
				
				// Commit atom states
				Set<PendingAtom> committed = new HashSet<PendingAtom>();
				for (AtomCertificate certificate : event.getBlock().getCertificates())
				{
					try
					{
						if (cerbyLog.hasLevel(Logging.DEBUG) == true)
							cerbyLog.debug(StateHandler.this.context.getName()+": Committing atom certificate "+certificate.getHash()+" for "+certificate.getAtom());
						
						PendingAtom pendingAtom = StateHandler.this.atoms.get(certificate.getAtom());
						if (pendingAtom == null)
							throw new IllegalStateException("Pending atom "+certificate.getAtom()+" not found");
						
						if (certificate.getHash().equals(pendingAtom.getCertificate().getHash()) == false)
							throw new ValidationException("Atom certificate mismatch for "+certificate.getAtom()+" expected "+pendingAtom.getCertificate().getHash()+" but discovered "+certificate.getHash());
						
						CommitOperation commitOperation = pendingAtom.getCommitOperation();
						if (commitOperation.getType().equals(CommitOperation.Type.ACCEPT) == true)
						{
							StateHandler.this.context.getEvents().post(new AtomCommitEvent(pendingAtom));
						}
						else if (commitOperation.getType().equals(CommitOperation.Type.REJECT) == true)
						{
							for (StateCertificate voteCertificate : certificate.getAll())
							{
								// TODO report all certificate negative decisions in the validation exception.
								if (voteCertificate.getDecision().equals(StateDecision.NEGATIVE) == true)
									cerbyLog.error("Rejection certificate for state "+voteCertificate.getState());
							}

							StateHandler.this.context.getEvents().post(new AtomCommitEvent(pendingAtom));
						}
						else
							throw new IllegalStateException("Unsupported commit operation type "+commitOperation.getType()+" found for atom "+certificate.getAtom());
						
						committed.add(pendingAtom);
					}
					catch (Exception ex)
					{
						// FIXME don't like how this is thrown in an event listener.
						// 		 should be able to send the commit without having to fetch the atom, state processors *should* have it by the time
						//		 we're sending commit certificates to them.
						cerbyLog.fatal(StateHandler.this.context.getName()+": Failed to process AtomCommittedEvent for "+certificate.getAtom(), ex);
					}
				}
				
				try
				{
					// Timeouts
					Set<PendingAtom> timedOut = new HashSet<PendingAtom>();
					for (PendingAtom pendingAtom : StateHandler.this.atoms.values())
					{
						if (pendingAtom.getStatus().greaterThan(CommitStatus.LOCKED) == true && 
							event.getBlock().getHeader().getHeight() > pendingAtom.getCommitTimeout())
							timedOut.add(pendingAtom);
					}

					// Timed out atoms may have been committed on the timeout block ... allow commit
					// TODO need to test this isn't strongly-subjective otherwise some could allow, some may have already timed out
					timedOut.removeAll(committed);
					for (PendingAtom pendingAtom : timedOut)
					{
						if (pendingAtom.getAtom() == null)
							cerbyLog.warn(StateHandler.this.context.getName()+": Atom "+pendingAtom.getHash()+" timeout but never seen at "+event.getBlock().getHeader());
						else
						{
							cerbyLog.warn(StateHandler.this.context.getName()+": Atom "+pendingAtom.getHash()+" timeout at block "+event.getBlock().getHeader());
							remove(pendingAtom);
							StateHandler.this.context.getEvents().post(new AtomCommitTimeoutEvent(pendingAtom));
						}
					}
				}
				catch (Exception ex)
				{
					cerbyLog.error(StateHandler.class.getName()+": Processing of atom timeouts failed", ex);
					return;
				}
			}
			finally
			{
				StateHandler.this.lock.writeLock().unlock();
			}
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

			StateHandler.this.lock.writeLock().lock();
			try
			{
				stateLog.info(StateHandler.this.context.getName()+": Sync status changed to "+event.isSynced()+", flushing state handler");
				StateHandler.this.atoms.clear();
				StateHandler.this.executionQueue.clear();
				StateHandler.this.provisionQueue.clear();
				StateHandler.this.provisionResponses.clear();
				StateHandler.this.stateInputs.invalidateAll();
				StateHandler.this.states.clear();
			}
			finally
			{
				StateHandler.this.lock.writeLock().unlock();
			}
		}
	};
}
