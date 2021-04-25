package org.fuserleer.ledger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
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
import java.util.stream.Collectors;

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
import org.fuserleer.ledger.events.AtomDiscardedEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomExecutedEvent;
import org.fuserleer.ledger.events.AtomRejectedEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.events.StateCertificateEvent;
import org.fuserleer.ledger.events.SyncStatusChangeEvent;
import org.fuserleer.ledger.messages.StateMessage;
import org.fuserleer.ledger.messages.SyncAcquiredMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.GossipFetcher;
import org.fuserleer.network.GossipFilter;
import org.fuserleer.network.GossipInventory;
import org.fuserleer.network.GossipReceiver;
import org.fuserleer.network.messages.BroadcastInventoryMessage;
import org.fuserleer.network.messages.SyncInventoryMessage;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.filters.StandardPeerFilter;
import org.fuserleer.node.Node;
import org.fuserleer.time.Time;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.Numbers;
import org.fuserleer.utils.UInt256;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public final class StateHandler implements Service
{
	private static final Logger stateLog = Logging.getLogger("state");
	private static final Logger cerbyLog = Logging.getLogger("cerby");
	
	private enum CertificateStatus
	{
		SUCCESS, FAILED, SKIPPED;
	}

	private final Context context;
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	private final Map<Hash, PendingAtom> atoms = new HashMap<Hash, PendingAtom>();
	private final Map<StateKey<?, ?>, PendingAtom> states = new HashMap<StateKey<?, ?>, PendingAtom>();

	private final BlockingQueue<PendingAtom> executionQueue;
	private final MappedBlockingQueue<StateKey<?, ?>, PendingAtom> localProvisionQueue;
	private final Multimap<Hash, Entry<StateKey<?, ?>, UInt256>> provisionedStateInputs;

	private final MappedBlockingQueue<Hash, StateCertificate> certificatesToProcessQueue;

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
						if (StateHandler.this.context.getNode().isSynced() == false)
						{
							Thread.sleep(1000);
							continue;
						}

						List<StateCertificate> stateCertificatesToBroadcast = new ArrayList<StateCertificate>();
						if (StateHandler.this.certificatesToProcessQueue.isEmpty() == false)
						{
							Entry<Hash, StateCertificate> stateCertificate;
							while((stateCertificate = StateHandler.this.certificatesToProcessQueue.peek()) != null)
							{
								try
								{
									if (StateHandler.this.context.getLedger().getLedgerStore().store(StateHandler.this.context.getLedger().getHead().getHeight(), stateCertificate.getValue()).equals(OperationStatus.SUCCESS) == false)
									{
										cerbyLog.warn(StateHandler.this.context.getName()+": Already seen state certificate of "+stateCertificate.getValue()+" for "+stateCertificate.getValue().getAtom());
										continue;
									}

									CertificateStatus status = process(stateCertificate.getValue());
									if (status == CertificateStatus.SUCCESS)
										stateCertificatesToBroadcast.add(stateCertificate.getValue());
									else if (status == CertificateStatus.SKIPPED)
									{
										if (cerbyLog.hasLevel(Logging.DEBUG) == true)
											cerbyLog.debug(StateHandler.this.context.getName()+": Processing of state certificate "+stateCertificate.getValue().getHash()+" was skipped for atom "+stateCertificate.getValue().getAtom()+" in block "+stateCertificate.getValue().getBlock());
									}
									else
										cerbyLog.warn(StateHandler.this.context.getName()+": Processing of state certificate "+stateCertificate.getValue().getHash()+" failed for atom "+stateCertificate.getValue().getAtom()+" in block "+stateCertificate.getValue().getBlock());
								}
								catch (Exception ex)
								{
									stateLog.error(StateHandler.this.context.getName()+": Error processing certificate "+stateCertificate.getValue(), ex);
								}
								finally
								{
									if (StateHandler.this.certificatesToProcessQueue.remove(stateCertificate.getKey(), stateCertificate.getValue()) == false)
										throw new IllegalStateException("State certificate process peek/remove failed for "+stateCertificate.getValue());
								}
							}
						}
						
						try
						{
							if (stateCertificatesToBroadcast.isEmpty() == false)
							{
								StateHandler.this.context.getMetaData().increment("ledger.pool.state.certificates", stateCertificatesToBroadcast.size());
								
								for (StateCertificate stateCertificate : stateCertificatesToBroadcast)
								{
									PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(stateCertificate.getAtom(), CommitStatus.NONE);
									if (pendingAtom == null)
									{
										cerbyLog.warn(StateHandler.this.context.getName()+": Pending atom not found for "+stateCertificate.getHash()+".  Possibly committed");
										continue;
									}
									
									long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(stateCertificate.getHeight());
									Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
									shardGroups.remove(ShardMapper.toShardGroup(stateCertificate.getState().get(), numShardGroups));
									if (shardGroups.isEmpty() == false)
										StateHandler.this.context.getNetwork().getGossipHandler().broadcast(stateCertificate, shardGroups);
								}
							}
						}
						catch (Exception ex)
						{
							stateLog.error(StateHandler.this.context.getName()+": Error broadcasting state certificates "+stateCertificatesToBroadcast, ex);
						}

						Entry<StateKey<?, ?>, PendingAtom> stateToProvision = StateHandler.this.localProvisionQueue.peek(1, TimeUnit.SECONDS);
						if (stateToProvision != null)
						{
							StateKey<?, ?> stateKey = stateToProvision.getKey();
							PendingAtom pendingAtom = stateToProvision.getValue();
							try
							{
								if (pendingAtom.getStatus().equals(CommitStatus.PROVISIONING) == false)
									throw new IllegalStateException("Pending atom "+pendingAtom+" is not in PROVISIONING commit state");
								
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
			                		
			                		// Process any state certificates received early in case of early out possibility
			                		// TODO this doesn't make sense here anymore, can't build until executed?
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
										
										// Send to remote shard group validators that need it
										StandardPeerFilter standardPeerFilter = StandardPeerFilter.build(StateHandler.this.context).setStates(PeerState.CONNECTED);
										List<ConnectedPeer> connectedPeers = StateHandler.this.context.getNetwork().get(standardPeerFilter);
										Set<Long> touchedShardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
										for (ConnectedPeer connectedPeer : connectedPeers)
										{
											long remoteShardGroup = ShardMapper.toShardGroup(connectedPeer.getNode().getIdentity(), numShardGroups);
											if (remoteShardGroup == localShardGroup)
												continue;
											
											if (touchedShardGroups.contains(remoteShardGroup) == false)
												continue;
											
											StateMessage stateMessage = new StateMessage(pendingAtom.getHash(), stateKey, value, pendingAtom.getStatus());
											connectedPeer.send(stateMessage);
										}
										
										Collection<Entry<StateKey<?, ?>, UInt256>> provisionedStateInputs = StateHandler.this.provisionedStateInputs.removeAll(pendingAtom.getHash());
										for (Entry<StateKey<?, ?>, UInt256> provisionedStateInput : provisionedStateInputs)
											provision(pendingAtom, provisionedStateInput.getKey(), provisionedStateInput.getValue());
			            			}
			                	}
			                	else
			                		throw new IllegalStateException("Attempting to provision non-local state "+stateKey+" in atom "+pendingAtom.getHash());
							}
							catch (Exception ex)
							{
								cerbyLog.error(StateHandler.this.context.getName()+": Error processing provisioning for "+stateKey+" in atom "+pendingAtom.getHash(), ex);
								StateHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
							}
							finally
							{
								if (StateHandler.this.localProvisionQueue.remove(stateToProvision.getKey(), stateToProvision.getValue()) == false)
									throw new IllegalStateException("State provisioning queue peek/remove failed for "+stateToProvision.getKey());
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
								
								StateHandler.this.execute(pendingAtom);
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
		this.provisionedStateInputs = Multimaps.synchronizedSetMultimap(HashMultimap.create());
		this.localProvisionQueue = new MappedBlockingQueue<StateKey<?, ?>, PendingAtom>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.executionQueue = new LinkedBlockingQueue<PendingAtom>(this.context.getConfiguration().get("ledger.atom.queue", 1<<16));
		this.certificatesToProcessQueue = new MappedBlockingQueue<Hash, StateCertificate>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));

//		cerbyLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
//		stateLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
		cerbyLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
		stateLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
//		cerbyLog.setLevels(Logging.ERROR | Logging.FATAL);
//		stateLog.setLevels(Logging.ERROR | Logging.FATAL);
	}
	
	void provision(final PendingAtom pendingAtom)
	{
		provision(pendingAtom, pendingAtom.getStateKeys());
	}

	void provision(final PendingAtom pendingAtom, final Collection<StateKey<?, ?>> stateKeys)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom for provisioning is null");
		Objects.requireNonNull(stateKeys, "Pending atom state keys to provision is null");
		Numbers.isZero(stateKeys.size(), "Pending atom state keys to provision is empty");
		
		StateHandler.this.lock.readLock().lock();
		try
		{
			if (pendingAtom.equals(this.atoms.get(pendingAtom.getHash())) == false)
				throw new IllegalStateException("Expected pending atom "+pendingAtom.getHash()+" not found");
			
			if (cerbyLog.hasLevel(Logging.DEBUG) == true)
				cerbyLog.debug(this.context.getName()+": Provisioning state "+stateKeys+" for "+pendingAtom+" in block "+pendingAtom.getBlock());

			long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()));
			long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
			for (StateKey<?, ?> stateKey : stateKeys)
			{
				if (pendingAtom.equals(this.states.get(stateKey)) == false)
					throw new IllegalStateException("Expected pending state "+stateKey+" in atom "+pendingAtom.getHash()+" not found");
				
				long provisionShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
				if (provisionShardGroup != localShardGroup)
					continue;
				
				if (this.localProvisionQueue.putIfAbsent(stateKey, pendingAtom) != null)
					cerbyLog.warn(StateHandler.this.context.getName()+": Provisioning "+stateKey+" should be absent for "+pendingAtom.getHash());
			}
		}
		finally
		{
			StateHandler.this.lock.readLock().unlock();
		}
	}

	private void provision(final PendingAtom pendingAtom, final StateKey<?, ?> stateKey, final UInt256 value) throws InterruptedException, IOException
	{
		try
		{
			if (pendingAtom.provision(stateKey, value) == true)
			{
	    		if (cerbyLog.hasLevel(Logging.DEBUG))
					cerbyLog.debug(StateHandler.this.context.getName()+": Storing provisioned state information for atom "+pendingAtom.getHash()+" for state recovery");

	    		if (this.context.getLedger().getLedgerStore().store(pendingAtom.getInputs()).equals(OperationStatus.SUCCESS) == false)
					cerbyLog.warn(StateHandler.this.context.getName()+": Already stored provisioned state inputs for atom "+pendingAtom.getHash()+" in block "+pendingAtom.getBlock());
				
	    		if (cerbyLog.hasLevel(Logging.DEBUG))
					cerbyLog.debug(StateHandler.this.context.getName()+": Queuing pending atom "+pendingAtom.getHash()+" for execution");
	    		
	    		this.executionQueue.put(pendingAtom);
			}
		}
		catch (ValidationException vex)
		{
			// Let provisioning validation exceptions make it into the state pool as they represent a "no" vote for commit
			cerbyLog.error(StateHandler.this.context.getName()+": State machine throw validation exception processing provisioning for "+stateKey+" in atom "+pendingAtom.getHash(), vex);
			StateHandler.this.context.getEvents().post(new AtomExecutedEvent(pendingAtom, vex));
		}
	}
	
	void execute(final PendingAtom pendingAtom) throws ValidationException, IOException
	{
		pendingAtom.execute();
		this.context.getEvents().post(new AtomExecutedEvent(pendingAtom));
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
						if (StateHandler.this.certificatesToProcessQueue.contains(item) == true || 
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

/*		this.context.getNetwork().getMessaging().register(GetStateMessage.class, this.getClass(), new MessageProcessor<GetStateMessage>()
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
								if (pendingAtom != null && pendingAtom.getStatus().greaterThan(CommitStatus.PREPARED) == true && 
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
								pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(getStateMessage.getAtom(), CommitStatus.NONE);
								if (pendingAtom != null && pendingAtom.getStatus().greaterThan(CommitStatus.PREPARED) == true)
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
								
								StateInputs stateInputs = StateHandler.this.context.getLedger().getLedgerStore().get(new StateAddress(StateInputs.class, getStateMessage.getAtom()).get(), StateInputs.class);
								if (stateInputs != null)
								{
									value = stateInputs.getInput(getStateMessage.getKey()); 
									if (value != null)
									{
										status = CommitStatus.PROVISIONED;
										
										if (cerbyLog.hasLevel(Logging.DEBUG) == true)
											cerbyLog.debug(StateHandler.this.context.getName()+": State request "+getStateMessage.getKey()+" in atom "+getStateMessage.getAtom()+" served from persisted state inputs for " + peer);
	
										StateHandler.this.context.getNetwork().getMessaging().send(new StateMessage(getStateMessage.getAtom(), getStateMessage.getKey(), value.orElse(null), status), peer);
									}
								}
								
								// Pending atom is not found in any pool or commit. Need to set a future response
								if (pendingAtom == null)
									cerbyLog.warn(StateHandler.this.context.getName()+": Pending atom "+getStateMessage.getAtom()+" was null for state request "+getStateMessage.getKey()+" for " + peer);

								// Pending atom was found but is not in a suitable state to serve the request. Need to set a future response
								if (pendingAtom != null && pendingAtom.getStatus().lessThan(CommitStatus.ACCEPTED) == false)
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
		});*/

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
						long numShardGroups = StateHandler.this.context.getLedger().numShardGroups();
						long stateShardGroup = ShardMapper.toShardGroup(stateMessage.getKey().get(), numShardGroups);
						long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
						if (stateShardGroup == localShardGroup)
						{
							cerbyLog.error(StateHandler.this.context.getName()+": Received state input "+stateMessage.getKey()+" for local shard group");
							// 	Disconnected and ban
							return;
						}

						StateHandler.this.lock.writeLock().lock();
						try
						{
							PendingAtom atom = StateHandler.this.atoms.get(stateMessage.getAtom());
							if (atom == null)
							{
								Commit commit = StateHandler.this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, stateMessage.getAtom()));
								if (commit != null)
								{
									if (commit.isTimedout() == true || commit.getPath().get(Elements.CERTIFICATE) != null)
									{
										cerbyLog.warn(StateHandler.this.context.getName()+": Received unexpected state message "+stateMessage.getKey()+":"+stateMessage.getValue()+" with status "+stateMessage.getStatus()+" for atom "+stateMessage.getAtom()+" from " + peer);
										return;
									}
								}
							}
							
							if (atom == null || atom.getStatus().lessThan(CommitStatus.PROVISIONING) == true)
							{
								if (StateHandler.this.provisionedStateInputs.put(stateMessage.getAtom(), new AbstractMap.SimpleEntry<StateKey<?, ?>, UInt256>(stateMessage.getKey(), stateMessage.getValue())) == true)
								{
									if (cerbyLog.hasLevel(Logging.DEBUG) == true)
										cerbyLog.debug(StateHandler.this.context.getName()+": Cached state message "+stateMessage.getKey()+":"+stateMessage.getValue()+" with status "+stateMessage.getStatus()+" for atom "+stateMessage.getAtom()+" from " + peer);
								}
							}
							else if (atom.getStatus().equals(CommitStatus.PROVISIONING) == true)
								provision(atom, stateMessage.getKey(), stateMessage.getValue());
						}
						catch (Exception ex)
						{
							cerbyLog.error(StateHandler.this.context.getName()+": ledger.messages.state" + peer, ex);
						}
						finally
						{
							StateHandler.this.lock.writeLock().unlock();
						}
					}
				});
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
						StateHandler.this.lock.readLock().lock();
						try
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StateHandler.this.context.getName()+": State pool (certificates) inventory request from "+peer);
							
							final Set<PendingAtom> pendingAtoms = new HashSet<PendingAtom>(StateHandler.this.atoms.values());
							final Set<Hash> stateCertificateInventory = new LinkedHashSet<Hash>();
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
								
								for (StateKey<?, ?> stateKey : pendingAtom.getStateKeys())
								{
									long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()));
									long stateShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
									long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
									
									if (localShardGroup == stateShardGroup)
										continue;

									Optional<UInt256> value = pendingAtom.getInput(stateKey);
									if (value == null)
										continue;
									
									StateMessage stateMessage = new StateMessage(pendingAtom.getHash(), stateKey, value.orElse(null), pendingAtom.getStatus());
									peer.send(stateMessage);
								}
								
								for (Entry<StateKey<?, ?>, UInt256> provisionedStateInput : StateHandler.this.provisionedStateInputs.get(pendingAtom.getHash()))
								{
									long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()));
									long stateShardGroup = ShardMapper.toShardGroup(provisionedStateInput.getKey().get(), numShardGroups);
									long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
									
									if (localShardGroup == stateShardGroup)
										continue;

									StateMessage stateMessage = new StateMessage(pendingAtom.getHash(), provisionedStateInput.getKey(), provisionedStateInput.getValue(), pendingAtom.getStatus());
									peer.send(stateMessage);
								}
							}
							
							long height = StateHandler.this.context.getLedger().getHead().getHeight();
							while (height >= Math.max(0, syncAcquiredMessage.getHead().getHeight() - Node.OOS_TRIGGER_LIMIT))
							{
								// TODO optimise
								for (Hash stateCertificateHash : StateHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, StateCertificate.class))
								{
									StateCertificate stateCertificate = StateHandler.this.context.getLedger().getLedgerStore().get(stateCertificateHash, StateCertificate.class);
									long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(stateCertificate.getHeight());
									long stateShardGroup = ShardMapper.toShardGroup(stateCertificate.getState().get(), numShardGroups);
									long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
									
									if (localShardGroup == stateShardGroup)
										// Handled by sending state votes from state pool 
										continue;

									stateCertificateInventory.add(stateCertificate.getHash());
								}
								height--;
							}
							
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StateHandler.this.context.getName()+": Broadcasting about "+stateCertificateInventory+" pool state certificates to "+peer);

							while(stateCertificateInventory.isEmpty() == false)
							{
								SyncInventoryMessage stateCertificateInventoryMessage = new SyncInventoryMessage(stateCertificateInventory, 0, Math.min(BroadcastInventoryMessage.MAX_ITEMS, stateCertificateInventory.size()), StateCertificate.class);
								StateHandler.this.context.getNetwork().getMessaging().send(stateCertificateInventoryMessage, peer);
								stateCertificateInventory.removeAll(stateCertificateInventoryMessage.getItems());
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
	
	public Collection<Hash> pending()
	{
		this.lock.readLock().lock();
		try
		{
			List<Hash> pending = this.atoms.values().stream().filter(pa -> pa.getCertificate() != null).map(pa -> pa.getCertificate().getHash()).collect(Collectors.toList());
			Collections.sort(pending);
			return pending;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public Collection<Hash> provisioned()
	{
		this.lock.readLock().lock();
		try
		{
			List<Hash> provisioned = this.provisionedStateInputs.values().stream().map(psi -> psi.getKey().get()).collect(Collectors.toList());
			Collections.sort(provisioned);
			return provisioned;
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
	
	private CertificateStatus process(final StateCertificate certificate) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(certificate, "State certificate is null");
		
		StateHandler.this.lock.writeLock().lock();
		PendingAtom pendingAtom = null;
		try
		{
			pendingAtom = this.context.getLedger().getAtomHandler().get(certificate.getAtom(), CommitStatus.ACCEPTED);
			if (pendingAtom == null)
			{
				Commit commit = this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, certificate.getAtom()));
				if (commit != null && commit.getPath().get(Elements.CERTIFICATE) != null)
					return CertificateStatus.SKIPPED;

				throw new IllegalStateException("Pending atom "+certificate.getAtom()+" required by certificate "+certificate.getHash()+" not found");
			}
		
			if (pendingAtom.addCertificate(certificate) == false)
				return CertificateStatus.FAILED;
				
			tryFinalize(pendingAtom);
			return CertificateStatus.SUCCESS;
		}
		finally
		{
			StateHandler.this.lock.writeLock().unlock();
		}
	}
	
	private boolean tryFinalize(final PendingAtom pendingAtom) throws IOException, CryptoException, ValidationException
	{
		// Don't build atom certificate from state certificates until executed
		if (pendingAtom.getStatus().greaterThan(CommitStatus.PROVISIONED) == false)
			return false;

		if (pendingAtom.getCertificate() != null)
			return false;
		
		AtomCertificate atomCertificate = pendingAtom.buildCertificate();
		if (atomCertificate != null)
		{
			
			this.context.getMetaData().increment("ledger.pool.atom.certificates");
			if (cerbyLog.hasLevel(Logging.DEBUG) == true)
				cerbyLog.debug(this.context.getName()+": Created atom certificate "+atomCertificate.getHash()+" for atom "+atomCertificate.getHash()+" with decision "+atomCertificate.getDecision());

			this.context.getEvents().post(new AtomCertificateEvent(atomCertificate));
			return true;
		}
		
		return false;
	}
	
	boolean add(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		StateHandler.this.lock.writeLock().lock();
		try
		{
			if (this.atoms.putIfAbsent(pendingAtom.getHash(), pendingAtom) == null)
			{
				if (cerbyLog.hasLevel(Logging.DEBUG) == true)
					cerbyLog.debug(this.context.getName()+": Adding state for "+pendingAtom+" in block "+pendingAtom.getBlock());
				
				for (StateKey<?, ?> stateKey : pendingAtom.getStateKeys())
				{
					if (this.states.putIfAbsent(stateKey, pendingAtom) != null)
						cerbyLog.warn(this.context.getName()+": State "+stateKey+" should be absent for "+pendingAtom.getHash());
					else if (cerbyLog.hasLevel(Logging.DEBUG) == true)
						cerbyLog.debug(this.context.getName()+": Added state "+stateKey+" for "+pendingAtom+" in block "+pendingAtom.getBlock());
				}
				
				return true;
			}
			
			return false;
		}
		finally
		{
			StateHandler.this.lock.writeLock().unlock();
		}
	}

	private void remove(final PendingAtom pendingAtom) throws IOException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		StateHandler.this.lock.writeLock().lock();
		try
		{
			if (cerbyLog.hasLevel(Logging.DEBUG) == true)
				cerbyLog.debug(this.context.getName()+": Removing states for "+pendingAtom+" in block "+pendingAtom.getBlock());

			boolean removed = pendingAtom.equals(StateHandler.this.atoms.remove(pendingAtom.getHash()));
			pendingAtom.getStateKeys().forEach(sk -> {
				if (StateHandler.this.states.remove(sk, pendingAtom) == true && cerbyLog.hasLevel(Logging.DEBUG))
					cerbyLog.debug(this.context.getName()+": Removed state "+sk+" for "+pendingAtom+" in block "+pendingAtom.getBlock());
			});

			this.provisionedStateInputs.removeAll(pendingAtom.getHash());
			pendingAtom.getStateKeys().forEach(sk -> StateHandler.this.localProvisionQueue.remove(sk, pendingAtom));
			
			if (pendingAtom.getStatus().greaterThan(CommitStatus.PREPARED) && removed == false)
				throw new IllegalStateException("Expected pending atom "+pendingAtom.getHash()+" but was not found");
			
			// Store any partially provisioned state inputs
			if (pendingAtom.getStatus().equals(CommitStatus.PROVISIONING) == true)
			{
				StateInputs stateInputs = pendingAtom.getInputs();
				if (stateInputs.isEmpty() == false)
					this.context.getLedger().getLedgerStore().store(pendingAtom.getInputs());
			}
		}
		finally
		{
			StateHandler.this.lock.writeLock().unlock();
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
			
			StateHandler.this.certificatesToProcessQueue.put(stateCertificateEvent.getCertificate().getHash(), stateCertificateEvent.getCertificate());
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
				
				StateHandler.this.context.getLedger().getLedgerStore().store(StateHandler.this.context.getLedger().getHead().getHeight(), atomCertificateEvent.getCertificate());
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
		public void on(final AtomAcceptedEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomRejectedEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomDiscardedEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
			if (event.getPendingAtom().getStatus().greaterThan(CommitStatus.PREPARED) == true)
				StateHandler.this.context.getLedger().getLedgerStore().timedOut(event.getPendingAtom().getHash());
		}

		@Subscribe
		public void on(final AtomExceptionEvent event) throws IOException 
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
				for (Atom atom : blockCommittedEvent.getBlock().getAtoms())
				{
					PendingAtom pendingAtom = StateHandler.this.atoms.get(atom.getHash());
					if (pendingAtom == null)
						continue;
					
            		if (cerbyLog.hasLevel(Logging.DEBUG))
						cerbyLog.debug(StateHandler.this.context.getName()+": Queuing pending atom "+pendingAtom.getHash()+" for provisioning");

					provision(pendingAtom);
				}
			}
			catch (Exception ex)
			{
				cerbyLog.fatal(StateHandler.class.getName()+": Failed to provision pending atom set for "+blockCommittedEvent.getBlock().getHeader()+" when processing async BlockCommittedEvent", ex);
				return;
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
		public void on(final BlockCommittedEvent blockCommittedEvent) 
		{
			StateHandler.this.lock.writeLock().lock();
			try
			{
				Set<PendingAtom> committed = new HashSet<PendingAtom>();
				try
				{
					// Creating pending atom from accepted event if not seen // This is the most likely place for a pending atom object to be created
					for (Atom atom : blockCommittedEvent.getBlock().getAtoms())
					{
						PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(atom.getHash(), CommitStatus.NONE);
						if (pendingAtom == null)
							throw new IllegalStateException("Pending atom "+atom.getHash()+" not found");
						
						add(pendingAtom);
					}
				}
				catch (Exception ex)
				{
					cerbyLog.fatal(StateHandler.class.getName()+": Failed to create PendingAtom set for "+blockCommittedEvent.getBlock().getHeader()+" when processing BlockCommittedEvent", ex);
					return;
				}
				
				// Commit atom states
				for (AtomCertificate certificate : blockCommittedEvent.getBlock().getCertificates())
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
						if (pendingAtom.getStatus().greaterThan(CommitStatus.PREPARED) == true && 
							blockCommittedEvent.getBlock().getHeader().getHeight() > pendingAtom.getCommitBlockTimeout() && 
							Time.getSystemTime() > pendingAtom.getAcceptedAt() + PendingAtom.ATOM_INCLUSION_TIMEOUT_CLOCK_SECONDS)
							timedOut.add(pendingAtom);
					}

					// Timed out atoms may have been committed on the timeout block ... allow commit
					// TODO need to test this isn't strongly-subjective otherwise some could allow, some may have already timed out
					timedOut.removeAll(committed);
					for (PendingAtom pendingAtom : timedOut)
					{
						if (pendingAtom.getAtom() == null)
							cerbyLog.warn(StateHandler.this.context.getName()+": Atom "+pendingAtom.getHash()+" timeout but never seen at "+blockCommittedEvent.getBlock().getHeader());
						else
						{
							cerbyLog.warn(StateHandler.this.context.getName()+": Atom "+pendingAtom.getHash()+" timeout at block "+blockCommittedEvent.getBlock().getHeader());
//							remove(pendingAtom);
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
			StateHandler.this.lock.writeLock().lock();
			try
			{
				if (event.isSynced() == true)
				{
					cerbyLog.info(StateHandler.this.context.getName()+": Sync status changed to "+event.isSynced()+", loading known state handler state");
					for (long height = Math.max(0, StateHandler.this.context.getLedger().getHead().getHeight() - Node.OOS_TRIGGER_LIMIT) ; height <= StateHandler.this.context.getLedger().getHead().getHeight() ; height++)
					{
						try
						{
							Collection<Hash> items = StateHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, AtomCertificate.class);
							for (Hash item : items)
							{
								AtomCertificate atomCertificate = StateHandler.this.context.getLedger().getLedgerStore().get(item, AtomCertificate.class);
								PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(atomCertificate.getAtom(), CommitStatus.ACCEPTED);
								if (pendingAtom == null)
									continue;
								
								pendingAtom.setCertificate(atomCertificate);
								atomCertificate.getAll().forEach(sc -> pendingAtom.addCertificate(sc));
								add(pendingAtom);
							}
						}
						catch (Exception ex)
						{
							cerbyLog.error(StateHandler.this.context.getName()+": Failed to load atom certificate for state handler at height "+height, ex);
						}
					}
					
					for (long height = Math.max(0, StateHandler.this.context.getLedger().getHead().getHeight() - Node.OOS_TRIGGER_LIMIT) ; height <= StateHandler.this.context.getLedger().getHead().getHeight() ; height++)
					{
						try
						{
							Collection<Hash> items = StateHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, StateCertificate.class);
							for (Hash item : items)
							{
								StateCertificate stateCertificate = StateHandler.this.context.getLedger().getLedgerStore().get(item, StateCertificate.class);
								PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(stateCertificate.getAtom(), CommitStatus.ACCEPTED);
								if (pendingAtom == null)
									continue;
								
								if (pendingAtom.getCertificate() != null)
									continue;
								
								add(pendingAtom);
									
								CertificateStatus status = process(stateCertificate);
								if (status == CertificateStatus.SKIPPED)
								{
									if (stateLog.hasLevel(Logging.DEBUG) == true)
										stateLog.debug(StateHandler.this.context.getName()+": Syncing of state certificate "+stateCertificate.getHash()+" was skipped for atom "+stateCertificate.getAtom()+" in block "+stateCertificate.getBlock());
								}
								else if (status == CertificateStatus.FAILED)
									stateLog.warn(StateHandler.this.context.getName()+": Syncing of state certificate "+stateCertificate.getHash()+" failed for atom "+stateCertificate.getAtom()+" in block "+stateCertificate.getBlock());
							}
						}
						catch (Exception ex)
						{
							cerbyLog.error(StateHandler.this.context.getName()+": Failed to load state certificates for state handler at height "+height, ex);
						}
					}					
				}
				else
				{
					stateLog.info(StateHandler.this.context.getName()+": Sync status changed to "+event.isSynced()+", flushing state handler");
					StateHandler.this.atoms.clear();
					StateHandler.this.executionQueue.clear();
					StateHandler.this.localProvisionQueue.clear();
					StateHandler.this.provisionedStateInputs.clear();
					StateHandler.this.states.clear();
					StateHandler.this.certificatesToProcessQueue.clear();
				}
			}
			finally
			{
				StateHandler.this.lock.writeLock().unlock();
			}
		}
	};
}
