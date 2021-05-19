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
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
import org.fuserleer.ledger.LedgerStore.SyncInventoryType;
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.AtomPositiveCommitEvent;
import org.fuserleer.ledger.events.AtomCertificateEvent;
import org.fuserleer.ledger.events.AtomCommitEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomExecutedEvent;
import org.fuserleer.ledger.events.AtomNegativeCommitEvent;
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
import org.fuserleer.network.messages.BroadcastInventoryMessage;
import org.fuserleer.network.messages.SyncInventoryMessage;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.node.Node;
import org.fuserleer.utils.Ints;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.Numbers;
import org.fuserleer.utils.UInt256;

import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public final class StateHandler implements Service
{
	private static final Logger stateLog = Logging.getLogger("state");
	private static final Logger cerbyLog = Logging.getLogger("cerby");
	
	static
	{
//		cerbyLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
//		stateLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
		cerbyLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
		stateLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
//		cerbyLog.setLevels(Logging.ERROR | Logging.FATAL);
//		stateLog.setLevels(Logging.ERROR | Logging.FATAL);
	}
	
	private enum CertificateStatus
	{
		SUCCESS, FAILED, SKIPPED;
	}

	private final Context context;
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	private final Map<Hash, PendingState> pendingStates = Collections.synchronizedMap(new HashMap<Hash, PendingState>());

	private final BlockingQueue<PendingAtom> executionQueue;
	private final Map<Hash, StateInput> remoteProvisionDelayed;
	private final MappedBlockingQueue<Hash, StateInput> remoteProvisionQueue;
	private final MappedBlockingQueue<PendingState, PendingAtom> localProvisionQueue;

	private final Map<Hash, StateCertificate> certificatesToProcessDelayed;
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
										stateLog.warn(StateHandler.this.context.getName()+": Already seen state certificate of "+stateCertificate.getValue()+" for "+stateCertificate.getValue().getAtom());
										continue;
									}

									CertificateStatus status = process(stateCertificate.getValue());
									if (status == CertificateStatus.SUCCESS)
									{
										if (stateLog.hasLevel(Logging.DEBUG) == true)
											stateLog.debug(StateHandler.this.context.getName()+": Processed state certificate "+stateCertificate.getValue().getHash()+" for atom "+stateCertificate.getValue().getAtom()+" in block "+stateCertificate.getValue().getBlock());

										stateCertificatesToBroadcast.add(stateCertificate.getValue());
									}
									else if (status == CertificateStatus.SKIPPED)
									{
										if (stateLog.hasLevel(Logging.DEBUG) == true)
											stateLog.debug(StateHandler.this.context.getName()+": Processing of state certificate "+stateCertificate.getValue().getHash()+" was skipped for atom "+stateCertificate.getValue().getAtom()+" in block "+stateCertificate.getValue().getBlock());
									}
									else
										stateLog.warn(StateHandler.this.context.getName()+": Processing of state certificate "+stateCertificate.getValue().getHash()+" failed for atom "+stateCertificate.getValue().getAtom()+" in block "+stateCertificate.getValue().getBlock());
								}
								catch (Exception ex)
								{
									stateLog.error(StateHandler.this.context.getName()+": Error processing certificate "+stateCertificate.getValue(), ex);
								}
								finally
								{
									if (StateHandler.this.certificatesToProcessQueue.remove(stateCertificate.getKey(), stateCertificate.getValue()) == false)
										// FIXME sync state can initially flip/flop between ... annoying, so just throw these as warns for now (theres are in all queue handlers!)
										cerbyLog.warn(StateHandler.this.context.getName()+": State certificate process peek/remove failed for "+stateCertificate.getValue());
//										throw new IllegalStateException("State certificate process peek/remove failed for "+stateCertificate.getValue());
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
									PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(stateCertificate.getAtom());
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

						Entry<PendingState, PendingAtom> stateToProvision = StateHandler.this.localProvisionQueue.peek(1, TimeUnit.SECONDS);
						if (stateToProvision != null)
						{
							PendingState pendingState = stateToProvision.getKey();
							PendingAtom pendingAtom = stateToProvision.getValue();
							try
							{
								if (pendingAtom.getStatus().equals(CommitStatus.PROVISIONING) == false && pendingAtom.getStatus().equals(CommitStatus.AUTOMATA) == false)
									throw new IllegalStateException("Pending atom "+pendingAtom+" is not in PROVISIONING or AUTOMATA commit state");
								
								if (cerbyLog.hasLevel(Logging.DEBUG))
									cerbyLog.debug(StateHandler.this.context.getName()+": Provisioning state "+pendingState+" for atom "+pendingAtom.getHash());

								long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()));
								long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
								long provisionShardGroup = ShardMapper.toShardGroup(pendingState.getKey().get(), numShardGroups);
			                	if (provisionShardGroup == localShardGroup)
			                	{
			            			UInt256 value = StateHandler.this.context.getLedger().getLedgerStore().get(pendingState.getKey());
			                		if (stateLog.hasLevel(Logging.DEBUG))
										stateLog.debug(StateHandler.this.context.getName()+": State "+pendingState.getKey()+" in atom "+pendingAtom.getHash()+" was provisioned locally");
			                		
			                		// Process any state certificates received early in case of early out possibility
			                		// TODO this doesn't make sense here anymore, can't build until executed?
			            			if (pendingAtom.getCertificate() == null && pendingAtom.buildCertificate() == null)
			            			{
				                		// No certificate possible with early received state certificates so provision local state
			            				if (pendingAtom.thrown() != null)
										{
											if (stateLog.hasLevel(Logging.DEBUG))
												stateLog.debug(StateHandler.this.context.getName()+": Aborting state provisioning "+pendingState.getKey()+" for atom "+pendingAtom.getHash()+" as execution exception thrown");
											
											continue;
										}

			            				if (pendingAtom.getInput(pendingState.getKey()) == null)
			            				{
											StateInput stateInput = new StateInput(pendingAtom.getHash(), pendingState.getKey(), value); 
											if (stateLog.hasLevel(Logging.DEBUG))
												stateLog.debug(StateHandler.this.context.getName()+": Storing local provisioned state "+pendingState.getKey()+" for atom "+pendingAtom.getHash()+" for state recovery");

								    		if (StateHandler.this.context.getLedger().getLedgerStore().store(StateHandler.this.context.getLedger().getHead().getHeight(), stateInput).equals(OperationStatus.SUCCESS) == false)
												stateLog.warn(StateHandler.this.context.getName()+": Already stored provisioned state "+pendingState.getKey()+" for atom "+pendingAtom.getHash()+" in block "+pendingAtom.getBlock());

								    		provision(pendingAtom, stateInput);

											Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
											if (provisionShardGroup == localShardGroup)
												shardGroups.remove(provisionShardGroup);
											if (shardGroups.isEmpty() == false)
												StateHandler.this.context.getNetwork().getGossipHandler().broadcast(stateInput, shardGroups);
			            				}
			            				else
			            					stateLog.warn(StateHandler.this.context.getName()+": State "+pendingState.getKey()+" is already provisioned for pending atom "+pendingAtom+" (recent restart?) ");

										if (pendingAtom.isProvisioned() == false)
										{
											for (StateKey<?, ?> provisionedStateKey : pendingAtom.getStateKeys())
											{
					            				if (pendingAtom.getInput(provisionedStateKey) == null)
					            				{
					            					StateInput stateInput = StateHandler.this.context.getLedger().getLedgerStore().get(StateInput.getHash(pendingAtom.getHash(), provisionedStateKey), StateInput.class);
					            					if (stateInput == null)
					            						continue;

					            					StateHandler.this.remoteProvisionQueue.remove(stateInput.getHash());
					            					StateHandler.this.remoteProvisionDelayed.remove(stateInput.getHash());
					            					provision(pendingAtom, stateInput);
					            					
													provisionShardGroup = ShardMapper.toShardGroup(provisionedStateKey.get(), numShardGroups);
													Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
													if (provisionShardGroup == localShardGroup)
														shardGroups.remove(provisionShardGroup);
													if (shardGroups.isEmpty() == false)
														StateHandler.this.context.getNetwork().getGossipHandler().broadcast(stateInput, shardGroups);
					            				}
											}
										}
			            			}
			                	}
			                	else
			                		throw new IllegalStateException("Attempting to provision non-local state "+pendingState.getKey()+" in atom "+pendingAtom.getHash());
							}
							catch (Exception ex)
							{
								cerbyLog.error(StateHandler.this.context.getName()+": Error processing provisioning for "+pendingState.getKey()+" in atom "+pendingAtom.getHash(), ex);
								StateHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
							}
							finally
							{
								if (StateHandler.this.localProvisionQueue.remove(stateToProvision.getKey(), stateToProvision.getValue()) == false)
									// FIXME sync state can initially flip/flop between ... annoying, so just throw these as warns for now (theres are in all queue handlers!)
									cerbyLog.warn(StateHandler.this.context.getName()+": State provisioning queue peek/remove failed for "+stateToProvision.getKey());
//									throw new IllegalStateException("State provisioning queue peek/remove failed for "+stateToProvision.getKey());
							}
						}
							
						Entry<Hash, StateInput> stateInput;
						while((stateInput = StateHandler.this.remoteProvisionQueue.peek()) != null)
						{
							try
							{
								if (stateLog.hasLevel(Logging.DEBUG))
									stateLog.debug(StateHandler.this.context.getName()+": Storing remote provisioned state "+stateInput.getValue().getKey()+" for atom "+stateInput.getValue().getAtom()+" for state recovery");

					    		if (StateHandler.this.context.getLedger().getLedgerStore().store(StateHandler.this.context.getLedger().getHead().getHeight(), stateInput.getValue()).equals(OperationStatus.SUCCESS) == false)
					    		{
									stateLog.warn(StateHandler.this.context.getName()+": Already stored provisioned state "+stateInput.getValue().getKey()+" for atom "+stateInput.getValue().getAtom());
									continue;
					    		}

								PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(stateInput.getValue().getAtom());
								if (pendingAtom == null)
								{
									cerbyLog.warn(StateHandler.this.context.getName()+": Pending atom not found for state input "+stateInput.getValue()+".  Possibly committed");
									continue;
								}

								if ((pendingAtom.getStatus().equals(CommitStatus.PROVISIONING) == true || pendingAtom.getStatus().equals(CommitStatus.AUTOMATA) == true) && 
									pendingAtom.getInput(stateInput.getValue().getKey()) == null)
								{
									provision(pendingAtom, stateInput.getValue());
								
									long numShardGroups = StateHandler.this.context.getLedger().numShardGroups();
									long stateShardGroup = ShardMapper.toShardGroup(stateInput.getValue().getKey().get(), numShardGroups);
									long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
									Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
									if (stateShardGroup == localShardGroup)
										shardGroups.remove(stateShardGroup);
									if (shardGroups.isEmpty() == false)
										StateHandler.this.context.getNetwork().getGossipHandler().broadcast(stateInput.getValue(), shardGroups);
								}
							}
							catch (Exception ex)
							{
								stateLog.error(StateHandler.this.context.getName()+": Error processing state input "+stateInput.getValue(), ex);
							}
							finally
							{
								if (StateHandler.this.remoteProvisionQueue.remove(stateInput.getKey(), stateInput.getValue()) == false)
									// FIXME sync state can initially flip/flop between ... annoying, so just throw these as warns for now (theres are in all queue handlers!)
									cerbyLog.warn(StateHandler.this.context.getName()+": State provisioning queue peek/remove failed for "+stateInput.getValue());
//									throw new IllegalStateException("State input process peek/remove failed for "+stateInput.getValue());
							}
						}
						
						// Delayed state certificates
						final List<Hash> certificateRemovals = new ArrayList<Hash>();
						StateHandler.this.certificatesToProcessDelayed.forEach((h,sc) -> 
						{
							try
							{
								PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(sc.getAtom());
								if (pendingAtom != null && pendingAtom.getStatus().greaterThan(CommitStatus.ACCEPTED) == true)
								{
									StateHandler.this.certificatesToProcessQueue.put(sc.getHash(), sc);
									certificateRemovals.add(h);
								}
							}
							catch (IOException ioex)
							{
								cerbyLog.error(StateHandler.this.context.getName()+": Failed to process certificate delayed for atom "+sc.getAtom(), ioex);
							}
						});
						synchronized(StateHandler.this.certificatesToProcessDelayed) { StateHandler.this.certificatesToProcessDelayed.keySet().removeAll(certificateRemovals); }
						
						// Delayed state inputs 
						final List<Hash> inputRemovals = new ArrayList<Hash>();
						StateHandler.this.remoteProvisionDelayed.forEach((h,si) -> 
						{
							try
							{
								PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(si.getAtom());
								if (pendingAtom != null && pendingAtom.getStatus().greaterThan(CommitStatus.ACCEPTED) == true)
								{
									StateHandler.this.remoteProvisionQueue.put(si.getHash(), si);
									inputRemovals.add(h);
								}
							}
							catch (IOException ioex)
							{
								cerbyLog.error(StateHandler.this.context.getName()+": Failed to process provision delayed for atom "+si.getAtom(), ioex);
							}
						});
						synchronized(StateHandler.this.remoteProvisionDelayed) { StateHandler.this.remoteProvisionDelayed.keySet().removeAll(inputRemovals); }

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
		this.localProvisionQueue = new MappedBlockingQueue<PendingState, PendingAtom>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.remoteProvisionDelayed = Collections.synchronizedMap(new HashMap<Hash, StateInput>());
		this.remoteProvisionQueue = new MappedBlockingQueue<Hash, StateInput>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.executionQueue = new LinkedBlockingQueue<PendingAtom>(this.context.getConfiguration().get("ledger.atom.queue", 1<<16));
		this.certificatesToProcessQueue = new MappedBlockingQueue<Hash, StateCertificate>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.certificatesToProcessDelayed = Collections.synchronizedMap(new HashMap<Hash, StateCertificate>());
	}
	
	public int size()
	{
		return this.pendingStates.size();
	}

	/**
	 * Returns all pending states.
	 * 
	 * @throws  
	 */
	public Collection<PendingState> getAll()
	{
		// Dont use the state handler lock here.  
		// Just sync on the pending state object as this function is called from many places
		synchronized(this.pendingStates)
		{
			return new ArrayList<PendingState>(this.pendingStates.values());
		}
	}

	/**
	 * Returns an existing pending state or creates it providing that it is not timed out or committed.
	 * 
	 * @param state The pending state hash
	 * @throws IOException
	 * @throws  
	 */
	PendingState get(final Hash block, final Hash atom, final StateKey<?, ?> stateKey) throws IOException
	{
		Objects.requireNonNull(stateKey, "State key is null");
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is zero");
		Objects.requireNonNull(block, "Block hash is null");
		Hash.notZero(block, "Block hash is zero");
		
		if (Ints.fromByteArray(block.toByteArray()) > 0)
			throw new IllegalArgumentException("Block hash doesn't look like a block hash");

		if (Ints.fromByteArray(atom.toByteArray()) == 0)
			throw new IllegalArgumentException("Atom hash doesn't look like a atom hash");

		// TODO dont think this is needed now
		if (this.context.getNode().isSynced() == false)
			throw new IllegalStateException("Sync state is false!  StateHandler::get called");

		// Dont use the state handler lock here.  
		// Just sync on the pending state object as this function is called from many places
		synchronized(this.pendingStates)
		{
			Hash pendingStateHash = PendingState.getHash(atom, stateKey);
			PendingState pendingState = this.pendingStates.get(pendingStateHash);
			if (pendingState != null)
				return pendingState;
			
			Commit commit = this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, atom));
			if (commit != null)
			{
				if (commit.getPath().get(Elements.CERTIFICATE) != null)
					return null;
						
				if (commit.isCommitTimedout() == true)
					return null;
					
				if (commit.isAcceptTimedout() == true)
					return null;
			}
	
			pendingState = new PendingState(this.context, stateKey, atom, block);
			this.pendingStates.put(pendingStateHash, pendingState);
			this.context.getMetaData().increment("ledger.pool.state.added");

			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Pending state "+stateKey+" creation stack", new Exception());

			return pendingState;
		}
	}

	void provision(final PendingAtom pendingAtom, final Collection<StateKey<?, ?>> stateKeys) throws IOException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom for provisioning is null");
		Objects.requireNonNull(stateKeys, "Pending atom state keys to provision is null");
		Numbers.isZero(stateKeys.size(), "Pending atom state keys to provision is empty");
		
		StateHandler.this.lock.readLock().lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Provisioning state "+stateKeys+" for "+pendingAtom+" in block "+pendingAtom.getBlock());

			long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()));
			long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
			for (StateKey<?, ?> stateKey : stateKeys)
			{
				PendingState pendingState = get(pendingAtom.getBlock(), pendingAtom.getHash(), stateKey);
				if (pendingState == null || pendingAtom.getHash().equals(pendingState.getAtom()) == false)
					throw new IllegalStateException("Expected pending state "+stateKey+" in atom "+pendingAtom.getHash()+" not found");
				
				long provisionShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
				if (provisionShardGroup != localShardGroup)
					continue;
				
				if (this.localProvisionQueue.putIfAbsent(pendingState, pendingAtom) != null)
					stateLog.warn(StateHandler.this.context.getName()+": Provisioning "+stateKey+" should be absent for "+pendingAtom.getHash());
			}
		}
		finally
		{
			StateHandler.this.lock.readLock().unlock();
		}
	}

	void provision(final PendingAtom pendingAtom, final StateKey<?, ?> stateKey) throws IOException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom for provisioning is null");
		Objects.requireNonNull(stateKey, "Pending atom state key to provision is null");
		
		StateHandler.this.lock.readLock().lock();
		try
		{
			if (pendingAtom.getInput(stateKey) != null)
				throw new IllegalStateException("State "+stateKey+" is already provisioned for "+pendingAtom+" in block"+pendingAtom.getBlock());
			
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Provisioning state "+stateKey+" for "+pendingAtom+" in block "+pendingAtom.getBlock());

			long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()));
			long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
			PendingState pendingState = get(pendingAtom.getBlock(), pendingAtom.getHash(), stateKey);
			if (pendingState == null || pendingAtom.getHash().equals(pendingState.getAtom()) == false)
				throw new IllegalStateException("Expected pending state "+stateKey+" in atom "+pendingAtom.getHash()+" not found");
				
			long provisionShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
			if (provisionShardGroup == localShardGroup)
			{
				if (this.localProvisionQueue.putIfAbsent(pendingState, pendingAtom) != null)
					stateLog.warn(StateHandler.this.context.getName()+": Provisioning "+stateKey+" should be absent for "+pendingAtom.getHash());
			}
		}
		finally
		{
			StateHandler.this.lock.readLock().unlock();
		}
	}

	private void provision(final PendingAtom pendingAtom, final StateInput stateinput) throws InterruptedException, IOException
	{
		try
		{
			if (pendingAtom.provision(stateinput) == true)
			{
				if (pendingAtom.getStatus().equals(CommitStatus.PROVISIONED) == true)
				{
		    		if (stateLog.hasLevel(Logging.DEBUG))
						stateLog.debug(StateHandler.this.context.getName()+": Queuing pending atom "+pendingAtom.getHash()+" for execution");
		    		
		    		this.executionQueue.put(pendingAtom);
				}
				// TODO Shouldnt be here, needed for executing automata results outside of a worker...change it
				else if (pendingAtom.getStatus().equals(CommitStatus.EXECUTED) == true)
					this.context.getEvents().post(new AtomExecutedEvent(pendingAtom));
			}
		}
		catch (ValidationException vex)
		{
			// Let provisioning validation exceptions make it into the state pool as they represent a "no" vote for commit
			stateLog.error(StateHandler.this.context.getName()+": State machine throw validation exception processing provisioning for "+stateinput.getKey()+" in atom "+pendingAtom.getHash(), vex);
			StateHandler.this.context.getEvents().post(new AtomExecutedEvent(pendingAtom, vex));
		}
	}
	
	void execute(final PendingAtom pendingAtom) throws ValidationException, IOException
	{
		pendingAtom.execute();
		if (pendingAtom.getStatus().equals(CommitStatus.EXECUTED))
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
					PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(((StateCertificate)stateCertificate).getAtom());
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
			public Collection<Hash> required(Class<? extends Primitive> type, Collection<Hash> items) throws IOException
			{
				if (type.equals(StateCertificate.class) == false)
				{
					stateLog.error(StateHandler.this.context.getName()+": State certificate type expected but got "+type);
					return Collections.emptyList();
				}
					
				StateHandler.this.lock.readLock().lock();
				try
				{
					Set<Hash> required = new HashSet<Hash>();
					for (Hash item : items)
					{
						if (StateHandler.this.certificatesToProcessQueue.contains(item) == true || 
							StateHandler.this.certificatesToProcessDelayed.containsKey(item) == true ||
							StateHandler.this.context.getLedger().getLedgerStore().has(item) == true)
							continue;
					
						if (stateLog.hasLevel(Logging.DEBUG) == true)
							stateLog.debug(StateHandler.this.context.getName()+": Added request for state certificate "+item);
						
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
					stateLog.error(StateHandler.this.context.getName()+": Received state certificate "+stateCertificate.getState()+" for local shard");
					// 	Disconnected and ban
					return;
				}

				PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(stateCertificate.getAtom());
				if (pendingAtom != null && pendingAtom.getStatus().greaterThan(CommitStatus.PREPARED) == true)
					StateHandler.this.certificatesToProcessQueue.put(stateCertificate.getHash(), stateCertificate);
				else
					StateHandler.this.certificatesToProcessDelayed.put(stateCertificate.getHash(), stateCertificate);
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
						StateCertificate stateCertificate = StateHandler.this.certificatesToProcessQueue.get(item);
						if (stateCertificate == null)
							stateCertificate = StateHandler.this.certificatesToProcessDelayed.get(item);
						if (stateCertificate == null)
							stateCertificate = getCertificate(item, StateCertificate.class);
						
						if (stateCertificate == null)
						{
							stateLog.error(StateHandler.this.context.getName()+": Requested state certificate "+item+" not found");
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
		
		// STATE INPUT GOSSIP //
		this.context.getNetwork().getGossipHandler().register(StateInput.class, new GossipFilter(this.context) 
		{
			@Override
			public Set<Long> filter(Primitive stateInput) throws IOException
			{
				// TODO this may be redundant as presenting the broadcast with the shard groups included.
				//		reason being that if the broadcast is latent, the pending atom is gone from all sources due to a commit accept/reject
				StateHandler.this.lock.readLock().lock();
				try
				{
					PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(((StateInput)stateInput).getAtom());
					if (pendingAtom == null)
					{
						cerbyLog.error(StateHandler.this.context.getName()+": Pending atom "+((StateInput)stateInput).getAtom()+" not found for inventory filter");
						return Collections.emptySet();
					}
					
					long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()));
					long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
					long stateShardGroup = ShardMapper.toShardGroup(((StateInput)stateInput).getKey().get(), numShardGroups);
					Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
					if (stateShardGroup == localShardGroup)
						shardGroups.remove(stateShardGroup);
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

		this.context.getNetwork().getGossipHandler().register(StateInput.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(Class<? extends Primitive> type, Collection<Hash> items) throws IOException
			{
				if (type.equals(StateInput.class) == false)
				{
					stateLog.error(StateHandler.this.context.getName()+": State input type expected but got "+type);
					return Collections.emptyList();
				}
					
				StateHandler.this.lock.readLock().lock();
				try
				{
					Set<Hash> required = new HashSet<Hash>();
					for (Hash item : items)
					{
						if (StateHandler.this.remoteProvisionQueue.contains(item) == true ||
							StateHandler.this.remoteProvisionDelayed.containsKey(item) == true ||
							StateHandler.this.context.getLedger().getLedgerStore().has(item) == true)
							continue;
					
						if (stateLog.hasLevel(Logging.DEBUG) == true)
							stateLog.debug(StateHandler.this.context.getName()+": Added request for state input "+item);
						
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

		this.context.getNetwork().getGossipHandler().register(StateInput.class, new GossipReceiver() 
		{
			@Override
			public void receive(Primitive object) throws IOException, ValidationException, CryptoException
			{
				StateInput stateInput = (StateInput)object;
				long numShardGroups = StateHandler.this.context.getLedger().numShardGroups();
				long stateShardGroup = ShardMapper.toShardGroup(stateInput.getKey().get(), numShardGroups);
				long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
				if (stateShardGroup == localShardGroup)
				{
					if (stateLog.hasLevel(Logging.DEBUG) == true)
						stateLog.debug(StateHandler.this.context.getName()+": Received state input "+stateInput.getKey()+" for local shard (possible consequence of gossip");
					// 	Disconnected and ban
					return;
				}

				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(StateHandler.this.context.getName()+": Received state input "+stateInput.getKey()+":"+stateInput.getValue()+" for atom "+stateInput.getAtom());

				PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(stateInput.getAtom());
				if (pendingAtom != null && pendingAtom.getStatus().greaterThan(CommitStatus.PREPARED) == true)
					StateHandler.this.remoteProvisionQueue.put(stateInput.getHash(), stateInput);
				else
					StateHandler.this.remoteProvisionDelayed.put(stateInput.getHash(), stateInput);
			}
		});
					
		this.context.getNetwork().getGossipHandler().register(StateInput.class, new GossipFetcher() 
		{
			@Override
			public Collection<StateInput> fetch(Collection<Hash> items) throws IOException
			{
				StateHandler.this.lock.readLock().lock();
				try
				{
					Set<StateInput> fetched = new HashSet<StateInput>();
					for (Hash item : items)
					{
						StateInput stateInput = StateHandler.this.remoteProvisionQueue.get(item); 
						if (stateInput == null)
							stateInput = StateHandler.this.remoteProvisionDelayed.get(item);
						if (stateInput == null)
							stateInput = StateHandler.this.context.getLedger().getLedgerStore().get(item, StateInput.class);
						
						if (stateInput == null)
						{
							stateLog.error(StateHandler.this.context.getName()+": Requested state input "+item+" not found");
							continue;
						}
						
						fetched.add(stateInput);
					}
					return fetched;
				}
				finally
				{
					StateHandler.this.lock.readLock().unlock();
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
						final long numShardGroups = StateHandler.this.context.getLedger().numShardGroups(); // Can be final as should only receive SyncAcquiredMessages from validators in same group
						long localShardGroup = ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), numShardGroups);
						long remoteShardGroup = ShardMapper.toShardGroup(peer.getNode().getIdentity(), numShardGroups);

						if (remoteShardGroup != localShardGroup)
						{
							stateLog.error(StateHandler.this.context.getName()+": Received SyncAcquiredMessage from "+peer+" in shard group "+remoteShardGroup+" but local is "+localShardGroup);
							// Disconnect and ban?
							return;
						}

						StateHandler.this.lock.readLock().lock();
						try
						{
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StateHandler.this.context.getName()+": State pool (certificates) inventory request from "+peer);
							
							final Set<PendingAtom> pendingAtoms = new HashSet<PendingAtom>(StateHandler.this.context.getLedger().getAtomHandler().getAll());
							final Set<Hash> stateCertificateInventory = new LinkedHashSet<Hash>();
							final Set<Hash> stateInputInventory = new LinkedHashSet<Hash>();
							StateHandler.this.certificatesToProcessQueue.forEach((h, sc) -> stateCertificateInventory.add(h));
							StateHandler.this.certificatesToProcessDelayed.forEach((h, sc) -> stateCertificateInventory.add(h));
							StateHandler.this.remoteProvisionQueue.forEach((h, si) -> stateInputInventory.add(h));
							StateHandler.this.remoteProvisionDelayed.forEach((h, si) -> stateInputInventory.add(h));
							for (PendingAtom pendingAtom : pendingAtoms)
							{
								for (StateCertificate stateCertificate : pendingAtom.getCertificates())
								{
									remoteShardGroup = ShardMapper.toShardGroup(peer.getNode().getIdentity(), numShardGroups);
									long stateShardGroup = ShardMapper.toShardGroup(stateCertificate.getState().get(), numShardGroups);
									
									if (remoteShardGroup == stateShardGroup)
										// Handled by sending state votes from state pool 
										continue;
									
									stateCertificateInventory.add(stateCertificate.getHash());
								}
								
								for (StateKey<?, ?> stateKey : pendingAtom.getStateKeys())
								{
									remoteShardGroup = ShardMapper.toShardGroup(peer.getNode().getIdentity(), numShardGroups);
									long stateShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
									
									if (remoteShardGroup == stateShardGroup)
										continue;

									Optional<UInt256> value = pendingAtom.getInput(stateKey);
									if (value == null)
										continue;
									
									stateInputInventory.add(StateInput.getHash(pendingAtom.getHash(), stateKey));
								}
							}
							
							long height = StateHandler.this.context.getLedger().getHead().getHeight();
							while (height >= Math.max(0, syncAcquiredMessage.getHead().getHeight() -1))
							{
								// TODO optimise
								for (Hash stateCertificateHash : StateHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, StateCertificate.class, SyncInventoryType.COMMIT))
								{
									StateCertificate stateCertificate = StateHandler.this.context.getLedger().getLedgerStore().get(stateCertificateHash, StateCertificate.class);
									remoteShardGroup = ShardMapper.toShardGroup(peer.getNode().getIdentity(), numShardGroups);
									long stateShardGroup = ShardMapper.toShardGroup(stateCertificate.getState().get(), numShardGroups);
									
									if (remoteShardGroup == stateShardGroup)
										// Handled by sending state votes from state pool 
										continue;

									stateCertificateInventory.add(stateCertificate.getHash());
								}
								
								for (Hash stateInputHash : StateHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, StateInput.class, SyncInventoryType.COMMIT))
								{
									StateInput stateInput = StateHandler.this.context.getLedger().getLedgerStore().get(stateInputHash, StateInput.class);
									remoteShardGroup = ShardMapper.toShardGroup(peer.getNode().getIdentity(), numShardGroups);
									long stateShardGroup = ShardMapper.toShardGroup(stateInput.getKey().get(), numShardGroups);
									
									if (remoteShardGroup == stateShardGroup)
										// Handled by sending state votes from state pool 
										continue;

									stateInputInventory.add(stateInput.getHash());
								}
								height--;
							}
							
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StateHandler.this.context.getName()+": Broadcasting about "+stateCertificateInventory+" state certificates to "+peer);

							while(stateCertificateInventory.isEmpty() == false)
							{
								SyncInventoryMessage stateCertificateInventoryMessage = new SyncInventoryMessage(stateCertificateInventory, 0, Math.min(BroadcastInventoryMessage.MAX_ITEMS, stateCertificateInventory.size()), StateCertificate.class);
								StateHandler.this.context.getNetwork().getMessaging().send(stateCertificateInventoryMessage, peer);
								stateCertificateInventory.removeAll(stateCertificateInventoryMessage.getItems());
							}
							
							if (cerbyLog.hasLevel(Logging.DEBUG) == true)
								cerbyLog.debug(StateHandler.this.context.getName()+": Broadcasting about "+stateInputInventory+" state inputs to "+peer);

							while(stateInputInventory.isEmpty() == false)
							{
								SyncInventoryMessage stateInputInventoryMessage = new SyncInventoryMessage(stateInputInventory, 0, Math.min(BroadcastInventoryMessage.MAX_ITEMS, stateInputInventory.size()), StateInput.class);
								StateHandler.this.context.getNetwork().getMessaging().send(stateInputInventoryMessage, peer);
								stateInputInventory.removeAll(stateInputInventoryMessage.getItems());
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
	
	private CertificateStatus process(final StateCertificate certificate) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(certificate, "State certificate is null");
		
		StateHandler.this.lock.writeLock().lock();
		PendingAtom pendingAtom = null;
		try
		{
			pendingAtom = this.context.getLedger().getAtomHandler().get(certificate.getAtom());
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
				cerbyLog.debug(this.context.getName()+": Created atom certificate "+atomCertificate.getHash()+" for atom "+atomCertificate.getAtom()+" with decision "+atomCertificate.getDecision());

			this.context.getEvents().post(new AtomCertificateEvent(atomCertificate));
			return true;
		}
		
		return false;
	}
	
	void push(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom for injection is null");
		
		if (pendingAtom.getStatus().equals(CommitStatus.PROVISIONING) == false)
			throw new IllegalStateException(this.context.getName()+": Pending atom "+pendingAtom.getHash()+" for injection must be in PROVISIONING state");
		
		long numShardGroups = this.context.getLedger().numShardGroups(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()));
		long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		for (StateKey<?, ?> stateKey : pendingAtom.getStateKeys())
		{
			long stateShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
			if (stateShardGroup != localShardGroup)
				continue;
				
			synchronized(this.pendingStates)
			{
				PendingState pendingState = new PendingState(this.context, stateKey, pendingAtom.getHash(), pendingAtom.getBlock());
				if (this.pendingStates.containsKey(pendingState.getHash()) == true)
					throw new IllegalStateException(this.context.getName()+": Pending state "+pendingState.getKey()+" for atom "+pendingAtom.getHash()+" in block "+pendingAtom.getBlock()+" is already present");
	
				this.pendingStates.put(pendingState.getHash(), pendingState);
				this.context.getMetaData().increment("ledger.pool.state.added");
			}
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
			
			if (pendingAtom.getCertificate() != null)
				this.context.getLedger().getLedgerStore().storeSyncInventory(this.context.getLedger().getHead().getHeight(), pendingAtom.getCertificate().getHash(), AtomCertificate.class, SyncInventoryType.COMMIT);

			for (StateCertificate stateCertificate : pendingAtom.getCertificates())
				this.context.getLedger().getLedgerStore().storeSyncInventory(this.context.getLedger().getHead().getHeight(), stateCertificate.getHash(), StateCertificate.class, SyncInventoryType.COMMIT);
			
			if (pendingAtom.getStatus().greaterThan(CommitStatus.PREPARED) == true)
			{
				for (StateKey<?, ?> stateKey : pendingAtom.getStateKeys())
				{
					PendingState pendingState = get(pendingAtom.getBlock(), pendingAtom.getHash(), stateKey);
					if (pendingState == null)
						stateLog.warn(this.context.getName()+": Pending state "+stateKey+" not found for "+pendingAtom+" in block "+pendingAtom.getBlock());
					
					if (this.pendingStates.remove(pendingState.getHash(), pendingState) == true)
					{
						if(cerbyLog.hasLevel(Logging.DEBUG))
							cerbyLog.debug(this.context.getName()+": Removed pending state "+stateKey+" for "+pendingAtom+" in block "+pendingAtom.getBlock());

						// TODO these dont increment here because pending atom status is not set yet!
						if (pendingAtom.getStatus().equals(CommitStatus.COMPLETED) == true)
							this.context.getMetaData().increment("ledger.pool.state.committed");
						else if (pendingAtom.getStatus().equals(CommitStatus.ABORTED) == true)
							this.context.getMetaData().increment("ledger.pool.state.aborted");

						this.context.getMetaData().increment("ledger.pool.state.removed");
					}
					else
						cerbyLog.debug(this.context.getName()+": Removal failed for pending state "+stateKey+" for "+pendingAtom+" in block "+pendingAtom.getBlock());
					
					this.localProvisionQueue.remove(pendingState, pendingAtom);
					Optional<UInt256> value = pendingAtom.getInput(stateKey);
					if (value == null)
						continue;
					
					StateInput stateInput = new StateInput(pendingAtom.getHash(), stateKey, value.orElse(null));
					this.context.getLedger().getLedgerStore().storeSyncInventory(this.context.getLedger().getHead().getHeight(), stateInput.getHash(), StateInput.class, SyncInventoryType.COMMIT);
				}
			}

			final List<Hash> certificateRemovals = new ArrayList<Hash>();
			this.certificatesToProcessDelayed.forEach((h,sc) -> {
				if (pendingAtom.getHash().equals(sc.getAtom()) == true)
					certificateRemovals.add(h);
			});
			this.certificatesToProcessQueue.forEach((h,sc) -> {
				if (pendingAtom.getHash().equals(sc.getAtom()) == true)
					certificateRemovals.add(h);
			});
			this.certificatesToProcessQueue.removeAll(certificateRemovals);
			synchronized(this.certificatesToProcessDelayed) { this.certificatesToProcessDelayed.keySet().removeAll(certificateRemovals); }
			
			final List<Hash> inputRemovals = new ArrayList<Hash>();
			this.remoteProvisionDelayed.forEach((h,si) -> {
				if (pendingAtom.getHash().equals(si.getAtom()) == true)
					inputRemovals.add(h);
			});
			this.remoteProvisionQueue.forEach((h,si) -> {
				if (pendingAtom.getHash().equals(si.getAtom()) == true)
					inputRemovals.add(h);
			});
			this.remoteProvisionQueue.removeAll(inputRemovals);
			synchronized(this.remoteProvisionDelayed) { this.remoteProvisionDelayed.keySet().removeAll(inputRemovals); }
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
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(StateHandler.this.context.getName()+": State certificate "+stateCertificateEvent.getCertificate().getState()+" from local");
			
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
				if (StateHandler.this.context.getLedger().getAtomHandler().get(atomCertificateEvent.getCertificate().getAtom()) == null)
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
		public void on(final AtomPositiveCommitEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomNegativeCommitEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomExceptionEvent event) throws IOException 
		{
			if (event.getException() instanceof StateLockedException)
				return;
				
			remove(event.getPendingAtom());
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
				// Provision accepted atoms
				try
				{
					for (Atom atom : blockCommittedEvent.getBlock().getAtoms())
					{
						PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(atom.getHash());
						if (pendingAtom == null)
							throw new IllegalStateException(StateHandler.this.context.getName()+": Pending atom "+atom+" committed in block "+blockCommittedEvent.getBlock().getHash()+" not found");
						
	            		if (cerbyLog.hasLevel(Logging.DEBUG))
							cerbyLog.debug(StateHandler.this.context.getName()+": Queuing pending atom "+pendingAtom.getHash()+" for provisioning");
	            		
	        			pendingAtom.provision(blockCommittedEvent.getBlock().getHeader());
						provision(pendingAtom, pendingAtom.getStateKeys());
					}
				}
				catch (Exception ex)
				{
					cerbyLog.fatal(StateHandler.class.getName()+": Failed to provision pending atom set for "+blockCommittedEvent.getBlock().getHeader()+" when processing async BlockCommittedEvent", ex);
				}

				// Commit atom states
				for (AtomCertificate certificate : blockCommittedEvent.getBlock().getCertificates())
				{
					try
					{
						if (cerbyLog.hasLevel(Logging.DEBUG) == true)
							cerbyLog.debug(StateHandler.this.context.getName()+": Committing atom certificate "+certificate.getHash()+" for "+certificate.getAtom());
						
						PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().get(certificate.getAtom());
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
					}
					catch (Exception ex)
					{
						// FIXME don't like how this is thrown in an event listener.
						// 		 should be able to send the commit without having to fetch the atom, state processors *should* have it by the time
						//		 we're sending commit certificates to them.
						cerbyLog.fatal(StateHandler.this.context.getName()+": Failed to process BlockCommittedEvent on certificate "+certificate.getHash()+" for "+certificate.getAtom(), ex);
					}
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
							Collection<Hash> items = StateHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, AtomCertificate.class, SyncInventoryType.SEEN);
							for (Hash item : items)
							{
								AtomCertificate atomCertificate = StateHandler.this.context.getLedger().getLedgerStore().get(item, AtomCertificate.class);
								PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().load(atomCertificate.getAtom());
								if (pendingAtom == null)
									continue;
								
								pendingAtom.setCertificate(atomCertificate);
								atomCertificate.getAll().forEach(sc -> pendingAtom.addCertificate(sc));
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
							Collection<Hash> items = StateHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, StateCertificate.class, SyncInventoryType.SEEN);
							for (Hash item : items)
							{
								StateCertificate stateCertificate = StateHandler.this.context.getLedger().getLedgerStore().get(item, StateCertificate.class);
								PendingAtom pendingAtom = StateHandler.this.context.getLedger().getAtomHandler().load(stateCertificate.getAtom());
								if (pendingAtom == null)
									continue;
									
								CertificateStatus status = process(stateCertificate);
								if (status == CertificateStatus.SKIPPED)
								{
									if (stateLog.hasLevel(Logging.DEBUG) == true)
										stateLog.debug(StateHandler.this.context.getName()+": Syncing of state certificate "+stateCertificate.getHash()+" was skipped for atom "+stateCertificate.getAtom()+" in block "+stateCertificate.getBlock());
								}
								else if (status == CertificateStatus.FAILED)
									stateLog.warn(StateHandler.this.context.getName()+": Syncing of state certificate "+stateCertificate.getHash()+" failed for atom "+stateCertificate.getAtom()+" in block "+stateCertificate.getBlock());
								else if (stateLog.hasLevel(Logging.DEBUG) == true)
									stateLog.debug(StateHandler.this.context.getName()+": Synced state certificate "+stateCertificate.getHash()+" for atom "+stateCertificate.getAtom()+" in block "+stateCertificate.getBlock());
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
					cerbyLog.info(StateHandler.this.context.getName()+": Sync status changed to "+event.isSynced()+", flushing state handler");
					StateHandler.this.executionQueue.clear();
					StateHandler.this.localProvisionQueue.clear();
					StateHandler.this.remoteProvisionQueue.clear();
					StateHandler.this.remoteProvisionDelayed.clear();
					StateHandler.this.pendingStates.clear();
					StateHandler.this.certificatesToProcessQueue.clear();
					StateHandler.this.certificatesToProcessDelayed.clear();
				}
			}
			finally
			{
				StateHandler.this.lock.writeLock().unlock();
			}
		}
	};
}
