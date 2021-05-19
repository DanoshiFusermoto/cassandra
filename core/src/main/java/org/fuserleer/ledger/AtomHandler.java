package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.Universe;
import org.fuserleer.collections.MappedBlockingQueue;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.AtomPool.AtomVoteStatus;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.LedgerStore.SyncInventoryType;
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.atoms.AutomataExtension;
import org.fuserleer.ledger.events.AtomPositiveCommitEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomAcceptedEvent;
import org.fuserleer.ledger.events.AtomAcceptedTimeoutEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomPersistedEvent;
import org.fuserleer.ledger.events.AtomNegativeCommitEvent;
import org.fuserleer.ledger.events.AtomUnpreparedTimeoutEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
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
import org.fuserleer.time.Time;

import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public class AtomHandler implements Service
{
	private static final Logger atomsLog = Logging.getLogger("atoms");
	
	static
	{
//		atomsLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
		atomsLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
//		atomsLog.setLevels(Logging.ERROR | Logging.FATAL);
	}

	private final Context context;
	
	private final Map<Hash, PendingAtom> pendingAtoms = Collections.synchronizedMap(new HashMap<Hash, PendingAtom>());
	private final Map<Hash, Long> timedout = Collections.synchronizedMap(new HashMap<Hash, Long>());
	private final MappedBlockingQueue<Hash, Atom> atomQueue;
	private final MappedBlockingQueue<Hash, AutomataExtension> automataExtQueue;

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

	private Executable atomProcessor = new Executable()
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
						if (AtomHandler.this.context.getNode().isSynced() == false)
						{
							Thread.sleep(1000);
							continue;
						}

						Entry<Hash, Atom> atom = AtomHandler.this.atomQueue.peek(1, TimeUnit.SECONDS);
						if (atom != null)
						{
							if (atomsLog.hasLevel(Logging.DEBUG))
								atomsLog.debug(AtomHandler.this.context.getName()+": Verifying atom "+atom.getValue().getHash());

							// FIXME some quirky flow logic here to avoid a dead lock
							final PendingAtom pendingAtom;
							final Collection<Long> shardGroups;
							final long numShardGroups = AtomHandler.this.context.getLedger().numShardGroups(AtomHandler.this.context.getLedger().getHead().getHeight());
							final long localShardGroup = ShardMapper.toShardGroup(AtomHandler.this.context.getNode().getIdentity(), numShardGroups);
							boolean hasAutomata = false;
							AtomHandler.this.lock.writeLock().lock();
							try
							{
								pendingAtom = AtomHandler.this.pendingAtoms.computeIfAbsent(atom.getKey(), (k) -> PendingAtom.create(AtomHandler.this.context, atom.getValue()));
								try
								{
									if (pendingAtom.getStatus().equals(CommitStatus.NONE) == false)
									{
										atomsLog.warn(AtomHandler.this.context.getName()+": Atom "+atom.getValue().getHash()+" is already pending with state "+pendingAtom.getStatus());
										continue;
									}
									
									if (pendingAtom.getAtom() == null)
										pendingAtom.setAtom(atom.getValue());

									// Store all valid atoms even if they aren't within the local shard group.
									// Those atoms will be broadcast the the relevant groups and it needs to be stored
									// to be able to serve the requests for it.  Such atoms can be pruned per epoch
									if (pendingAtom.getStatus().equals(CommitStatus.NONE))
										AtomHandler.this.context.getLedger().getLedgerStore().store(AtomHandler.this.context.getLedger().getHead().getHeight(), pendingAtom.getAtom());  // TODO handle failure
	
									// FIXME quick hack to test automata
									hasAutomata = !pendingAtom.getAtom().getAutomata().isEmpty();
									if (hasAutomata)
										pendingAtom.getAtom().clearAutomata();
									
									pendingAtom.prepare();
									shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
									if (hasAutomata)
										shardGroups.add(localShardGroup);
									
									if (shardGroups.contains(localShardGroup) == false)
										AtomHandler.this.pendingAtoms.remove(pendingAtom.getHash(), pendingAtom);
								}
								catch (Exception ex)
								{
									atomsLog.error(AtomHandler.this.context.getName()+": Error processing for atom for " + atom.getValue().getHash(), ex);
									AtomHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
									continue;
								}
							}
							finally
							{
			                	AtomHandler.this.atomQueue.remove(atom.getKey());
			                	AtomHandler.this.lock.writeLock().unlock();
							}
							
		                	if (hasAutomata == true || shardGroups.contains(localShardGroup) == true)
		                		AtomHandler.this.context.getEvents().post(new AtomPersistedEvent(pendingAtom));

		                	AtomHandler.this.context.getNetwork().getGossipHandler().broadcast(pendingAtom.getAtom(), shardGroups);
						}
					} 
					catch (InterruptedException e) 
					{
						// DO NOTHING //
						continue;
					}

					Entry<Hash, AutomataExtension> automataExt = AtomHandler.this.automataExtQueue.peek();
					if (automataExt != null)
					{
						if (atomsLog.hasLevel(Logging.DEBUG))
							atomsLog.debug(AtomHandler.this.context.getName()+": Verifying automata extension "+automataExt.getValue().getHash()+" for atom "+automataExt.getValue().getAtom().getHash());

						final PendingAtom pendingAtom;
						final Collection<Long> shardGroups;
						final long numShardGroups = AtomHandler.this.context.getLedger().numShardGroups(AtomHandler.this.context.getLedger().getHead().getHeight());
						final long localShardGroup = ShardMapper.toShardGroup(AtomHandler.this.context.getNode().getIdentity(), numShardGroups);
						AtomHandler.this.lock.writeLock().lock();
						try
						{
							OperationStatus status = AtomHandler.this.context.getLedger().getLedgerStore().store(AtomHandler.this.context.getLedger().getHead().getHeight(), automataExt.getValue());
							if (status.equals(OperationStatus.SUCCESS) == false)
								atomsLog.error(AtomHandler.this.context.getName()+": Error storing automata extension for atom "+automataExt.getValue().getAtom().getHash()+" due to "+status);

							pendingAtom = AtomHandler.this.pendingAtoms.computeIfAbsent(automataExt.getValue().getAtom().getHash(), (k) -> PendingAtom.create(AtomHandler.this.context, automataExt.getValue().getAtom()));
							try
							{
								if (pendingAtom.getStatus().equals(CommitStatus.NONE) == false)
								{
									atomsLog.warn(AtomHandler.this.context.getName()+": Automata ext atom "+automataExt.getValue().getHash()+" is already pending with state "+pendingAtom.getStatus());
									continue;
								}
								
								if (pendingAtom.getAtom() == null)
									pendingAtom.setAtom(automataExt.getValue().getAtom());
								
								shardGroups = ShardMapper.toShardGroups(automataExt.getValue().getShards(), numShardGroups);
								if (shardGroups.contains(localShardGroup) == false)
								{
									AtomHandler.this.pendingAtoms.remove(pendingAtom.getHash(), pendingAtom);
									atomsLog.error(AtomHandler.this.context.getName()+": Automata ext for atom "+automataExt.getValue().getHash()+" does not intersect with local shard group "+localShardGroup);
									continue;
								}

								if (pendingAtom.getStatus().equals(CommitStatus.NONE))
									AtomHandler.this.context.getLedger().getLedgerStore().store(AtomHandler.this.context.getLedger().getHead().getHeight(), pendingAtom.getAtom());  // TODO handle failure

								pendingAtom.prepare(automataExt.getValue());
								shardGroups.addAll(ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups));
							}
							catch (Exception ex)
							{
								atomsLog.error(AtomHandler.this.context.getName()+": Error processing automata atom extension for "+automataExt.getValue().getHash(), ex);
								AtomHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
								continue;
							}
						}
						finally
						{
		                	AtomHandler.this.automataExtQueue.remove(automataExt.getKey());
		                	AtomHandler.this.lock.writeLock().unlock();
						}

						AtomHandler.this.context.getEvents().post(new AtomPersistedEvent(pendingAtom));
	                	AtomHandler.this.context.getNetwork().getGossipHandler().broadcast(automataExt.getValue(), Collections.singleton(localShardGroup));
	                	
	                	// TODO temporary, should be pushed into queue over in AtomPool
	                	for (AtomVote atomVote : automataExt.getValue().getVotes())
	                	{
							AtomVoteStatus status = AtomHandler.this.context.getLedger().getAtomPool().process(atomVote); 
							if (status.equals(AtomVoteStatus.SUCCESS) == true)
							{
								if (atomsLog.hasLevel(Logging.DEBUG) == true)
									atomsLog.debug(AtomHandler.this.context.getName()+":  Processed automata extension atom vote "+atomVote.getHash()+" for atom "+atomVote.getAtom()+" by "+atomVote.getOwner());
							}
							else if (status.equals(AtomVoteStatus.SKIPPED) == true)
							{
								if (atomsLog.hasLevel(Logging.DEBUG) == true)
									atomsLog.debug(AtomHandler.this.context.getName()+":  Processing of automata extension atom vote "+atomVote.getHash()+" was skipped for atom "+atomVote.getAtom()+" by "+atomVote.getOwner());
							}
							else
								atomsLog.warn(AtomHandler.this.context.getName()+": Processing of automata extension atom vote "+atomVote.getHash()+" failed for atom "+atomVote.getAtom()+" by "+atomVote.getOwner());
	                	}
	                	
	                	// TODO temporary, should be processed in StateHandler
	                	for (StateInput stateInput : automataExt.getValue().getInputs())
	                	{
				    		if (AtomHandler.this.context.getLedger().getLedgerStore().store(AtomHandler.this.context.getLedger().getHead().getHeight(), stateInput).equals(OperationStatus.SUCCESS) == false)
								atomsLog.warn(AtomHandler.this.context.getName()+": Already stored automata extension state input "+stateInput.getKey()+" for atom "+pendingAtom.getHash()+" in block "+pendingAtom.getBlock());
	                	}
					}
				}
			}
			catch (Throwable throwable)
			{
				// TODO want to actually handle this?
				atomsLog.fatal(AtomHandler.this.context.getName()+": Error processing atom queue", throwable);
			}
		}
	};
	
	AtomHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context);
		this.atomQueue = new MappedBlockingQueue<Hash, Atom>(this.context.getConfiguration().get("ledger.atom.queue", 1<<16));
		this.automataExtQueue = new MappedBlockingQueue<Hash, AutomataExtension>(this.context.getConfiguration().get("ledger.atom.automata.queue", 1<<10));
	}

	@Override
	public void start() throws StartupException 
	{
		// ATOMS //
		this.context.getNetwork().getGossipHandler().register(Atom.class, new GossipFilter(this.context) 
		{
			@Override
			public Set<Long> filter(Primitive atom) throws IOException
			{
				PendingAtom pendingAtom = AtomHandler.this.get(atom.getHash());
				return ShardMapper.toShardGroups(pendingAtom.getShards(), AtomHandler.this.context.getLedger().numShardGroups(AtomHandler.this.context.getLedger().getHead().getHeight()));
			}
		});
		
		this.context.getNetwork().getGossipHandler().register(Atom.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(final Class<? extends Primitive> type, final Collection<Hash> items) throws IOException
			{
				Set<Hash> required = new HashSet<Hash>();
				for (Hash item : items)
				{
					PendingAtom pendingAtom = AtomHandler.this.pendingAtoms.get(item);
					if (pendingAtom != null && pendingAtom.getAtom() != null)
						continue;
					
					if (AtomHandler.this.atomQueue.contains(item) == true ||
						AtomHandler.this.context.getLedger().getLedgerStore().has(item) == true)
						continue;
				
					required.add(item);
				}
				return required;
			}
		});

		this.context.getNetwork().getGossipHandler().register(Atom.class, new GossipReceiver() 
		{
			@Override
			public void receive(final Primitive object) throws InterruptedException
			{
				AtomHandler.this.submit((Atom) object);
			}
		});

		this.context.getNetwork().getGossipHandler().register(Atom.class, new GossipFetcher() 
		{
			@Override
			public Collection<Atom> fetch(final Collection<Hash> items) throws IOException
			{
				Set<Atom> fetched = new HashSet<Atom>();
				for (Hash item : items)
				{
					Atom atom = null;
					if (AtomHandler.this.pendingAtoms.containsKey(item) == true)
						atom = AtomHandler.this.pendingAtoms.get(item).getAtom();
					if (atom == null)
						atom = AtomHandler.this.atomQueue.get(item);
					if (atom == null)
						atom = AtomHandler.this.context.getLedger().getLedgerStore().get(item, Atom.class);

					if (atom == null)
					{
						if (atomsLog.hasLevel(Logging.DEBUG) == true)
							atomsLog.debug(AtomHandler.this.context.getName()+": Requested atom "+item+" not found");
						continue;
					}
					
					fetched.add(atom);
				}
				return fetched;
			}
		});
		
		// AUTOMATA EXTENSIONS //
		this.context.getNetwork().getGossipHandler().register(AutomataExtension.class, new GossipFilter(this.context) 
		{
			@Override
			public Set<Long> filter(Primitive object) throws IOException
			{
				AutomataExtension automataExt = (AutomataExtension)object;
				return ShardMapper.toShardGroups(automataExt.getShards(), AtomHandler.this.context.getLedger().numShardGroups(AtomHandler.this.context.getLedger().getHead().getHeight()));
			}
		});

		this.context.getNetwork().getGossipHandler().register(AutomataExtension.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(final Class<? extends Primitive> type, final Collection<Hash> items) throws IOException
			{
				Set<Hash> required = new HashSet<Hash>();
				for (Hash item : items)
				{
					if (AtomHandler.this.context.getLedger().getLedgerStore().has(item) == true)
						continue;
				
					required.add(item);
				}
				return required;
			}
		});

		this.context.getNetwork().getGossipHandler().register(AutomataExtension.class, new GossipReceiver() 
		{
			@Override
			public void receive(final Primitive object) throws InterruptedException
			{
				AtomHandler.this.automataExtQueue.putIfAbsent(object.getHash(), (AutomataExtension)object);
			}
		});

		this.context.getNetwork().getGossipHandler().register(AutomataExtension.class, new GossipFetcher() 
		{
			@Override
			public Collection<AutomataExtension> fetch(final Collection<Hash> items) throws IOException
			{
				Set<AutomataExtension> fetched = new HashSet<AutomataExtension>();
				for (Hash item : items)
				{
					AutomataExtension automataExt = AtomHandler.this.automataExtQueue.get(item); 
					if (automataExt == null)
						automataExt = AtomHandler.this.context.getLedger().getLedgerStore().get(item, AutomataExtension.class);

					if (automataExt == null)
					{
						if (atomsLog.hasLevel(Logging.DEBUG) == true)
							atomsLog.debug(AtomHandler.this.context.getName()+": Requested automata extension "+item+" not found");
						continue;
					}
					
					fetched.add(automataExt);
				}
				return fetched;
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
						AtomHandler.this.lock.readLock().lock();
						try
						{
							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomHandler.this.context.getName()+": Atom pool inventory request from "+peer);

							final long numShardGroups = AtomHandler.this.context.getLedger().numShardGroups(AtomHandler.this.context.getLedger().getHead().getHeight());
							final long localShardGroup = ShardMapper.toShardGroup(AtomHandler.this.context.getNode().getIdentity(), numShardGroups);

							// TODO will cause problems when pool is BIG
							Set<PendingAtom> pendingAtoms = new HashSet<PendingAtom>(AtomHandler.this.pendingAtoms.values());
							final Set<Hash> pendingAtomInventory = new LinkedHashSet<Hash>();
							final Set<Hash> automataExtensionInventory = new LinkedHashSet<Hash>();
							
							for (PendingAtom pendingAtom : pendingAtoms)
							{
								if (pendingAtom.getStatus().lessThan(CommitStatus.PREPARED) == true)
									continue;
								
								if (pendingAtom.getAutomataExtension() != null)
									continue;

								Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
			                	if (shardGroups.contains(localShardGroup) == false)
			                		continue;

			                	pendingAtomInventory.add(pendingAtom.getHash());
							}
							
							for (PendingAtom pendingAtom : pendingAtoms)
							{
								if (pendingAtom.getStatus().lessThan(CommitStatus.PREPARED) == true)
									continue;
								
								if (pendingAtom.getAutomataExtension() == null)
									continue;
								
			                	Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getAutomataExtension().getShards(), numShardGroups);
			                	if (shardGroups.contains(localShardGroup) == false)
			                		continue;

			                	automataExtensionInventory.add(pendingAtom.getHash());
							}

							long height = AtomHandler.this.context.getLedger().getHead().getHeight();
							while (height >= Math.max(0, syncAcquiredMessage.getHead().getHeight() - 1))
							{
								pendingAtomInventory.addAll(AtomHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, Atom.class, SyncInventoryType.COMMIT));
								height--;
							}
							
							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomHandler.this.context.getName()+": Broadcasting "+pendingAtomInventory.size()+" / "+pendingAtomInventory+" pool atoms to "+peer);

							while(pendingAtomInventory.isEmpty() == false)
							{
								SyncInventoryMessage pendingAtomInventoryMessage = new SyncInventoryMessage(pendingAtomInventory, 0, Math.min(SyncInventoryMessage.MAX_ITEMS, pendingAtomInventory.size()), Atom.class);
								AtomHandler.this.context.getNetwork().getMessaging().send(pendingAtomInventoryMessage, peer);
								pendingAtomInventory.removeAll(pendingAtomInventoryMessage.getItems());
							}

							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomHandler.this.context.getName()+": Broadcasting "+automataExtensionInventory.size()+" / "+automataExtensionInventory+" atom automata extensions to "+peer);

							while(automataExtensionInventory.isEmpty() == false)
							{
								SyncInventoryMessage automataExtensionInventoryMessage = new SyncInventoryMessage(automataExtensionInventory, 0, Math.min(SyncInventoryMessage.MAX_ITEMS, automataExtensionInventory.size()), AutomataExtension.class);
								AtomHandler.this.context.getNetwork().getMessaging().send(automataExtensionInventoryMessage, peer);
								automataExtensionInventory.removeAll(automataExtensionInventoryMessage.getItems());
							}
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomHandler.this.context.getName()+": ledger.messages.atom.get.pool " + peer, ex);
						}
						finally
						{
							AtomHandler.this.lock.readLock().unlock();
						}
					}
				});
			}
		});

		this.context.getEvents().register(this.syncChangeListener);
		this.context.getEvents().register(this.syncAtomListener);
		this.context.getEvents().register(this.syncBlockListener);

		Thread atomProcessorThread = new Thread(this.atomProcessor);
		atomProcessorThread.setDaemon(true);
		atomProcessorThread.setName(this.context.getName()+" Atom Processor");
		atomProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException 
	{
		this.atomProcessor.terminate(true);

		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	public Collection<PendingAtom> timedout()
	{
		AtomHandler.this.lock.readLock().lock();
		try
		{
			final List<PendingAtom> timedout = new ArrayList<PendingAtom>();
			this.lock.readLock().lock();
			try
			{
				this.timedout.keySet().forEach(h -> {
					PendingAtom pendingAtom = this.pendingAtoms.get(h);
					if (pendingAtom == null)
						return;
					
					timedout.add(pendingAtom);
				});
				
				return timedout;
			}
			finally
			{
				this.lock.readLock().unlock();
			}
		}
		finally
		{
			AtomHandler.this.lock.readLock().unlock();
		}
	}
	
	public List<PendingAtom> timedout(final int limit, final Collection<Hash> exclusions)
	{
		final List<PendingAtom> timedout = new ArrayList<PendingAtom>();
		this.lock.readLock().lock();
		try
		{
			for (Hash timeout : this.timedout.keySet())
			{
				if (exclusions.contains(timeout) == true)
					continue;
				
				if (this.pendingAtoms.containsKey(timeout) == false)
					continue;
				
				timedout.add(this.pendingAtoms.get(timeout));
				if (timedout.size() == limit)
					break;
			}
		}
		finally
		{
			this.lock.readLock().unlock();
		}
		
		return timedout;
	}

	/**
	 * Returns all pending atoms.
	 * 
	 * @throws  
	 */
	public Collection<PendingAtom> getAll()
	{
		// Dont use the atom handler lock here.  
		// Just sync on the pending atoms object as this function is called from many places
		synchronized(this.pendingAtoms)
		{
			return new ArrayList<PendingAtom>(this.pendingAtoms.values());
		}
	}

	/**
	 * Returns an existing pending atom or creates it providing that it is not timed out or committed.
	 * 
	 * @param atom The atom hash
	 * @throws IOException
	 * @throws  
	 */
	PendingAtom get(final Hash atom) throws IOException
	{
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is zero");

		// TODO bit hacky ... quick fix to prevent genesis atoms being loaded into pending atom flow on startup 
		if (Universe.getDefault().getGenesis().contains(atom) == true)
			return null;

		// TODO dont think this is needed now
		if (this.context.getNode().isSynced() == false)
			throw new IllegalStateException("Sync state is false!  AtomHandler::get called");

		// Dont use the atom handler lock here.  
		// Just sync on the pending atoms object as this function is called from many places
		synchronized(this.pendingAtoms)
		{
			PendingAtom pendingAtom = this.pendingAtoms.get(atom);
			if (pendingAtom != null)
				return pendingAtom;
			
			final BlockHeader persistedBlock;
			final Atom persistedAtom;
			Commit commit = this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, atom));
			if (commit != null)
			{
				if (commit.getPath().get(Elements.CERTIFICATE) != null)
					return null;
					
				if (commit.isCommitTimedout() == true)
					return null;
				
				if (commit.isAcceptTimedout() == true)
					return null;

//				if (Time.getSystemTime() - PendingAtom.ATOM_INCLUSION_TIMEOUT_CLOCK_SECONDS > commit.getTimestamp())
//					return null;
					
				persistedAtom = this.context.getLedger().getLedgerStore().get(atom, Atom.class);
				if (persistedAtom == null)
					throw new IllegalStateException("Expected to find persisted atom "+atom);

				if (commit.getPath().get(Elements.BLOCK) != null)
				{
					persistedBlock = this.context.getLedger().get(commit.getPath().get(Elements.BLOCK), BlockHeader.class);
					if (persistedBlock == null)
						throw new IllegalStateException("Expected to find block "+commit.getPath().get(Elements.BLOCK)+" containing atom "+atom);
				}
				else
					persistedBlock = null;
			}
			else
			{
				persistedBlock = null;
				persistedAtom = null;
			}

			pendingAtom = PendingAtom.create(this.context, atom);
			if (persistedAtom != null)
			{
				pendingAtom.setAtom(persistedAtom);
				try
				{
					pendingAtom.prepare();
					if (persistedBlock != null)
					{
						pendingAtom.accepted();
						pendingAtom.provision(persistedBlock);
					}
					
				}
				catch (ValidationException vex)
				{
					throw new IOException("Loading of persisted atom "+atom+" failed", vex);
				}
			}
			
			this.pendingAtoms.put(atom, pendingAtom);
			
			if (atomsLog.hasLevel(Logging.DEBUG) == true)
				atomsLog.debug(this.context.getName()+": Pending atom "+atom+" creation stack", new Exception());
			
			return pendingAtom;
		}
	}
	
	/**
	 * Returns an existing pending atom or creates it providing that it is not committed.
	 * 
	 * @param atom The atom hash
	 * @throws IOException
	 * @throws  
	 */
	PendingAtom load(final Hash atom) throws IOException
	{
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is zero");
		
		// TODO bit hacky ... quick fix to prevent genesis atoms being loaded into pending atom flow on startup 
		if (Universe.getDefault().getGenesis().contains(atom) == true)
			return null;

		// TODO dont think this is needed now
		if (this.context.getNode().isSynced() == false)
			throw new IllegalStateException("Sync state is false!  AtomHandler::get called");
		
		// Dont use the atom handler lock here.  
		// Just sync on the pending atoms object as this function is called from many places
		synchronized(this.pendingAtoms)
		{
			PendingAtom pendingAtom = this.pendingAtoms.get(atom);
			if (pendingAtom != null)
				return pendingAtom;
			
			final BlockHeader persistedBlock;
			final Atom persistedAtom;
			Commit commit = this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, atom));
			if (commit != null)
			{
				if (commit.getPath().get(Elements.CERTIFICATE) != null)
					return null;
					
				persistedAtom = this.context.getLedger().getLedgerStore().get(atom, Atom.class);
				if (persistedAtom == null)
					throw new IllegalStateException("Expected to find persisted atom "+atom);

				if (commit.getPath().get(Elements.BLOCK) != null)
				{
					persistedBlock = this.context.getLedger().getLedgerStore().get(commit.getPath().get(Elements.BLOCK), BlockHeader.class);
					if (persistedBlock == null)
						throw new IllegalStateException("Expected to find block "+commit.getPath().get(Elements.BLOCK)+" containing atom "+atom);
				}
				else
					persistedBlock = null;
			}
			else
			{
				persistedBlock = null;
				persistedAtom = null;
			}

			if (commit != null)
				pendingAtom = new PendingAtom(this.context, atom, commit.getTimestamp());
			else
				pendingAtom = PendingAtom.create(this.context, atom);
			
			if (persistedAtom != null)
			{
				pendingAtom.setAtom(persistedAtom);
				try
				{
					pendingAtom.prepare();
					if (persistedBlock != null)
					{
						pendingAtom.accepted();
						pendingAtom.provision(persistedBlock);
					}
					
				}
				catch (ValidationException vex)
				{
					throw new IOException("Loading of persisted atom "+atom+" failed", vex);
				}
			}
			
			this.pendingAtoms.put(atom, pendingAtom);
			
			if (atomsLog.hasLevel(Logging.DEBUG) == true)
				atomsLog.debug(this.context.getName()+": Pending atom "+atom+" creation stack", new Exception());

			return pendingAtom;
		}
	}

	
	void push(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom for injection is null");
		
		if (pendingAtom.getStatus().equals(CommitStatus.PROVISIONING) == false)
			throw new IllegalStateException(this.context.getName()+": Pending atom "+pendingAtom.getHash()+" for injection must be in PROVISIONING state");
		
		// Dont use the atom handler lock here.  
		// Just sync on the pending atoms object as this function is called from many places
		synchronized(this.pendingAtoms)
		{
			if (this.pendingAtoms.containsKey(pendingAtom.getHash()) == true)
				throw new IllegalStateException(this.context.getName()+": Pending atom "+pendingAtom.getHash()+" is already present");
			
			this.pendingAtoms.put(pendingAtom.getHash(), pendingAtom);
		}
	}

	void remove(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom for injection is null");
		
		// Dont use the atom handler lock here.  
		// Just sync on the pending atoms object as this function is called from many places
		synchronized(this.pendingAtoms)
		{
			this.pendingAtoms.remove(pendingAtom.getHash());
		}
		
		this.timedout.remove(pendingAtom.getHash());
		this.atomQueue.remove(pendingAtom.getHash());
		
		if (atomsLog.hasLevel(Logging.DEBUG) == true)
			atomsLog.debug(AtomHandler.this.context.getName()+": Removed pending atom "+pendingAtom);
	}
	
	public List<AtomCertificate> certificates(final int limit, final Collection<Hash> exclusions)
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

		synchronized(this.pendingAtoms)
		{
			for (PendingAtom pendingAtom : this.pendingAtoms.values())
			{
				if (filter.test(pendingAtom) == false)
					continue;
				
				certificates.add(pendingAtom.getCertificate());
				
				if (certificates.size() == limit)
					break;
			}
		}
		
		return certificates;
	}


	boolean submit(final Atom atom) throws InterruptedException
	{
		Objects.requireNonNull(atom, "Atom is null");
		
		if (this.atomQueue.putIfAbsent(atom.getHash(), atom) == null)
		{
			if (atomsLog.hasLevel(Logging.DEBUG) == true)
				atomsLog.debug(AtomHandler.this.context.getName()+": Queued atom for storage "+atom.getHash());
		
			return true;
		}
		
		if (atomsLog.hasLevel(Logging.DEBUG) == true)
			atomsLog.debug(AtomHandler.this.context.getName()+": Atom "+atom.getHash()+" already queued for storage");

		return false;
	}

	// ASYNC BLOCK LISTENER //
	// TODO needs to go in ledger class part of pruning
/*	private EventListener asyncBlockListener = new EventListener()
	{
		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent)
		{
			AtomHandler.this.lock.writeLock().lock();
			try
			{
//				long cleanTo = blockCommittedEvent.getBlock().getHeader().getHeight() - Node.OOS_TRIGGER_LIMIT;
//				if (cleanTo > 0)
//					AtomHandler.this.context.getLedger().getLedgerStore().cleanSyncInventory(height);
			}
			finally
			{
				AtomHandler.this.lock.writeLock().unlock();
			}
		}
	};*/

	// SYNC BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final BlockCommittedEvent blockCommittedEvent)
		{
			AtomHandler.this.lock.writeLock().lock();
			try
			{
				for (Hash atom : blockCommittedEvent.getBlock().getHeader().getInventory(InventoryType.ATOMS))
				{
					PendingAtom pendingAtom = AtomHandler.this.pendingAtoms.get(atom);
					if (pendingAtom == null)
						throw new IllegalStateException("Pending atom "+atom+" accepted in block commit "+blockCommittedEvent.getBlock().getHeader()+" not found");
					
					pendingAtom.accepted();
					AtomHandler.this.context.getEvents().post(new AtomAcceptedEvent(pendingAtom));
				}
				
				for (AtomCertificate certificate : blockCommittedEvent.getBlock().getCertificates())
				{
					PendingAtom pendingAtom = AtomHandler.this.pendingAtoms.get(certificate.getAtom());
					if (pendingAtom == null)
						throw new IllegalStateException("Pending atom "+certificate.getAtom()+" committed via certificate "+certificate.getHash()+" in block commit "+blockCommittedEvent.getBlock().getHeader()+" not found");
					
					pendingAtom.completed();
				}

				for (Hash atom : blockCommittedEvent.getBlock().getHeader().getInventory(InventoryType.TIMEOUTS))
				{
					PendingAtom pendingAtom = AtomHandler.this.pendingAtoms.get(atom);
					if (pendingAtom == null)
						throw new IllegalStateException("Pending atom "+atom+" timed out in block commit "+blockCommittedEvent.getBlock().getHeader()+" not found");
					
					if (pendingAtom.getStatus().lessThan(CommitStatus.ACCEPTED) == true)
						AtomHandler.this.context.getEvents().post(new AtomAcceptedTimeoutEvent(pendingAtom));
					else if (pendingAtom.getStatus().greaterThan(CommitStatus.EXECUTED) == false)
						AtomHandler.this.context.getEvents().post(new AtomCommitTimeoutEvent(pendingAtom));
				}

				// Timeout atoms
				// Don't process timeouts until after the entire branch has been committed as 
				// pending atoms that would timeout may be committed somewhere on the branch ahead of this commit.
				// TODO atoms should be witnessed and timedout based on agreed ledger time
				long systemTime = Time.getSystemTime();
				synchronized(AtomHandler.this.pendingAtoms)
				{
					for (PendingAtom pendingAtom : AtomHandler.this.pendingAtoms.values())
					{
						if (pendingAtom.getStatus().equals(CommitStatus.NONE) == true && systemTime > pendingAtom.getWitnessedAt() + TimeUnit.SECONDS.toMillis(PendingAtom.ATOM_INCLUSION_TIMEOUT_CLOCK_SECONDS))
						{
							AtomHandler.this.context.getEvents().post(new AtomUnpreparedTimeoutEvent(pendingAtom));
						}
						else if (pendingAtom.getStatus().lessThan(CommitStatus.ACCEPTED) == true)
						{
							if (pendingAtom.lockCount() == 0 && systemTime > pendingAtom.getInclusionTimeout())
								AtomHandler.this.timedout.put(pendingAtom.getHash(), blockCommittedEvent.getBlock().getHeader().getHeight());
						}
						else if (pendingAtom.getStatus().greaterThan(CommitStatus.EXECUTED) == false)
						{
							if (blockCommittedEvent.getBlock().getHeader().getHeight() > pendingAtom.getCommitBlockTimeout() && 
								systemTime > pendingAtom.getAcceptedAt() + TimeUnit.SECONDS.toMillis(PendingAtom.ATOM_INCLUSION_TIMEOUT_CLOCK_SECONDS))
								AtomHandler.this.timedout.put(pendingAtom.getHash(), blockCommittedEvent.getBlock().getHeader().getHeight());
						}
					}
				}
				
				// Timeout housekeeping
				// TODO I think timeout housekeeping is required here.  Locally may have considered an atom timed out due to weak-subjectivity/latency,
				// but the network never agrees with the local opinion and the atom is accepted into a block or escalates to a commit timeout.
				Iterator<Entry<Hash, Long>> timeoutsIterator = AtomHandler.this.timedout.entrySet().iterator();
				while(timeoutsIterator.hasNext() == true)
				{
					Entry<Hash, Long> timeout = timeoutsIterator.next();
					if (timeout.getValue() > blockCommittedEvent.getBlock().getHeader().getHeight() - PendingAtom.ATOM_COMMIT_TIMEOUT_BLOCKS)
						continue;
					
					timeoutsIterator.remove();
				}
			}
			finally
			{
				AtomHandler.this.lock.writeLock().unlock();
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
			AtomHandler.this.context.getLedger().getLedgerStore().storeSyncInventory(AtomHandler.this.context.getLedger().getHead().getHeight(), event.getPendingAtom().getHash(), Atom.class, SyncInventoryType.COMMIT);
		}

		@Subscribe
		public void on(final AtomNegativeCommitEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
			AtomHandler.this.context.getLedger().getLedgerStore().storeSyncInventory(AtomHandler.this.context.getLedger().getHead().getHeight(), event.getPendingAtom().getHash(), Atom.class, SyncInventoryType.COMMIT);
		}

		@Subscribe
		public void on(final AtomUnpreparedTimeoutEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
			AtomHandler.this.context.getLedger().getLedgerStore().commitTimedOut(event.getPendingAtom().getHash());
			AtomHandler.this.context.getLedger().getLedgerStore().storeSyncInventory(AtomHandler.this.context.getLedger().getHead().getHeight(), event.getPendingAtom().getHash(), Atom.class, SyncInventoryType.COMMIT);
		}
		
		@Subscribe
		public void on(final AtomAcceptedTimeoutEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
			AtomHandler.this.context.getLedger().getLedgerStore().acceptTimedOut(event.getPendingAtom().getHash());
			AtomHandler.this.context.getLedger().getLedgerStore().storeSyncInventory(AtomHandler.this.context.getLedger().getHead().getHeight(), event.getPendingAtom().getHash(), Atom.class, SyncInventoryType.COMMIT);
		}
		
		@Subscribe
		public void on(AtomExceptionEvent event)
		{
			if (event.getException() instanceof StateLockedException)
			{
				PendingAtom pendingAtom = AtomHandler.this.pendingAtoms.get(event.getAtom().getHash());
				if (pendingAtom == null)
					return;
				
				pendingAtom.setInclusionDelay(Math.max(TimeUnit.SECONDS.toMillis(10), pendingAtom.getInclusionDelay() * 2));
				return;
			}
			else
				AtomHandler.this.remove(event.getPendingAtom());
		}
	};
	
	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncStatusChangeEvent event) 
		{
			AtomHandler.this.lock.writeLock().lock();
			try
			{
				if (event.isSynced() == true)
				{
					atomsLog.info(AtomHandler.this.context.getName()+": Sync status changed to "+event.isSynced()+", loading known atom handler state");
					
					for (long height = Math.max(0, AtomHandler.this.context.getLedger().getHead().getHeight() - Node.OOS_TRIGGER_LIMIT) ; height <= AtomHandler.this.context.getLedger().getHead().getHeight() ; height++)
					{
						try
						{
							Collection<Hash> items = AtomHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, Atom.class, SyncInventoryType.SEEN);
							for (Hash item : items)
								AtomHandler.this.load(item);
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomHandler.this.context.getName()+": Failed to load state for atom handler at height "+height, ex);
						}
					}
				}
				else
				{
					atomsLog.info(AtomHandler.this.context.getName()+": Sync status changed to "+event.isSynced()+", flushing atom handler");
					AtomHandler.this.atomQueue.clear();
					AtomHandler.this.pendingAtoms.clear();
					AtomHandler.this.timedout.clear();
				}
			}
			finally
			{
				AtomHandler.this.lock.writeLock().unlock();
			}
		}
	};
}
