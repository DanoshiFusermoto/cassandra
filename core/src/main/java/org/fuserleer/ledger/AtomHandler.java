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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.collections.MappedBlockingQueue;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.EventListener;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.LedgerStore.SyncInventoryType;
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.events.AtomAcceptedEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomDiscardedEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomPersistedEvent;
import org.fuserleer.ledger.events.AtomRejectedEvent;
import org.fuserleer.ledger.events.BlockCommitEvent;
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

public class AtomHandler implements Service
{
	private static final Logger atomsLog = Logging.getLogger("atoms");

	private final Context context;
	
	private final Map<Hash, PendingAtom> pendingAtoms = Collections.synchronizedMap(new HashMap<Hash, PendingAtom>());
	private final Map<Hash, Long> atomTimeouts = Collections.synchronizedMap(new HashMap<Hash, Long>());
	private final MappedBlockingQueue<Hash, Atom> atomQueue;

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
	
									pendingAtom.prepare();
									
				                	// Store all valid atoms even if they aren't within the local shard group.
									// Those atoms will be broadcast the the relevant groups and it needs to be stored
									// to be able to serve the requests for it.  Such atoms can be pruned per epoch
				                	AtomHandler.this.context.getLedger().getLedgerStore().store(AtomHandler.this.context.getLedger().getHead().getHeight(), pendingAtom.getAtom());  // TODO handle failure
				                	
				                	shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
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
							
		                	if (shardGroups.contains(localShardGroup) == true)
		                		AtomHandler.this.context.getEvents().post(new AtomPersistedEvent(pendingAtom));

		                	AtomHandler.this.context.getNetwork().getGossipHandler().broadcast(pendingAtom.getAtom(), shardGroups);
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
				atomsLog.fatal(AtomHandler.this.context.getName()+": Error processing atom queue", throwable);
			}
		}
	};
	
	AtomHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context);
		this.atomQueue = new MappedBlockingQueue<Hash, Atom>(this.context.getConfiguration().get("ledger.atom.queue", 1<<16));

//		atomsLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
//		atomsLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
//		atomsLog.setLevels(Logging.ERROR | Logging.FATAL);
	}

	@Override
	public void start() throws StartupException 
	{
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
					Atom atom = AtomHandler.this.atomQueue.get(item);
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
							
							for (PendingAtom pendingAtom : pendingAtoms)
							{
								if (pendingAtom.getStatus().lessThan(CommitStatus.PREPARED) == true)
									continue;
								
			                	Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), numShardGroups);
			                	if (shardGroups.contains(localShardGroup) == false)
			                		continue;

			                	pendingAtomInventory.add(pendingAtom.getHash());
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
		this.context.getEvents().register(this.asyncBlockListener);
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
		this.context.getEvents().unregister(this.asyncBlockListener);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	public int numQueued()
	{
		return this.atomQueue.size();
	}
	
	public int numPending()
	{
		return this.pendingAtoms.size();
	}
	
	public Collection<Hash> pending()
	{
		AtomHandler.this.lock.readLock().lock();
		try
		{
			List<Hash> pending = new ArrayList<Hash>(this.pendingAtoms.keySet());
			Collections.sort(pending);
			return pending;
		}
		finally
		{
			AtomHandler.this.lock.readLock().unlock();
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
			
			// ReadWriteLock doesn't support upgrades, so sync on pending atoms for thw "write"
			synchronized(this.pendingAtoms)
			{
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
			}
			
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
	private EventListener asyncBlockListener = new EventListener()
	{
		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent)
		{
			// TODO needs to go in ledger class
/*			AtomHandler.this.lock.writeLock().lock();
			try
			{
//				long cleanTo = blockCommittedEvent.getBlock().getHeader().getHeight() - Node.OOS_TRIGGER_LIMIT;
//				if (cleanTo > 0)
//					AtomHandler.this.context.getLedger().getLedgerStore().cleanSyncInventory(height);
			}
			finally
			{
				AtomHandler.this.lock.writeLock().unlock();
			}*/
		}
	};

	// SYNC BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(BlockCommitEvent blockCommitEvent)
		{
			AtomHandler.this.lock.writeLock().lock();
			try
			{
				for (Hash atom : blockCommitEvent.getBlock().getHeader().getInventory(InventoryType.ATOMS))
				{
					PendingAtom pendingAtom = AtomHandler.this.pendingAtoms.get(atom);
					if (pendingAtom == null)
						throw new IllegalStateException("Pending atom "+atom+" in block commit "+blockCommitEvent.getBlock().getHeader()+" not found");
					
					pendingAtom.accepted();
				}
				
				// Timeout atoms on progress
				// Don't process timeouts until after the entire branch has been committed as 
				// pending atoms that would timeout may be committed somewhere on the branch ahead of this commit.
				// TODO atoms should be witnessed and timedout based on agreed ledger time
				long systemTime = Time.getSystemTime();
				List<AtomDiscardedEvent> timedout = new ArrayList<AtomDiscardedEvent>();
				synchronized(AtomHandler.this.pendingAtoms)
				{
					for (PendingAtom pendingAtom : AtomHandler.this.pendingAtoms.values())
					{
						if (pendingAtom.lockCount() == 0 && systemTime > pendingAtom.getInclusionTimeout() && pendingAtom.getStatus().lessThan(CommitStatus.ACCEPTED) == true)
							timedout.add(new AtomDiscardedEvent(pendingAtom, "Timed out"));
					}
				}
				
				if (timedout.isEmpty() == false)
					timedout.forEach(ade -> 
					{
						AtomHandler.this.context.getEvents().post(ade);	// TODO ensure no synchronous event processing happens on this!
					});
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
		public void on(final AtomAcceptedEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
			AtomHandler.this.context.getLedger().getLedgerStore().storeSyncInventory(AtomHandler.this.context.getLedger().getHead().getHeight(), event.getPendingAtom().getHash(), Atom.class, SyncInventoryType.COMMIT);
		}

		@Subscribe
		public void on(final AtomRejectedEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
			AtomHandler.this.context.getLedger().getLedgerStore().storeSyncInventory(AtomHandler.this.context.getLedger().getHead().getHeight(), event.getPendingAtom().getHash(), Atom.class, SyncInventoryType.COMMIT);
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
			AtomHandler.this.context.getLedger().getLedgerStore().commitTimedOut(event.getPendingAtom().getHash());
		}
		
		@Subscribe
		public void on(final AtomDiscardedEvent event) throws IOException 
		{
			remove(event.getPendingAtom());
			// TODO Phantom pending atoms appear here after sync
//			if (event.getPendingAtom().getStatus().greaterThan(CommitStatus.NONE) == true)
				AtomHandler.this.context.getLedger().getLedgerStore().acceptTimedOut(event.getPendingAtom().getHash());
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
				
				if (Time.getSystemTime() > pendingAtom.getInclusionTimeout())
					AtomHandler.this.context.getEvents().post(new AtomDiscardedEvent(event.getPendingAtom(), event.getException().getMessage()));
					
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
				}
			}
			finally
			{
				AtomHandler.this.lock.writeLock().unlock();
			}
		}
	};
}
