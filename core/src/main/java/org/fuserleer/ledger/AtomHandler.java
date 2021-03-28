package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.events.AtomAcceptedEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomDiscardedEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomPersistedEvent;
import org.fuserleer.ledger.events.AtomRejectedEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
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

import com.google.common.eventbus.Subscribe;

public class AtomHandler implements Service
{
	private static final Logger atomsLog = Logging.getLogger("atoms");

	private final Context context;
	
	private final Map<Hash, PendingAtom> pendingAtoms = Collections.synchronizedMap(new HashMap<>());
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

			@Override
			public int requestLimit()
			{
				return GetInventoryItemsMessage.MAX_ITEMS;
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

							// TODO will cause problems when pool is BIG
							Set<PendingAtom> pendingAtoms = new HashSet<PendingAtom>(AtomHandler.this.pendingAtoms.values());
							final Set<Hash> pendingAtomInventory = new HashSet<Hash>();
							final Set<Hash> atomVoteInventory = new HashSet<Hash>();
							
							for (PendingAtom pendingAtom : pendingAtoms)
							{
								pendingAtomInventory.add(pendingAtom.getHash());
								
								for (AtomVote atomVote : pendingAtom.votes())
									atomVoteInventory.add(atomVote.getHash());
							}
							
							long height = AtomHandler.this.context.getLedger().getHead().getHeight();
							while (height >= Math.max(0, syncAcquiredMessage.getHead().getHeight() - Node.OOS_RESOLVED_LIMIT))
							{
								pendingAtomInventory.addAll(AtomHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, Atom.class));
								atomVoteInventory.addAll(AtomHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, AtomVote.class));
								height--;
							}
							
							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomHandler.this.context.getName()+": Broadcasting "+pendingAtomInventory.size()+" / "+pendingAtomInventory+" pool atoms to "+peer);

							int offset = 0;
							while(offset < pendingAtomInventory.size())
							{
								SyncInventoryMessage pendingAtomInventoryMessage = new SyncInventoryMessage(pendingAtomInventory, offset, Math.min(offset+SyncInventoryMessage.MAX_ITEMS, pendingAtomInventory.size()), Atom.class);
								AtomHandler.this.context.getNetwork().getMessaging().send(pendingAtomInventoryMessage, peer);
								offset += SyncInventoryMessage.MAX_ITEMS; 
							}

							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomHandler.this.context.getName()+": Broadcasting "+atomVoteInventory.size()+" / "+atomVoteInventory+" pool atom votes to "+peer);

							offset = 0;
							while(offset < atomVoteInventory.size())
							{
								SyncInventoryMessage atomVoteInventoryMessage = new SyncInventoryMessage(atomVoteInventory, offset, Math.min(offset+SyncInventoryMessage.MAX_ITEMS, atomVoteInventory.size()),  AtomVote.class);
								AtomHandler.this.context.getNetwork().getMessaging().send(atomVoteInventoryMessage, peer);
								offset += SyncInventoryMessage.MAX_ITEMS; 
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

		Thread atomProcessorThread = new Thread(this.atomProcessor);
		atomProcessorThread.setDaemon(true);
		atomProcessorThread.setName(this.context.getName()+" Atom Processor");
		atomProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException 
	{
		this.atomProcessor.terminate(true);

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

	/**
	 * Returns an existing pending atom or creates it providing that the atom has not being included in a block.
	 * <br><br>
	 * TODO The above assumption should be tested in all cases.
	 * 
	 * @param atom The atom hash
	 * @return A pending atom or null
	 * @throws IOException
	 */
	PendingAtom get(Hash atom) throws IOException
	{
		AtomHandler.this.lock.readLock().lock();
		try
		{
			PendingAtom pendingAtom = this.pendingAtoms.get(atom);
			if (pendingAtom != null)
				return pendingAtom;
			
			if (pendingAtom == null && this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, atom)) != null)
				return null;

			pendingAtom = PendingAtom.create(this.context, atom);
			this.pendingAtoms.put(atom, pendingAtom);
			return pendingAtom;
		}
		finally
		{
			AtomHandler.this.lock.readLock().unlock();
		}
	}
	
	void push(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom for injection is null");
		
		if (pendingAtom.getStatus().equals(CommitStatus.PROVISIONING) == false)
			throw new IllegalStateException(this.context.getName()+": Pending atom "+pendingAtom.getHash()+" for injection must be in PROVISIONING state");
		
		AtomHandler.this.lock.writeLock().lock();
		try
		{
			if (this.pendingAtoms.containsKey(pendingAtom.getHash()) == true)
				throw new IllegalStateException(this.context.getName()+": Pending atom "+pendingAtom.getHash()+" is already present");
			
			this.pendingAtoms.put(pendingAtom.getHash(), pendingAtom);
		}
		finally
		{
			AtomHandler.this.lock.writeLock().unlock();
		}
	}

	void remove(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom for injection is null");
		
		AtomHandler.this.lock.writeLock().lock();
		try
		{
			this.pendingAtoms.remove(pendingAtom.getHash());
		}
		finally
		{
			AtomHandler.this.lock.writeLock().unlock();
		}
	}

	boolean submit(Atom atom) throws InterruptedException
	{
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

	// SYNC ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
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
		public void on(final AtomDiscardedEvent event) 
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
					for (long height = Math.max(0, AtomHandler.this.context.getLedger().getHead().getHeight() - Node.OOS_TRIGGER_LIMIT) ; height <  AtomHandler.this.context.getLedger().getHead().getHeight() ; height++)
					{
						try
						{
							Collection<Hash> items = AtomHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, Atom.class);
							for (Hash item : items)
							{
								Commit commit = AtomHandler.this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, item));
								if (commit != null && commit.getPath().get(Elements.CERTIFICATE) != null)
									continue;
									
								PendingAtom pendingAtom = AtomHandler.this.get(item);
								if (pendingAtom == null)
								{
									Atom atom = AtomHandler.this.context.getLedger().getLedgerStore().get(item, Atom.class);
									submit(atom);
								}
							}
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
