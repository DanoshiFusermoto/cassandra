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

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.collections.MappedBlockingQueue;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.events.AtomAcceptedEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomDiscardedEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomPersistedEvent;
import org.fuserleer.ledger.events.AtomRejectedEvent;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.GossipFetcher;
import org.fuserleer.network.GossipFilter;
import org.fuserleer.network.GossipInventory;
import org.fuserleer.network.GossipReceiver;

import com.google.common.eventbus.Subscribe;

public class AtomHandler implements Service
{
	private static final Logger atomsLog = Logging.getLogger("atoms");

	private final Context context;
	
	private final Map<Hash, PendingAtom> pendingAtoms = Collections.synchronizedMap(new HashMap<>());
	private final MappedBlockingQueue<Hash, Atom> atomQueue;
	
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

							// TODO may need another exception wrapping this incase exceptions get thrown ... will be messy though
							final PendingAtom pendingAtom = AtomHandler.this.pendingAtoms.computeIfAbsent(atom.getKey(), (k) -> PendingAtom.create(AtomHandler.this.context, atom.getValue()));
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
								
								// TODO atom verification here (signatures etc)
								
			                	// Store all valid atoms even if they aren't within the local shard group.
								// Those atoms will be broadcast the the relevant groups and it needs to be stored
								// to be able to serve the requests for it.  Such atoms can be pruned per epoch
			                	AtomHandler.this.context.getLedger().getLedgerStore().store(pendingAtom.getAtom());  // TODO handle failure
			                	
			                	Collection<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), AtomHandler.this.context.getLedger().numShardGroups(AtomHandler.this.context.getLedger().getHead().getHeight()));
			                	if (shardGroups.contains(ShardMapper.toShardGroup(AtomHandler.this.context.getNode().getIdentity(), AtomHandler.this.context.getLedger().numShardGroups(AtomHandler.this.context.getLedger().getHead().getHeight()))) == true)
			                		AtomHandler.this.context.getEvents().post(new AtomPersistedEvent(pendingAtom));
			                	else
			                		AtomHandler.this.pendingAtoms.remove(pendingAtom.getHash(), pendingAtom);
			                	
			                	AtomHandler.this.context.getNetwork().getGossipHandler().broadcast(pendingAtom.getAtom(), shardGroups);
							}
							catch (Exception ex)
							{
								atomsLog.error(AtomHandler.this.context.getName()+": Error processing for atom for " + atom.getValue().getHash(), ex);
								AtomHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
							}
							finally
							{
			                	AtomHandler.this.atomQueue.remove(atom.getKey());
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
				atomsLog.fatal(AtomHandler.this.context.getName()+": Error processing atom queue", throwable);
			}
		}
	};
	
	AtomHandler(Context context)
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
			public Collection<Hash> required(Class<? extends Primitive> type, Collection<Hash> items) throws IOException
			{
				Set<Hash> required = new HashSet<Hash>();
				for (Hash item : items)
				{
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
			public void receive(Primitive object) throws InterruptedException
			{
				AtomHandler.this.submit((Atom) object);
			}
		});

		this.context.getNetwork().getGossipHandler().register(Atom.class, new GossipFetcher() 
		{
			@Override
			public Collection<Atom> fetch(Collection<Hash> items) throws IOException
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

		this.context.getEvents().register(this.syncAtomListener);

		Thread atomProcessorThread = new Thread(this.atomProcessor);
		atomProcessorThread.setDaemon(true);
		atomProcessorThread.setName(this.context.getName()+" Atom Processor");
		atomProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException 
	{
		this.atomProcessor.terminate(true);

		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	public int getQueueSize()
	{
		return this.atomQueue.size();
	}
	
	/**
	 * Returns an existing pending atom or creates it providing that the atom is not already present in the store.
	 * <br><br>
	 * If the atom is not in the pending list and is persisted, it is assumed that it has already undergone
	 * the pending atom process, as the only mechanism for it to be persisted is to first exist as a pending atom.
	 * <br><br>
	 * TODO The above assumption should be tested in all cases.
	 * 
	 * @param atom The atom hash
	 * @return A pending atom or null
	 * @throws IOException
	 */
	PendingAtom get(Hash atom) throws IOException
	{
		synchronized(this.pendingAtoms)
		{
			PendingAtom pendingAtom = this.pendingAtoms.get(atom);
			if (pendingAtom != null)
				return pendingAtom;
			
			if (pendingAtom == null && this.context.getLedger().getLedgerStore().has(atom) == true)
				return null;

			pendingAtom = PendingAtom.create(this.context, atom);
			this.pendingAtoms.put(atom, pendingAtom);
			return pendingAtom;
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
	// SYNC ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomAcceptedEvent event) 
		{
			AtomHandler.this.pendingAtoms.remove(event.getPendingAtom().getHash());
		}

		@Subscribe
		public void on(final AtomRejectedEvent event) 
		{
			AtomHandler.this.pendingAtoms.remove(event.getPendingAtom().getHash());
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event) 
		{
			AtomHandler.this.pendingAtoms.remove(event.getPendingAtom().getHash());
		}
		
		@Subscribe
		public void on(final AtomDiscardedEvent event) 
		{
			AtomHandler.this.pendingAtoms.remove(event.getPendingAtom().getHash());
		}
		
		@Subscribe
		public void on(final AtomExceptionEvent event) 
		{
			if (event.getException() instanceof StateLockedException)
				return;
				
			AtomHandler.this.pendingAtoms.remove(event.getPendingAtom().getHash());
		}
	};
}
