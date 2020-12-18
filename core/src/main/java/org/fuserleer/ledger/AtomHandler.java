package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.collections.MappedBlockingQueue;
import org.fuserleer.common.Direction;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomPersistedEvent;
import org.fuserleer.ledger.events.AtomTimeoutEvent;
import org.fuserleer.ledger.messages.AtomBroadcastMessage;
import org.fuserleer.ledger.messages.AtomsMessage;
import org.fuserleer.ledger.messages.GetAtomsMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.PeerTask;
import org.fuserleer.node.Node;

public class AtomHandler implements Service
{
	private static final Logger atomsLog = Logging.getLogger("atoms");

	private final Context context;
	
	private final Set<Hash> 	atomsRequested = Collections.synchronizedSet(new HashSet<Hash>());
	private final AtomicInteger atomRequestsCounter = new AtomicInteger(0);
	private final AtomicLong 	atomsRequestedCounter = new AtomicLong(0l);
	private final MappedBlockingQueue<Hash, Atom> atomQueue;
	
	private Executable atomProcessor = new Executable()
	{
		private List<Hash> atomsToBroadcast = new ArrayList<>();
		
		@Override
		public void execute()
		{
			try 
			{
				long lastBroadcast = System.currentTimeMillis();
				while (this.isTerminated() == false)
				{
					try
					{
						Entry<Hash, Atom> atom = AtomHandler.this.atomQueue.peek(1, TimeUnit.SECONDS);
						if (atom != null)
						{
							if (atomsLog.hasLevel(Logging.DEBUG))
								atomsLog.debug(AtomHandler.this.context.getName()+": Verifying atom "+atom.getValue().getHash());

							try
							{
								// TODO currently relying on the store atom to catch existing atoms for performance.  
								//		may need this initial check back in if verification of atom form goes screwy
								//if (AtomHandler.this.ledgerStore.has(atom.getHash()) == true)
								//	throw new ValidationException("Atom "+atom.getHash()+" already processed and persisted");

								// TODO atom verification here (signatures etc)
								
			                	AtomHandler.this.context.getLedger().getLedgerStore().store(atom.getValue());  // TODO handle failure
			                	AtomHandler.this.context.getEvents().post(new AtomPersistedEvent(atom.getValue()));
			                	this.atomsToBroadcast.add(atom.getValue().getHash());
			                	AtomHandler.this.atomQueue.remove(atom.getKey());
							}
	/*						catch (ValidationException vex)
							{
								atomsLog.error("Validation failed for atom " + atom.getHash(), vex);
								Events.getInstance().post(new AtomErrorEvent(atom, vex));
							}*/
							catch (Exception ex)
							{
								atomsLog.error(AtomHandler.this.context.getName()+": Error processing for atom for " + atom.getValue().getHash(), ex);
								AtomHandler.this.context.getEvents().post(new AtomExceptionEvent(atom.getValue(), ex));
							}
						}
						
						if (this.atomsToBroadcast.size() == AtomBroadcastMessage.MAX_ATOMS ||
							(System.currentTimeMillis() - lastBroadcast > TimeUnit.SECONDS.toMillis(1) && this.atomsToBroadcast.isEmpty() == false))
						{
							if (atomsLog.hasLevel(Logging.DEBUG))
								atomsLog.debug(AtomHandler.this.context.getName()+": Broadcasting about "+this.atomsToBroadcast.size()+" atoms");
							
							lastBroadcast = System.currentTimeMillis();
							broadcast(this.atomsToBroadcast);
							this.atomsToBroadcast.clear();
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
		
		private void broadcast(List<Hash> atomsToBroadcast)
		{
			AtomBroadcastMessage atomBroadcastMessage = new AtomBroadcastMessage(atomsToBroadcast);
			for (ConnectedPeer connectedPeer : AtomHandler.this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED))
			{
				if (AtomHandler.this.context.getNode().isInSyncWith(connectedPeer.getNode(), Node.OOS_TRIGGER_LIMIT) == false)
					return;
				
				if (AtomHandler.this.context.getConfiguration().get("network.broadcast.type") != null)
					if (connectedPeer.getDirection().equals(Direction.valueOf(AtomHandler.this.context.getConfiguration().get("network.broadcast.type").toUpperCase())) == false)
						continue;
				
				try
				{
					AtomHandler.this.context.getNetwork().getMessaging().send(atomBroadcastMessage, connectedPeer);
				}
				catch (IOException ex)
				{
					atomsLog.error(AtomHandler.this.context.getName()+": Unable to send AtomBroadcastMessage for broadcast of " + atomsToBroadcast.size() + " atoms to " + connectedPeer, ex);
				}
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
		this.context.getNetwork().getMessaging().register(AtomBroadcastMessage.class, this.getClass(), new MessageProcessor<AtomBroadcastMessage>()
		{
			@Override
			public void process(final AtomBroadcastMessage atomBroadcastMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomHandler.this.context.getName()+": Atom broadcast of " + atomBroadcastMessage.getAtoms().size() + " from " + peer);
							
							if (AtomHandler.this.context.getNode().isInSyncWith(peer.getNode(), Node.OOS_TRIGGER_LIMIT) == false)
								return;

							synchronized(AtomHandler.this.atomsRequested)
							{
								List<Hash> atomsToRequest = new ArrayList<Hash>();
								for (Hash atom : atomBroadcastMessage.getAtoms())
								{
									// The order in which we check these sources is important as the caches are
									// updated BEFORE the queues on a successful store.
									if (AtomHandler.this.atomsRequested.contains(atom) == true ||
										AtomHandler.this.atomQueue.contains(atom) == true ||
										AtomHandler.this.context.getLedger().getLedgerStore().has(atom) == true)
										continue;
									
									atomsToRequest.add(atom);
								}
								
								if (atomsToRequest.isEmpty() == false)
									AtomHandler.this.requestAtoms(peer, atomsToRequest);
							}
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomHandler.this.context.getName()+": ledger.atoms.messages.broadcast " + peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(GetAtomsMessage.class, this.getClass(), new MessageProcessor<GetAtomsMessage>()
		{
			@Override
			public void process(final GetAtomsMessage getAtomsMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomHandler.this.context.getName()+": Received atoms request from " + peer + " of " + getAtomsMessage.getSize() + " atoms");
	
							List<Atom> atomsToSend = new ArrayList<Atom>();
							for (Hash hash : getAtomsMessage.getAtoms())
							{
								Atom atom = AtomHandler.this.atomQueue.get(hash);
								if (atom == null)
									atom = AtomHandler.this.context.getLedger().getLedgerStore().get(hash, Atom.class);

								if (atom != null)
								{
									atomsToSend.add(atom);
									
									if (atomsToSend.size() == AtomsMessage.MAX_ATOMS)
									{
										AtomHandler.this.context.getNetwork().getMessaging().send(new AtomsMessage(getAtomsMessage.getNonce(), atomsToSend), peer);
										atomsToSend.clear();
									}
								}
								else
									atomsLog.error(AtomHandler.this.context.getName()+": Requested atom "+hash+" not found");
							}
							
							if (atomsToSend.isEmpty() == false)
								AtomHandler.this.context.getNetwork().getMessaging().send(new AtomsMessage(getAtomsMessage.getNonce(), atomsToSend), peer);
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomHandler.this.context.getName()+": ledger.messages.atoms.get " + peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(AtomsMessage.class, this.getClass(), new MessageProcessor<AtomsMessage>()
		{
			@Override
			public void process(final AtomsMessage atomsMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomHandler.this.context.getName()+": Received "+atomsMessage.getAtoms().size()+" atoms from " + peer);

							synchronized(AtomHandler.this.atomsRequested)
							{
								for (Atom atom : atomsMessage.getAtoms())
								{
									if (AtomHandler.this.atomsRequested.contains(atom.getHash()) == false)
									{
										atomsLog.error(AtomHandler.this.context.getName()+": Received unrequested atom "+atom.getHash()+" from "+peer);
										peer.disconnect("Received unrequested atom "+atom.getHash());
										break;
									}
	
									if (AtomHandler.this.atomsRequested.remove(atom.getHash()) == true)
										AtomHandler.this.submit(atom);
								}
							}
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomHandler.this.context.getName()+": ledger.messages.atom " + peer, ex);
						}
					}
				});
			}
		});

		Thread atomProcessorThread = new Thread(this.atomProcessor);
		atomProcessorThread.setDaemon(true);
		atomProcessorThread.setName(this.context.getName()+" Atom Processor");
		atomProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException 
	{
		this.atomProcessor.terminate(true);

		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	public int getQueueSize()
	{
		return this.atomQueue.size();
	}

	boolean submit(Atom atom) throws InterruptedException
	{
		this.atomQueue.put(atom.getHash(), atom);
		
		if (atomsLog.hasLevel(Logging.DEBUG) == true)
			atomsLog.debug(AtomHandler.this.context.getName()+": Queued atom for storage "+atom.getHash());
		
		return true;
	}
	
	@SuppressWarnings("unchecked")
	Collection<Hash> requestAtoms(final ConnectedPeer peer, final Collection<Hash> atoms) throws IOException
	{
		final List<Hash> atomsPending = new ArrayList<Hash>();
		final List<Hash> atomsToRequest = new ArrayList<Hash>();
			
		synchronized(this.atomsRequested)
		{
			for (Hash atom : atoms)
			{
				if (this.atomsRequested.contains(atom) == true || 
					this.atomQueue.contains(atom) == true)
				{
					atomsPending.add(atom);
				}
				else if (AtomHandler.this.context.getLedger().getLedgerStore().has(atom) == false)
				{
					atomsToRequest.add(atom);
					atomsPending.add(atom);
				}
			}

			if (atomsPending.isEmpty() == true)
			{
				atomsLog.warn(AtomHandler.this.context.getName()+": No atoms required from "+peer);
				return Collections.EMPTY_LIST;
			}
			
			if (atomsToRequest.isEmpty() == false)
			{
				try
				{
					this.atomsRequested.addAll(atomsToRequest);
					
					if (atomsLog.hasLevel(Logging.DEBUG))
					{	
						atomsToRequest.forEach(a -> {
							atomsLog.debug(AtomHandler.this.context.getName()+": Requesting atom " + a + " from " + peer);
						});
					}
	
					GetAtomsMessage getAtomsMessage = new GetAtomsMessage(atomsToRequest); 
					this.context.getNetwork().getMessaging().send(getAtomsMessage, peer);
					
					Executor.getInstance().schedule(new PeerTask(peer, 10, TimeUnit.SECONDS) 
					{
						final Collection<Hash> requestedAtoms = new ArrayList<Hash>(atomsToRequest);
						
						@Override
						public void execute()
						{
							List<Hash> failedAtomRequests = new ArrayList<Hash>();
							synchronized(AtomHandler.this.atomsRequested)
							{
								for (Hash requestedAtom : this.requestedAtoms)
								{
									if (AtomHandler.this.atomsRequested.remove(requestedAtom) == true)
										failedAtomRequests.add(requestedAtom);
								}
							}
							
							if (failedAtomRequests.isEmpty() == false)
							{
								for (Hash failedAtomRequest : failedAtomRequests)
									AtomHandler.this.context.getEvents().post(new AtomTimeoutEvent(failedAtomRequest));
								
								if (getPeer().getState().equals(PeerState.CONNECTED) || getPeer().getState().equals(PeerState.CONNECTING))
								{
									atomsLog.error(AtomHandler.this.context.getName()+": "+getPeer()+" did not respond to atom request of "+this.requestedAtoms.size()+" atoms");
									getPeer().disconnect("Did not respond to atom request of "+this.requestedAtoms.size()+" atoms");
								}
							}
						}
					});

					this.atomRequestsCounter.incrementAndGet();
					this.atomsRequestedCounter.addAndGet(atoms.size());
					
					if (atomsLog.hasLevel(Logging.DEBUG))
						atomsLog.debug(AtomHandler.this.context.getName()+": Requesting "+getAtomsMessage.getAtoms().size()+" atoms with nonce "+getAtomsMessage.getNonce()+" from "+peer);
				}
				catch (Throwable t)
				{
					this.atomsRequested.removeAll(atomsToRequest);
					throw t;
				}
			}
		}
		
		return atomsPending;
	}

	public long atomRequestsCount()
	{
		return this.atomRequestsCounter.get();
	}

	public long atomsRequestedCount()
	{
		return this.atomsRequestedCounter.get();
	}
}
