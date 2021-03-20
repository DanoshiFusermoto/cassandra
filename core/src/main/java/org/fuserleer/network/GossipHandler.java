package org.fuserleer.network;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.EventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.ShardMapper;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.messages.BroadcastInventoryMessage;
import org.fuserleer.network.messages.GetInventoryItemsMessage;
import org.fuserleer.network.messages.InventoryItemsMessage;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.PeerTask;
import org.fuserleer.network.peers.events.PeerDisconnectedEvent;
import org.fuserleer.network.peers.filters.StandardPeerFilter;
import org.fuserleer.serialization.Serialization;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.eventbus.Subscribe;

public class GossipHandler implements Service
{
	private static final Logger gossipLog = Logging.getLogger("gossip");

	private final class GossipPeerTask extends PeerTask 
	{
		final Class<? extends Primitive> type;
		final Map<Hash, Long> items;
//		final boolean rerequest;
		
		GossipPeerTask(final ConnectedPeer peer, final Map<Hash, Long> items, final Class<? extends Primitive> type)
		{
			super(peer, 10, TimeUnit.SECONDS);
			
			this.type = type;
			this.items = new HashMap<Hash, Long>(items);
//			this.rerequest = rerequest;
		}
		
		@Override
		public void execute()
		{
			List<Hash> failedItemRequests = new ArrayList<Hash>();
			
			GossipHandler.this.lock.writeLock().lock();
			try
			{
				GossipHandler.this.requestTasks.remove(getPeer(), this);
				
				for (Entry<Hash, Long> requestedItem : this.items.entrySet())
				{
					if (GossipHandler.this.itemsRequested.containsKey(requestedItem.getKey()) == true && 
						GossipHandler.this.itemsRequested.get(requestedItem.getKey()) == requestedItem.getValue())
					{
						GossipHandler.this.itemsRequested.remove(requestedItem.getKey());
						failedItemRequests.add(requestedItem.getKey());
					}
				}
			}
			finally
			{
				GossipHandler.this.lock.writeLock().unlock();
			}
			
			// Can do the disconnect and re-request outside of the lock
			if (failedItemRequests.isEmpty() == false)
			{
				try
				{
					gossipLog.error(GossipHandler.this.context.getName()+": "+getPeer()+" did not respond fully to request of "+this.items.size()+" items of type "+this.type+" "+failedItemRequests);
					if (getPeer().getState().equals(PeerState.CONNECTED) || getPeer().getState().equals(PeerState.CONNECTING))
//					if (this.rerequest == false && (getPeer().getState().equals(PeerState.CONNECTED) || getPeer().getState().equals(PeerState.CONNECTING)))
						getPeer().disconnect("Did not respond fully to request of "+this.items.size()+" items of type "+this.type+" "+failedItemRequests);
				}
				catch (Throwable t)
				{
					gossipLog.error(GossipHandler.this.context.getName()+": "+getPeer(), t);
				}
				
//				if (this.rerequest == false)
//					rerequest(failedItemRequests, this.type);
			}
		}

		@Override
		public void cancelled()
		{
			GossipHandler.this.lock.writeLock().lock();
			try
			{
				GossipHandler.this.requestTasks.remove(getPeer(), this);
				
				List<Hash> failedItemRequests = new ArrayList<Hash>();
				for (Entry<Hash, Long> item : this.items.entrySet())
				{
					if (GossipHandler.this.itemsRequested.containsKey(item.getKey()) == true && 
						GossipHandler.this.itemsRequested.get(item.getKey()) == item.getValue())
						failedItemRequests.add(item.getKey());
				}

				for (Entry<Hash, Long> requestedItem : this.items.entrySet())
					GossipHandler.this.itemsRequested.remove(requestedItem.getKey(), requestedItem.getValue());
				
//				if (failedItemRequests.isEmpty() == false && this.rerequest == false)
//					rerequest(failedItemRequests, this.type);
			}
			finally
			{
				GossipHandler.this.lock.writeLock().unlock();
			}
		}
		
/*		private void rerequest(final Collection<Hash> items, final Class<? extends Primitive> type)
		{
			// Build the re-requests
			long rerequestShardGroup = ShardMapper.toShardGroup(getPeer().getNode().getIdentity(), GossipHandler.this.context.getLedger().numShardGroups());
			List<ConnectedPeer> rerequestConnectedPeers = GossipHandler.this.context.getNetwork().get(StandardPeerFilter.build(GossipHandler.this.context).setStates(PeerState.CONNECTED).setShardGroup(rerequestShardGroup));
			if (rerequestConnectedPeers.isEmpty() == false)
			{
				ConnectedPeer rerequestPeer = rerequestConnectedPeers.get(0);
				try
				{
					GossipHandler.this.request(rerequestPeer, items, this.type, true);
				}
				catch (IOException ioex)
				{
					gossipLog.error(GossipHandler.this.context.getName()+": Failed to re-request "+items+" items of type "+this.type+" from "+rerequestPeer, ioex);
				}
			}
			else
				gossipLog.error(GossipHandler.this.context.getName()+": Unable to re-request "+items.size()+" items of type "+this.type);
		}*/
	}
		
	private class Broadcast
	{
		private final Primitive 	primitive;
		private final Set<Long>		shardGroups;
		
		Broadcast(Primitive primitive)
		{
			this.primitive = Objects.requireNonNull(primitive, "Primitive is null");
			this.shardGroups = new HashSet<Long>();
		}

		Broadcast(Primitive primitive, Collection<Long> shardGroups)
		{
			this.primitive = Objects.requireNonNull(primitive, "Primitive is null");
			this.shardGroups = new HashSet<Long>(Objects.requireNonNull(shardGroups, "Shard groups is null"));
		}

		public Primitive getPrimitive()
		{
			return this.primitive;
		}

		public Set<Long> getShardGroups()
		{
			return this.shardGroups;
		}
		
		public void setShardGroups(Collection<Long> shardGroups)
		{
			this.shardGroups.clear();
			this.shardGroups.addAll(Objects.requireNonNull(shardGroups, "Shard groups is null"));
		}
	}
	
	private final Context context;

	private final Semaphore queued = new Semaphore(0);
	private final Map<Hash, Long> itemsRequested = Collections.synchronizedMap(new HashMap<Hash, Long>());
	private final Multimap<ConnectedPeer, GossipPeerTask> requestTasks = Multimaps.synchronizedMultimap(HashMultimap.create());
	private final Multimap<ConnectedPeer, Hash> itemInventories = Multimaps.synchronizedMultimap(HashMultimap.create());

	private final Multimap<Class<? extends Primitive>, Broadcast> toBroadcast = Multimaps.synchronizedMultimap(HashMultimap.create());
	private final Map<Class<? extends Primitive>, GossipFilter> broadcastFilters = Collections.synchronizedMap(new HashMap<>());
	private final Map<Class<? extends Primitive>, GossipInventory> inventoryProcessors = Collections.synchronizedMap(new HashMap<>());
	private final Map<Class<? extends Primitive>, GossipFetcher> fetcherProcessors = Collections.synchronizedMap(new HashMap<>());
	private final Map<Class<? extends Primitive>, GossipReceiver> receiverProcessors = Collections.synchronizedMap(new HashMap<>());
	
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	
	private Executable broadcastProcessor = new Executable()
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
						Thread.sleep(100);

						// TODO convert to a wait / notify
						if (GossipHandler.this.queued.tryAcquire(1, TimeUnit.SECONDS) == false)
							continue;
						
						List<Class<? extends Primitive>> types = new ArrayList<>(GossipHandler.this.toBroadcast.keySet());
						for (Class<? extends Primitive> type : types)
						{
							List<Broadcast> broadcastQueue = new ArrayList<>(GossipHandler.this.toBroadcast.get(type));
							if (broadcastQueue.isEmpty() == true)
								continue;
							
							if (GossipHandler.this.context.getNode().isSynced() == true)
							{
								GossipFilter filter = GossipHandler.this.broadcastFilters.get(type);
								if (filter != null)
								{
									Multimap<Long, Hash> toBroadcast = HashMultimap.create();
									for (Broadcast broadcast : broadcastQueue)
									{
										try
										{
											if (broadcast.getShardGroups().isEmpty() == true)
												broadcast.setShardGroups(filter.filter(broadcast.getPrimitive()));
										}
										catch (Exception ex)
										{
											gossipLog.error(GossipHandler.this.context.getName()+": Filter for "+type+" failed on "+broadcast.getPrimitive(), ex);
											continue;
										}
											
										for(long shardGroup : broadcast.getShardGroups())
											toBroadcast.put(shardGroup, broadcast.getPrimitive().getHash());
									}
									
									for (long shardGroup : toBroadcast.keySet())
									{
										int offset = 0;
										List<Hash> toBroadcastList = new ArrayList<Hash>(toBroadcast.get(shardGroup));
										while(offset < toBroadcastList.size())
										{
											BroadcastInventoryMessage broadcastInventoryMessage = new BroadcastInventoryMessage(toBroadcastList.subList(offset, Math.min(offset+BroadcastInventoryMessage.MAX_ITEMS, toBroadcastList.size())), type);
											for (ConnectedPeer connectedPeer : GossipHandler.this.context.getNetwork().get(StandardPeerFilter.build(GossipHandler.this.context).setStates(PeerState.CONNECTED).setShardGroup(shardGroup)))
											{
												if (connectedPeer.getNode().isSynced() == false)
												{
													if (gossipLog.hasLevel(Logging.DEBUG) == true)
														gossipLog.debug(GossipHandler.this.context.getName()+": Aborting (not synced) broadcast of inv type "+type+" containing "+broadcastInventoryMessage.getItems().size()+" items to " + connectedPeer);

													continue;
												}
												
												try
												{
													if (gossipLog.hasLevel(Logging.DEBUG) == true)
														gossipLog.debug(GossipHandler.this.context.getName()+": Broadcasting inv type "+type+" containing "+broadcastInventoryMessage.getItems().size()+" items to " + connectedPeer);

													GossipHandler.this.context.getNetwork().getMessaging().send(broadcastInventoryMessage, connectedPeer);
												}
												catch (IOException ex)
												{
													gossipLog.error(GossipHandler.this.context.getName()+": Unable to send BroadcastInventoryMessage of "+toBroadcastList+" items in shard group "+shardGroup+" to "+connectedPeer, ex);
												}
											}
											
											offset += BroadcastInventoryMessage.MAX_ITEMS;
										}
									}
								}
							}
							
							for (Broadcast broadcast : broadcastQueue)
								GossipHandler.this.toBroadcast.remove(type, broadcast);
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
				gossipLog.fatal(GossipHandler.this.context.getName()+": Error processing gossip queue", throwable);
			}
		}
	};

	GossipHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");

		gossipLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
//		gossipLog.setLevels(Logging.ERROR | Logging.FATAL);
	}

	@Override
	public void start() throws StartupException
	{
		this.context.getNetwork().getMessaging().register(BroadcastInventoryMessage.class, this.getClass(), new MessageProcessor<BroadcastInventoryMessage>()
		{
			@Override
			public void process(final BroadcastInventoryMessage broadcastInvMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						GossipHandler.this.lock.writeLock().lock();
						try
						{
							if (GossipHandler.this.context.getLedger().isSynced() == false)
							{
								if (gossipLog.hasLevel(Logging.DEBUG) == true)
									gossipLog.debug(GossipHandler.this.context.getName()+": Aborting (not synced) processing of broadcast inv type "+broadcastInvMessage.getType()+" containing "+broadcastInvMessage.getItems().size()+" items from " + peer);

								return;
							}

							if (gossipLog.hasLevel(Logging.DEBUG) == true)
								gossipLog.debug(GossipHandler.this.context.getName()+": Broadcast inv type "+broadcastInvMessage.getType()+" containing "+broadcastInvMessage.getItems().size()+" items from " + peer);
							
							List<Hash> toRequest = new ArrayList<Hash>();
							List<Hash> required = new ArrayList<Hash>();
							GossipInventory inventoryProcessor = GossipHandler.this.inventoryProcessors.get(broadcastInvMessage.getType());
							if (inventoryProcessor == null)
							{
								gossipLog.error(GossipHandler.this.context.getName()+": Inventory processor for "+broadcastInvMessage.getType()+" is not found");
								return;
							}

							required.addAll(inventoryProcessor.required(broadcastInvMessage.getType(),  broadcastInvMessage.getItems()));
							if (required.isEmpty() == false)
							{
								GossipHandler.this.itemInventories.putAll(peer, required);

								for (Hash item : required)
								{
									if (GossipHandler.this.itemsRequested.containsKey(item) == true)
										continue;
	
									toRequest.add(item);
								}
								
								if (toRequest.isEmpty() == false)
									GossipHandler.this.request(peer, toRequest, broadcastInvMessage.getType());//, false);
							}
						}
						catch (Throwable t)
						{
							gossipLog.error(GossipHandler.this.context.getName()+": ledger.messages.gossip.inventory.broadcast "+peer, t);
						}
						finally
						{
							GossipHandler.this.lock.writeLock().unlock();
						}
					}
				});
			}
		});
		
		this.context.getNetwork().getMessaging().register(GetInventoryItemsMessage.class, this.getClass(), new MessageProcessor<GetInventoryItemsMessage>()
		{
			@Override
			public void process(final GetInventoryItemsMessage getInventoryItemsMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						GossipHandler.this.lock.readLock().lock();
						try
						{
							if (gossipLog.hasLevel(Logging.DEBUG) == true)
								gossipLog.debug(GossipHandler.this.context.getName()+": Received request of type "+getInventoryItemsMessage.getType()+" from "+peer+" of " + getInventoryItemsMessage.getItems().size() + " items "+getInventoryItemsMessage.getItems());
	
							GossipFetcher fetcher = GossipHandler.this.fetcherProcessors.get(getInventoryItemsMessage.getType());
							if (fetcher == null)
							{
								gossipLog.warn(GossipHandler.this.context.getName()+": No fetcher found for type "+getInventoryItemsMessage.getType());
								return;
							}
							
							Collection<? extends Primitive> fetched = fetcher.fetch(getInventoryItemsMessage.getItems());
							InventoryItemsMessage inventoryItemsMessage = null;
							for (Primitive object : fetched)
							{
								if (gossipLog.hasLevel(Logging.DEBUG) == true)
									gossipLog.debug(GossipHandler.this.context.getName()+": Sending requested item "+object.getHash()+" of type "+getInventoryItemsMessage.getType()+" to "+peer);
								
								if (inventoryItemsMessage == null)
									inventoryItemsMessage = new InventoryItemsMessage(getInventoryItemsMessage.getType());
								
								inventoryItemsMessage.add(object);
								
								if (inventoryItemsMessage.size() > InventoryItemsMessage.TRANSMIT_AT_SIZE)
								{
									GossipHandler.this.context.getNetwork().getMessaging().send(inventoryItemsMessage, peer);
									inventoryItemsMessage = null;
								}
							}

							if (inventoryItemsMessage != null)
								GossipHandler.this.context.getNetwork().getMessaging().send(inventoryItemsMessage, peer);
						}
						catch (Throwable t)
						{
							gossipLog.error(GossipHandler.this.context.getName()+": ledger.messages.gossip.inventory.item " + peer, t);
						}
						finally
						{
							GossipHandler.this.lock.readLock().unlock();
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(InventoryItemsMessage.class, this.getClass(), new MessageProcessor<InventoryItemsMessage>()
		{
			@Override
			public void process(final InventoryItemsMessage inventoryItemsMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						GossipHandler.this.lock.writeLock().lock();
						try
						{
							List<Primitive> unrequested = new ArrayList<Primitive>();
							for (Primitive item : inventoryItemsMessage.getItems())
							{
								if (gossipLog.hasLevel(Logging.DEBUG) == true)
									gossipLog.debug(GossipHandler.this.context.getName()+": Received item "+item.getHash()+" of type "+inventoryItemsMessage.getType()+" from " + peer);
	
								GossipHandler.this.received(item);
								if (GossipHandler.this.itemsRequested.remove(item.getHash()) == null)
								{
									gossipLog.error(GossipHandler.this.context.getName()+": Received unrequested item "+item.getHash()+" of type "+inventoryItemsMessage.getType()+" from "+peer);
									unrequested.add(item);
									continue;
								}
								else
									GossipHandler.this.receiverProcessors.get(inventoryItemsMessage.getType()).receive(item);
							}
							
							if (unrequested.isEmpty() == false)
								peer.disconnect("Received unrequested items "+unrequested+" of type "+inventoryItemsMessage.getType());
						}
						catch (Throwable t)
						{
							gossipLog.error(GossipHandler.this.context.getName()+": ledger.messages.gossip.inventory.items " + peer, t);
						}
						finally
						{
							GossipHandler.this.lock.writeLock().unlock();
						}
					}
				});
			}
		});

		this.context.getEvents().register(this.peerListener);

		Thread broadcastProcessorThread = new Thread(this.broadcastProcessor);
		broadcastProcessorThread.setDaemon(true);
		broadcastProcessorThread.setName(this.context.getName()+" Broadcast Processor");
		broadcastProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException
	{
		this.broadcastProcessor.terminate(true);
		this.context.getEvents().unregister(this.peerListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	public void register(final Class<? extends Primitive> type, final GossipFilter filter)
	{
		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(filter, "Filter is null");
		synchronized(this.broadcastFilters)
		{
			if (this.broadcastFilters.containsKey(type) == true)
				throw new IllegalStateException("Already exists a gossip filter for type "+type);
		
			this.broadcastFilters.put(type, filter);
		}
	}
	
	public void register(final Class<? extends Primitive> type, final GossipInventory inventory)
	{
		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(inventory, "Inventory is null");
		synchronized(this.broadcastFilters)
		{
			if (this.inventoryProcessors.containsKey(type) == true)
				throw new IllegalStateException("Already exists a inventory processors for type "+type);
			
			this.inventoryProcessors.put(type, inventory);
		}
	}

	public void register(final Class<? extends Primitive> type, final GossipFetcher fetcher)
	{
		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(fetcher, "Fetcher is null");
		synchronized(this.fetcherProcessors)
		{
			if (this.fetcherProcessors.containsKey(type) == true)
				throw new IllegalStateException("Already exists a fetcher processor for type "+type);
			
			this.fetcherProcessors.put(type, fetcher);
		}
	}

	public void register(final Class<? extends Primitive> type, final GossipReceiver receiver)
	{
		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(receiver, "Receiver is null");
		synchronized(this.receiverProcessors)
		{
			if (this.receiverProcessors.containsKey(type) == true)
				throw new IllegalStateException("Already exists a receiver processor for type "+type);
			
			this.receiverProcessors.put(type, receiver);
		}
	}

	public void broadcast(final Primitive object)
	{
		Objects.requireNonNull(object, "Object is null");
		
		if (Serialization.getInstance().getIdForClass(object.getClass()) == null)
			throw new IllegalArgumentException("Type "+object.getClass()+" is an unregistered class");
		
		this.toBroadcast.put(object.getClass(), new Broadcast(object));
		this.queued.release();
	}
	
	public void broadcast(Class<? extends Primitive> type, List<? extends Primitive> objects)
	{
		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(objects, "Objects is null");
		
		if (Serialization.getInstance().getIdForClass(type) == null)
			throw new IllegalArgumentException("Type "+type+" is an unregistered class");
		
		this.toBroadcast.putAll(type, objects.stream().map(i -> new Broadcast(i)).collect(Collectors.toList()));
		this.queued.release(objects.size());
	}
	
	public void broadcast(final Primitive object, Collection<Long> shardGroups)
	{
		Objects.requireNonNull(object, "Object is null");
		Objects.requireNonNull(shardGroups, "Shard groups is null");
		
		if (Serialization.getInstance().getIdForClass(object.getClass()) == null)
			throw new IllegalArgumentException("Type "+object.getClass()+" is an unregistered class");
		
		this.toBroadcast.put(object.getClass(), new Broadcast(object, shardGroups));
		this.queued.release();
	}

//	private Collection<Hash> request(final ConnectedPeer peer, final Collection<Hash> items, final Class<? extends Primitive> type, final boolean rerequest) throws IOException
	private Collection<Hash> request(final ConnectedPeer peer, final Collection<Hash> items, final Class<? extends Primitive> type) throws IOException
	{
		final List<Hash> itemsPending = new ArrayList<Hash>();
		final Map<Hash, Long> itemsToRequest = new HashMap<Hash, Long>();
			
		GossipHandler.this.lock.writeLock().lock();
		try
		{
			for (Hash item : items)
			{
				if (this.itemsRequested.containsKey(item) == true)
				{
					itemsPending.add(item);
				}
				else // if (this.context.getLedger().getLedgerStore().has(atom) == false)
				{
					itemsToRequest.put(item, ThreadLocalRandom.current().nextLong());
					itemsPending.add(item);
				}
			}

			if (itemsPending.isEmpty() == true)
			{
				gossipLog.warn(GossipHandler.this.context.getName()+": No items of type "+type+" required from "+peer);
				return Collections.emptyList();
			}
			
			if (itemsToRequest.isEmpty() == false)
			{
				GossipPeerTask gossipPeerTask = null;
				try
				{
					this.itemsRequested.putAll(itemsToRequest);
					
					if (gossipLog.hasLevel(Logging.DEBUG))
					{	
						itemsToRequest.forEach((i, n) -> {
							gossipLog.debug(GossipHandler.this.context.getName()+": Requesting item "+i+" of type "+type+" from "+peer);
						});
					}
	
					GetInventoryItemsMessage getInventoryItemsMessage = new GetInventoryItemsMessage(itemsToRequest.keySet(), type); 
					this.context.getNetwork().getMessaging().send(getInventoryItemsMessage, peer);
					
					gossipPeerTask = new GossipPeerTask(peer, itemsToRequest, type);//, rerequest);
					this.requestTasks.put(peer, gossipPeerTask);
					Executor.getInstance().schedule(gossipPeerTask);
					
					if (gossipLog.hasLevel(Logging.DEBUG))
						gossipLog.debug(GossipHandler.this.context.getName()+": Requesting "+getInventoryItemsMessage.getItems().size()+" items of type "+getInventoryItemsMessage.getType()+" from "+peer);
				}
				catch (Throwable t)
				{
					if (gossipPeerTask != null)
					{
						if (gossipPeerTask.cancel() == true)
							this.requestTasks.remove(peer, gossipPeerTask);
					}
					
					for (Hash itemToRequest : itemsToRequest.keySet())
						this.itemsRequested.remove(itemToRequest);
					
					throw t;
				}
			}
		}
		finally
		{
			GossipHandler.this.lock.writeLock().unlock();
		}
		
		return itemsPending;
	}
	
	private void received(final Primitive item)
	{
		Objects.requireNonNull(item, "Item is null");
		
		GossipHandler.this.lock.writeLock().lock();
		try
		{
			List<ConnectedPeer> peers = new ArrayList<ConnectedPeer>(this.itemInventories.keySet());
			for (ConnectedPeer peer : peers)
				this.itemInventories.remove(peer, item.getHash());
		}
		finally
		{
			GossipHandler.this.lock.writeLock().unlock();
		}
	}
	
	// PEER LISTENER //
	private EventListener peerListener = new EventListener()
	{
    	@Subscribe
		public void on(final PeerDisconnectedEvent event)
		{
   			GossipHandler.this.lock.writeLock().lock();
    		try
    		{
    			GossipHandler.this.itemInventories.removeAll(event.getPeer());
    			if (GossipHandler.this.requestTasks.containsKey(event.getPeer()) == false)
    				return;
    			
    			Collection<GossipPeerTask> requestTasks = new ArrayList<GossipPeerTask>(GossipHandler.this.requestTasks.get(event.getPeer()));
    			for (GossipPeerTask task : requestTasks)
    			{
    				try
    				{
    					if (task.isCancelled() == false)
    						task.cancel();

    					gossipLog.info(GossipHandler.this.context.getName()+": Cancelled gossip task of "+task.items.keySet()+" of type "+task.type+" from "+event.getPeer());
    				}
    	    		catch (Throwable t)
    	    		{
    	    			gossipLog.error(GossipHandler.this.context.getName()+": Failed to cancel gossip task of "+task.items.keySet()+" of type "+task.type+" from "+event.getPeer());
    	    		}
    			}
    			
    			GossipHandler.this.requestTasks.removeAll(event.getPeer());
    		}
    		finally
    		{
    			GossipHandler.this.lock.writeLock().unlock();
    		}
		}
	};
}
