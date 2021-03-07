package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.EventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.SyncBlockEvent;
import org.fuserleer.ledger.messages.GetAtomPoolInventoryMessage;
import org.fuserleer.ledger.messages.GetBlockPoolInventoryMessage;
import org.fuserleer.ledger.messages.GetStatePoolInventoryMessage;
import org.fuserleer.ledger.messages.GetSyncBlockInventoryMessage;
import org.fuserleer.ledger.messages.GetSyncBlockMessage;
import org.fuserleer.ledger.messages.InventoryMessage;
import org.fuserleer.ledger.messages.SyncBlockInventoryMessage;
import org.fuserleer.ledger.messages.SyncBlockMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.messages.NodeMessage;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.PeerTask;
import org.fuserleer.network.peers.events.PeerConnectedEvent;
import org.fuserleer.network.peers.events.PeerDisconnectedEvent;
import org.fuserleer.network.peers.filters.StandardPeerFilter;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.google.common.eventbus.Subscribe;
import com.google.common.primitives.Longs;

public class SyncHandler implements Service
{
	private static final Logger syncLog = Logging.getLogger("sync");
	
	private final Context context;
	private final Map<Hash, PendingAtom> atoms;
	private final Map<Hash, Block> blocks;
	private final Set<Hash> blocksRequested;
	private final TreeMultimap<ConnectedPeer, Hash> blockInventories;
	private final ReentrantLock lock = new ReentrantLock(true);

	private Executable syncProcessor = new Executable()
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
						Thread.sleep(TimeUnit.SECONDS.toMillis(1));
						
						long localShardGroup = ShardMapper.toShardGroup(SyncHandler.this.context.getNode().getIdentity(), SyncHandler.this.context.getLedger().numShardGroups());
						List<ConnectedPeer> syncPeers = SyncHandler.this.context.getNetwork().get(StandardPeerFilter.build(SyncHandler.this.context).setStates(PeerState.CONNECTED).
							    																  setShardGroup(localShardGroup));

						if (syncPeers.isEmpty() == true)
							continue;

						if (SyncHandler.this.context.getLedger().isSynced() == true)
							continue;

						SyncHandler.this.lock.lock();
						try
						{
							// Wait for block requests to finish or fail
							if (SyncHandler.this.blocksRequested.isEmpty() == false)
								continue;
							
							// Find committable block //
							Block bestBlock = null;
							Iterator<Block> blockIterator = SyncHandler.this.blocks.values().iterator(); 
							while(blockIterator.hasNext() == true)
							{
								Block block = blockIterator.next();
								if (block.getHeader().getHeight() <= SyncHandler.this.context.getLedger().getHead().getHeight())
								{
									blockIterator.remove();
									continue;
								}
								
								if (block.getHeader().getCertificate() == null)
								{
									syncLog.warn(SyncHandler.this.context.getName()+": Block "+block.getHeader()+" does not have a certificate!");
									continue;
								}
								
								long blockVotePower = SyncHandler.this.context.getLedger().getVoteRegulator().getVotePower(block.getHeader().getHeight() - VoteRegulator.VOTE_POWER_MATURITY, block.getHeader().getCertificate().getSigners());
								if (blockVotePower < SyncHandler.this.context.getLedger().getVoteRegulator().getVotePowerThreshold(block.getHeader().getHeight() - VoteRegulator.VOTE_POWER_MATURITY, Collections.singleton(localShardGroup)))
									continue;
								
								if (bestBlock == null || 
									bestBlock.getHeader().getAverageStep() < block.getHeader().getAverageStep() ||
									(bestBlock.getHeader().getAverageStep() == block.getHeader().getAverageStep() && bestBlock.getHeader().getStep() < block.getHeader().getStep()))
									bestBlock = block;
							}
							
							if (bestBlock != null)
							{
								syncLog.info(SyncHandler.this.context.getName()+": Selected block "+bestBlock.getHeader()+" as sync candidate");
								
								// Ensure there is a branch to commit 
								LinkedList<Block> bestBranch = new LinkedList<Block>();
								Block currentBranchBlock = bestBlock;
								
								do
								{
									bestBranch.add(currentBranchBlock);
									if (currentBranchBlock.getHeader().getPrevious().equals(SyncHandler.this.context.getLedger().getHead().getHash()) == true)
										break;
									
									currentBranchBlock = SyncHandler.this.blocks.get(currentBranchBlock.getHeader().getPrevious());
								}
								while(currentBranchBlock != null);
								
								if (bestBranch.getLast().getHeader().getPrevious().equals(SyncHandler.this.context.getLedger().getHead().getHash()) == false)
									syncLog.error(SyncHandler.this.context.getName()+": Selected branch terminating with block "+bestBranch.getLast().getHeader()+" does not link to "+SyncHandler.this.context.getLedger().getHead());
								else
								{
									try
									{
										commit(bestBranch);
									}
									catch (Exception ex)
									{
										// Dump the branch on any exception
										for (Block block : bestBranch)
											SyncHandler.this.blocks.remove(block.getHeader().getHash());

										syncLog.error(SyncHandler.this.context.getName()+": Commit of branch with head "+bestBranch.getFirst().getHeader()+" failed", ex);
									}
								}
							}
	
							// Is local replica now synced?
							if(SyncHandler.this.context.getLedger().isSynced(true) == true)
							{
								synchronized(SyncHandler.this.atoms)
								{
									// Inject remaining atoms that may be in a pending consensus phase
									for (PendingAtom pendingAtom : SyncHandler.this.atoms.values())
									{
										SyncHandler.this.context.getLedger().getAtomHandler().inject(pendingAtom);
										SyncHandler.this.context.getLedger().getStateHandler().queue(pendingAtom);
									}

									SyncHandler.this.atoms.clear();
								}
								
								// Tell all sync peers we're synced
								NodeMessage nodeMessage = new NodeMessage(SyncHandler.this.context.getNode());
								for (ConnectedPeer syncPeer : syncPeers)
								{
									try
									{
										SyncHandler.this.context.getNetwork().getMessaging().send(nodeMessage, syncPeer);
									}
									catch (IOException ioex)
									{
										syncLog.error("Could not send synced declaration to "+syncPeer, ioex);
									}
								}
								
								// Requests for current block pool, atom pool and state consensus primitives
								ConnectedPeer blockPoolInventoryPeer = syncPeers.get(ThreadLocalRandom.current().nextInt(syncPeers.size()));
								try
								{
									SyncHandler.this.context.getNetwork().getMessaging().send(new GetBlockPoolInventoryMessage(SyncHandler.this.context.getLedger().getHead()), blockPoolInventoryPeer);
								}
								catch (Exception ex)
								{
									syncLog.error(SyncHandler.this.context.getName()+": Unable to send GetBlockPoolInventoryMessage from "+SyncHandler.this.context.getLedger().getHead().getHash()+" to "+blockPoolInventoryPeer, ex);
								}

								ConnectedPeer atomPoolInventoryPeer = syncPeers.get(ThreadLocalRandom.current().nextInt(syncPeers.size()));
								try
								{
									SyncHandler.this.context.getNetwork().getMessaging().send(new GetAtomPoolInventoryMessage(SyncHandler.this.context.getLedger().getHead()), atomPoolInventoryPeer);
								}
								catch (Exception ex)
								{
									syncLog.error(SyncHandler.this.context.getName()+": Unable to send GetAtomPoolInventoryMessage from "+SyncHandler.this.context.getLedger().getHead().getHash()+" to "+blockPoolInventoryPeer, ex);
								}

								ConnectedPeer statePoolInventoryPeer = syncPeers.get(ThreadLocalRandom.current().nextInt(syncPeers.size()));
								try
								{
									SyncHandler.this.context.getNetwork().getMessaging().send(new GetStatePoolInventoryMessage(SyncHandler.this.context.getLedger().getHead()), statePoolInventoryPeer);
								}
								catch (Exception ex)
								{
									syncLog.error(SyncHandler.this.context.getName()+": Unable to send GetStatePoolInventoryMessage from "+SyncHandler.this.context.getLedger().getHead().getHash()+" to "+blockPoolInventoryPeer, ex);
								}

							}
							else
							{
								// Clean out inventories that are committed and maybe ask for some blocks //
								for (ConnectedPeer syncPeer : syncPeers)
								{
									try
									{
										if (SyncHandler.this.blockInventories.containsKey(syncPeer) == false)
											continue;
										
										Iterator<Hash> inventoryIterator = SyncHandler.this.blockInventories.get(syncPeer).iterator();
										while(inventoryIterator.hasNext() == true)
										{
											Hash block = inventoryIterator.next();
											if (SyncHandler.this.blocks.containsKey(block) == true || 
												SyncHandler.this.context.getLedger().getLedgerStore().has(new StateAddress(Block.class, block)) == CommitStatus.COMMITTED || 
												Longs.fromByteArray(block.toByteArray()) <= SyncHandler.this.context.getLedger().getHead().getHeight())
												inventoryIterator.remove();
										}
									}
									catch (Exception ex)
									{
										syncLog.error(SyncHandler.this.context.getName()+": Inventory management of "+syncPeer+" failed", ex);
									}
	
									try
									{
										if (SyncHandler.this.blockInventories.containsKey(syncPeer) == true)
											SyncHandler.this.requestBlocks(syncPeer, SyncHandler.this.blockInventories.get(syncPeer));
									}
									catch (Exception ex)
									{
										syncLog.error(SyncHandler.this.context.getName()+": Block request from "+syncPeer+" failed", ex);
									}
								}
								
								// Fetch some inventory? //
								for (ConnectedPeer syncPeer : syncPeers)
								{
									if (SyncHandler.this.blockInventories.containsKey(syncPeer) == false || 
										SyncHandler.this.blockInventories.get(syncPeer).isEmpty() == true)
									{
										try
										{
											SyncHandler.this.context.getNetwork().getMessaging().send(new GetSyncBlockInventoryMessage(SyncHandler.this.context.getLedger().getHead().getHash()), syncPeer);
										}
										catch (Exception ex)
										{
											syncLog.error(SyncHandler.this.context.getName()+": Unable to send GetSyncBlockInventoryMessage from "+SyncHandler.this.context.getLedger().getHead().getHash()+" to "+syncPeer, ex);
										}
									}
								}
							}
						}
						finally
						{
							SyncHandler.this.lock.unlock();
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
				syncLog.fatal(SyncHandler.this.context.getName()+": Error processing sync", throwable);
			}
		}
	};

	SyncHandler(Context context)
	{
		this.context = Objects.requireNonNull(context);
		this.blocksRequested = Collections.synchronizedSet(new HashSet<Hash>());
		this.blockInventories = TreeMultimap.create(Ordering.natural(), Collections.reverseOrder());
		this.blocks = Collections.synchronizedMap(new HashMap<Hash, Block>());
		this.atoms = Collections.synchronizedMap(new HashMap<Hash, PendingAtom>());
	}

	@Override
	public void start() throws StartupException
	{
		this.context.getNetwork().getMessaging().register(SyncBlockMessage.class, this.getClass(), new MessageProcessor<SyncBlockMessage>()
		{
			@Override
			public void process(final SyncBlockMessage syncBlockMessage, final ConnectedPeer peer)
			{
				SyncHandler.this.lock.lock();
				try
				{
					if (syncBlockMessage.getBlock().getHeader().getHeight() <= SyncHandler.this.context.getLedger().getHead().getHeight())
					{
						syncLog.warn(SyncHandler.this.context.getName()+": Block is old "+syncBlockMessage.getBlock().getHeader()+" from "+peer);
						return;
					}

					if (SyncHandler.this.context.getLedger().getLedgerStore().has(new StateAddress(Block.class, syncBlockMessage.getBlock().getHeader().getHash())) == CommitStatus.COMMITTED)
					{
						syncLog.warn(SyncHandler.this.context.getName()+": Block is committed "+syncBlockMessage.getBlock().getHeader()+" from "+peer);
						return;
					}

					if (SyncHandler.this.context.getLedger().getLedgerStore().has(syncBlockMessage.getBlock().getHeader().getHash()) == false)
					{
						if (syncLog.hasLevel(Logging.DEBUG) == true)
							syncLog.debug(SyncHandler.this.context.getName()+": Block "+syncBlockMessage.getBlock().getHeader().getHash()+" from "+peer);

						// TODO need a block validation / verification processor

						SyncHandler.this.context.getLedger().getLedgerStore().store(syncBlockMessage.getBlock());
						SyncHandler.this.blocks.put(syncBlockMessage.getBlock().getHash(), syncBlockMessage.getBlock());
						SyncHandler.this.blocksRequested.remove(syncBlockMessage.getBlock().getHash());
					}
				}
				catch (Exception ex)
				{
					syncLog.error(SyncHandler.this.context.getName()+": ledger.messages.block.sync "+peer, ex);
				}
				finally
				{
					SyncHandler.this.lock.unlock();
				}
			}
		});
		
		this.context.getNetwork().getMessaging().register(GetSyncBlockMessage.class, this.getClass(), new MessageProcessor<GetSyncBlockMessage>()
		{
			@Override
			public void process(final GetSyncBlockMessage getSyncBlockMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						SyncHandler.this.lock.lock();
						try
						{
							if (syncLog.hasLevel(Logging.DEBUG) == true)
								syncLog.debug(SyncHandler.this.context.getName()+": Block request for "+getSyncBlockMessage.getInventory().size()+" from "+peer);
							
							for (Hash blockHash : getSyncBlockMessage.getInventory())
							{
								Block block = SyncHandler.this.context.getLedger().getLedgerStore().get(blockHash, Block.class);
								if (block == null)
								{
									if (syncLog.hasLevel(Logging.DEBUG) == true)
										syncLog.error(SyncHandler.this.context.getName()+": Requested block "+blockHash+" not found for "+peer);
									
									// TODO disconnect and ban?  Asking for blocks we don't have
									return;
								}
							
								try
								{
									SyncHandler.this.context.getNetwork().getMessaging().send(new SyncBlockMessage(block), peer);
								}
								catch (IOException ex)
								{
									syncLog.error(SyncHandler.this.context.getName()+": Unable to send SyncBlockMessage for "+blockHash+" to "+peer, ex);
									break;
								}
							}
						}
						catch (Exception ex)
						{
							syncLog.error(SyncHandler.this.context.getName()+": ledger.messages.block.sync.get " + peer, ex);
						}
						finally
						{
							SyncHandler.this.lock.unlock();
						}
					}
				});
			}
		});
		
		this.context.getNetwork().getMessaging().register(GetSyncBlockInventoryMessage.class, this.getClass(), new MessageProcessor<GetSyncBlockInventoryMessage>()
		{
			@Override
			public void process(final GetSyncBlockInventoryMessage getSyncBlockInventoryMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						SyncHandler.this.lock.lock();
						try
						{
							if (syncLog.hasLevel(Logging.DEBUG) == true)
								syncLog.debug(SyncHandler.this.context.getName()+": Block inventory request from "+getSyncBlockInventoryMessage.getHead()+" from "+peer);

							List<Hash> inventory = new ArrayList<Hash>();
							Hash current = getSyncBlockInventoryMessage.getHead();
							do
							{
								Hash next = SyncHandler.this.context.getLedger().getLedgerStore().get(Longs.fromByteArray(current.toByteArray())+1);
								if (next != null)
									inventory.add(next);
								
								current = next;
							}
							while(current != null && inventory.size() < InventoryMessage.MAX_INVENTORY);

							if (inventory.isEmpty() == false)
							{
								try
								{
									SyncHandler.this.context.getNetwork().getMessaging().send(new SyncBlockInventoryMessage(inventory), peer);
								}
								catch (IOException ex)
								{
									syncLog.error(SyncHandler.this.context.getName()+": Unable to send SyncBlockInventoryMessage from "+getSyncBlockInventoryMessage.getHead()+" for "+inventory.size()+" blocks to "+peer, ex);
								}
							}
						}
						catch (Exception ex)
						{
							syncLog.error(SyncHandler.this.context.getName()+": ledger.messages.block.sync.inv.get " + peer, ex);
						}
						finally
						{
							SyncHandler.this.lock.unlock();
						}
					}
				});
			}
		});
		
		this.context.getNetwork().getMessaging().register(SyncBlockInventoryMessage.class, this.getClass(), new MessageProcessor<SyncBlockInventoryMessage>()
		{
			@Override
			public void process(final SyncBlockInventoryMessage syncBlockInventoryMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						SyncHandler.this.lock.lock();
						try
						{
							if (syncLog.hasLevel(Logging.DEBUG) == true)
								syncLog.debug(SyncHandler.this.context.getName()+": Block inventory for "+syncBlockInventoryMessage.getInventory().size()+" headers from " + peer);

							if (syncBlockInventoryMessage.getInventory().size() == 0)
							{
								syncLog.error(SyncHandler.this.context.getName()+": Received empty block inventory from " + peer);
								return;
							}
							
							synchronized(SyncHandler.this.blockInventories)
							{
								SyncHandler.this.blockInventories.putAll(peer, syncBlockInventoryMessage.getInventory());
							}
						}
						catch (Exception ex)
						{
							syncLog.error(SyncHandler.this.context.getName()+": ledger.messages.block.header.inv "+peer, ex);
						}
						finally
						{
							SyncHandler.this.lock.unlock();
						}
					}
				});
			}
		});
		
		this.context.getEvents().register(this.peerListener);
		
		Thread syncProcessorThread = new Thread(this.syncProcessor);
		syncProcessorThread.setDaemon(true);
		syncProcessorThread.setName(this.context.getName()+" Sync Processor");
		syncProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException
	{
		this.syncProcessor.terminate(true);
		this.context.getEvents().unregister(this.peerListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	@SuppressWarnings("unchecked")
	Collection<Hash> requestBlocks(final ConnectedPeer peer, final Collection<Hash> blocks) throws IOException
	{
		this.lock.lock();
		try
		{
			final List<Hash> blocksPending = new ArrayList<Hash>();
			final List<Hash> blocksToRequest = new ArrayList<Hash>();
				
			for (Hash block : blocks)
			{
				if (this.blocksRequested.contains(block) == true)
					blocksPending.add(block);
				else if (SyncHandler.this.context.getLedger().getLedgerStore().has(block) == false)
				{
					blocksToRequest.add(block);
					blocksPending.add(block);
				}
				else 
					SyncHandler.this.blocks.put(block, SyncHandler.this.context.getLedger().getLedgerStore().get(block, Block.class));
			}

			if (blocksPending.isEmpty() == true)
			{
				syncLog.warn(SyncHandler.this.context.getName()+": No blocks required from "+peer);
				return Collections.EMPTY_LIST;
			}
			
			if (blocksToRequest.isEmpty() == false)
			{
				try
				{
					this.blocksRequested.addAll(blocksToRequest);
					
					if (syncLog.hasLevel(Logging.DEBUG))
					{	
						blocksToRequest.forEach(b -> {
							syncLog.debug(SyncHandler.this.context.getName()+": Requesting block " + b + " from " + peer);
						});
					}
	
					GetSyncBlockMessage getBlockMessage = new GetSyncBlockMessage(blocksToRequest); 
					this.context.getNetwork().getMessaging().send(getBlockMessage, peer);
					
					Executor.getInstance().schedule(new PeerTask(peer, 10, TimeUnit.SECONDS) 
					{
						final Collection<Hash> requestedBlocks = new ArrayList<Hash>(blocksToRequest);
						
						@Override
						public void execute()
						{
							List<Hash> failedBlockRequests = new ArrayList<Hash>();
							synchronized(SyncHandler.this.blocksRequested)
							{
								for (Hash requestedBlock : this.requestedBlocks)
								{
									if (SyncHandler.this.blocksRequested.remove(requestedBlock) == true)
										failedBlockRequests.add(requestedBlock);
								}
							}
							
							if (failedBlockRequests.isEmpty() == false)
							{
								if (getPeer().getState().equals(PeerState.CONNECTED) || getPeer().getState().equals(PeerState.CONNECTING))
								{
									syncLog.error(SyncHandler.this.context.getName()+": "+getPeer()+" did not respond to block request "+this.requestedBlocks);
									getPeer().disconnect("Did not respond to sync block request of "+this.requestedBlocks);
								}
							}
						}
					});

					if (syncLog.hasLevel(Logging.DEBUG))
						syncLog.debug(SyncHandler.this.context.getName()+": Requesting "+getBlockMessage.getInventory()+" blocks from "+peer);
				}
				catch (Throwable t)
				{
					this.blocksRequested.removeAll(blocksToRequest);
					throw t;
				}
			}
			
			return blocksPending;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	void commit(LinkedList<Block> branch) throws IOException, ValidationException, StateLockedException
	{
		// TODO pretty much everything, limited validation here currently, just accepts blocks and atoms almost on faith
		List<Block> committedBlocks = new ArrayList<Block>();
		Iterator<Block> blockIterator = branch.descendingIterator();
		while(blockIterator.hasNext() == true)
		{
			Block block = blockIterator.next();
			this.context.getLedger().getLedgerStore().commit(block);
			committedBlocks.add(block);
			
			for (Atom atom : block.getAtoms())
			{
				if (this.atoms.containsKey(atom.getHash()) == true)
					throw new ValidationException(this.context.getName()+": Atom "+atom.getHash()+" in block "+block.getHash()+" is already pending!");
				
				PendingAtom pendingAtom = PendingAtom.create(this.context, atom);
				this.atoms.put(pendingAtom.getHash(), pendingAtom);
				pendingAtom.prepare();
				this.context.getLedger().getStateAccumulator().lock(pendingAtom);
				pendingAtom.provision(block.getHeader());
			}

			for (AtomCertificate certificate : block.getCertificates())
			{
				PendingAtom pendingAtom = this.atoms.get(certificate.getAtom());
				if (pendingAtom == null)
					throw new ValidationException(this.context.getName()+": Pending atom "+certificate.getAtom()+" not found for certificate "+certificate.getHash()+" in block "+block.getHash());
				pendingAtom.setCertificate(certificate);
				this.context.getLedger().getStateAccumulator().commit(pendingAtom);
				this.atoms.remove(pendingAtom.getHash());
			}
		}
		
		for (Block committedBlock : committedBlocks)
		{
			SyncBlockEvent syncBlockEvent = new SyncBlockEvent(committedBlock);
			SyncHandler.this.context.getEvents().post(syncBlockEvent); // TODO Might need to catch exceptions on this from synchronous listeners
		}
	}
	
    // PEER LISTENER //
    private EventListener peerListener = new EventListener()
    {
    	@Subscribe
		public void on(PeerConnectedEvent event)
		{
		}

    	@Subscribe
		public void on(PeerDisconnectedEvent event)
		{
			synchronized(SyncHandler.this.blockInventories)
			{
				SyncHandler.this.blockInventories.removeAll(event.getPeer());
			}
		}
    };
}
