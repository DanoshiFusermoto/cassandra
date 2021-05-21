package org.fuserleer.ledger;

import java.io.IOException;
import java.util.AbstractMap;
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
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.Universe;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.EventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.SyncBlockEvent;
import org.fuserleer.ledger.events.SyncStatusChangeEvent;
import org.fuserleer.ledger.messages.GetSyncBlockInventoryMessage;
import org.fuserleer.ledger.messages.GetSyncBlockMessage;
import org.fuserleer.ledger.messages.InventoryMessage;
import org.fuserleer.ledger.messages.SyncAcquiredMessage;
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
import org.fuserleer.node.Node;

import com.google.common.collect.Ordering;
import com.google.common.collect.TreeMultimap;
import com.google.common.eventbus.Subscribe;
import com.google.common.primitives.Longs;

public class SyncHandler implements Service
{
	private static final Logger syncLog = Logging.getLogger("sync");
	
	private final class SyncRequestPeerTask extends PeerTask 
	{
		final Entry<Hash, Long> requestedBlock;
		
		SyncRequestPeerTask(final ConnectedPeer peer, final Entry<Hash, Long> block)
		{
			super(peer, 10, TimeUnit.SECONDS);
			
			this.requestedBlock = block;
		}
		
		@Override
		public void execute()
		{
			boolean failedRequest = false;
			
			SyncHandler.this.lock.lock();
			try
			{
				SyncHandler.this.requestTasks.remove(getPeer(), this);
				if (SyncHandler.this.blocksRequested.remove(this.requestedBlock.getKey(), this.requestedBlock.getValue()) == true)
					failedRequest = true;
			}
			finally
			{
				SyncHandler.this.lock.unlock();
			}
			
			// Can do the disconnect and re-request outside of the lock
			if (failedRequest == true)
			{
				try
				{
					syncLog.error(SyncHandler.this.context.getName()+": "+getPeer()+" did not respond to block request of "+this.requestedBlock.getKey());
					if (getPeer().getState().equals(PeerState.CONNECTED) || getPeer().getState().equals(PeerState.CONNECTING))
						getPeer().disconnect("Did not respond to block request of "+this.requestedBlock.getKey());
				}
				catch (Throwable t)
				{
					syncLog.error(SyncHandler.this.context.getName()+": "+getPeer(), t);
				}
				
				rerequest(this.requestedBlock.getKey());
			}
		}

		public int remaining()
		{
			SyncHandler.this.lock.lock();
			try
			{
				if (SyncHandler.this.blocksRequested.get(this.requestedBlock.getKey()) == this.requestedBlock.getValue())
					return 1;

				return 0;
			}
			finally
			{
				SyncHandler.this.lock.unlock();
			}
		}

		@Override
		public void cancelled()
		{
			boolean failedRequest = false;

			SyncHandler.this.lock.lock();
			try
			{
				SyncHandler.this.requestTasks.remove(getPeer(), this);
				if (SyncHandler.this.blocksRequested.remove(this.requestedBlock.getKey(), this.requestedBlock.getValue()) == true)
					failedRequest = true;
				
				if (failedRequest == true)
					rerequest(this.requestedBlock.getKey());
			}
			finally
			{
				SyncHandler.this.lock.unlock();
			}
		}
		
		private void rerequest(final Hash block)
		{
			// Build the re-requests
			long rerequestShardGroup = ShardMapper.toShardGroup(getPeer().getNode().getIdentity(), SyncHandler.this.context.getLedger().numShardGroups());
			StandardPeerFilter standardPeerFilter = StandardPeerFilter.build(SyncHandler.this.context).setStates(PeerState.CONNECTED).setShardGroup(rerequestShardGroup);
			List<ConnectedPeer> rerequestConnectedPeers = SyncHandler.this.context.getNetwork().get(standardPeerFilter);
			if (rerequestConnectedPeers.isEmpty() == false)
			{
				ConnectedPeer rerequestPeer = rerequestConnectedPeers.get(0);
				try
				{
					SyncHandler.this.request(rerequestPeer, block);
				}
				catch (IOException ioex)
				{
					syncLog.error(SyncHandler.this.context.getName()+": Failed to re-request "+blocks+" blocks from "+rerequestPeer, ioex);
				}
			}
			else
				syncLog.error(SyncHandler.this.context.getName()+": Unable to re-request "+blocks.size()+" blocks");
		}
	}

	private final class SyncInventoryPeerTask extends PeerTask 
	{
		final Hash block;
		final long requestSeq;
		
		SyncInventoryPeerTask(final ConnectedPeer peer, final Hash block, final long requestSeq)
		{
			super(peer, 10, TimeUnit.SECONDS);
			
			this.requestSeq = requestSeq;
			this.block = block;
		}
		
		@Override
		public void execute()
		{
			SyncHandler.this.lock.lock();
			try
			{
				SyncInventoryPeerTask currentTask = SyncHandler.this.inventoryTasks.get(getPeer());
				if (currentTask == null)
					return;
				
				if (currentTask.requestSeq == this.requestSeq)
				{
					SyncHandler.this.inventoryTasks.remove(getPeer(), this);

					syncLog.error(SyncHandler.this.context.getName()+": "+getPeer()+" did not respond to inventory request "+this.requestSeq+":"+this.block);
					if (getPeer().getState().equals(PeerState.CONNECTED) || getPeer().getState().equals(PeerState.CONNECTING))
						getPeer().disconnect("Did not respond to inventory request "+this.requestSeq+":"+this.block);
				}
			}
			catch (Throwable t)
			{
				syncLog.error(SyncHandler.this.context.getName()+": "+getPeer(), t);
			}
			finally
			{
				SyncHandler.this.lock.unlock();
			}
		}

		@Override
		public void cancelled()
		{
			SyncHandler.this.lock.lock();
			try
			{
				SyncHandler.this.requestTasks.remove(getPeer(), this);
			}
			finally
			{
				SyncHandler.this.lock.unlock();
			}
		}
	}

	private final Context context;
	private final Map<Hash, PendingAtom> 	atoms;
	private final Map<Hash, Block> 			blocks;
	private final Map<Hash, Long> 			blocksRequested;
	private final Map<ConnectedPeer, SyncRequestPeerTask> requestTasks;
	private final Map<ConnectedPeer, SyncInventoryPeerTask> inventoryTasks;
	private final TreeMultimap<ConnectedPeer, Hash> blockInventories;
	private boolean synced;

	private final ReentrantLock lock = new ReentrantLock(true);

	private Executable syncProcessor = new Executable()
	{
		@Override
		public void execute()
		{
			long delay = 1000;
			try 
			{
				while (this.isTerminated() == false)
				{
					try
					{
						Thread.sleep(delay);
						
						long localShardGroup = ShardMapper.toShardGroup(SyncHandler.this.context.getNode().getIdentity(), SyncHandler.this.context.getLedger().numShardGroups());
						StandardPeerFilter standardPeerFilter = StandardPeerFilter.build(SyncHandler.this.context).setStates(PeerState.CONNECTED).setShardGroup(localShardGroup);
						List<ConnectedPeer> syncPeers = SyncHandler.this.context.getNetwork().get(standardPeerFilter);
						if (syncPeers.isEmpty() == true)
							continue;
						
						boolean haveShardGroupCoverage = true;
						// TODO remove this once sync is stable, adding explicit shard peer checks to relieve a concern and test an assumption of the fault
						for (long sg = 0 ; sg < SyncHandler.this.context.getLedger().numShardGroups(SyncHandler.this.context.getLedger().getHead().getHeight()) ; sg++)
						{
							long shardGroup = sg;
							if (shardGroup == localShardGroup)
								continue;
							
							List<ConnectedPeer> shardPeers = SyncHandler.this.context.getNetwork().get(StandardPeerFilter.build(SyncHandler.this.context).setStates(PeerState.CONNECTED).
									  															  	   setShardGroup(shardGroup));
							if (shardPeers.isEmpty() == true)
							{
								haveShardGroupCoverage = false;
								continue;
							}
						}
						
						if (haveShardGroupCoverage == false)
							continue;
						
						if (SyncHandler.this.checkSynced() == true)
							continue;

						SyncHandler.this.lock.lock();
						try
						{
							// Wait for block requests to finish or fail
							if (SyncHandler.this.blocksRequested.isEmpty() == false)
								continue;
							
							// Find best committable branch //
							SyncBranch bestBranch = null;
							double bestBranchVoteRatio = 0;
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
								
								if (block.getHeader().getCertificate().getSigners() == null ||
									block.getHeader().getCertificate().getSigners().count() == 0)
								{
									syncLog.warn(SyncHandler.this.context.getName()+": Block "+block.getHeader()+" has an empty certificate!");
									continue;
								}

								SyncBranch syncBranch = new SyncBranch(SyncHandler.this.context, SyncHandler.this.context.getLedger().getHead());
								syncBranch.push(block.getHeader());
								Block branchBlock = block;
								Block prevBranchBlock = null;
								while(branchBlock.getHeader().getPrevious().equals(syncBranch.getRoot().getHash()) == false)
								{
									prevBranchBlock = SyncHandler.this.blocks.get(branchBlock.getHeader().getPrevious());
									if (prevBranchBlock == null)
									{
										if (syncLog.hasLevel(Logging.DEBUG) == true)
											syncLog.debug(SyncHandler.this.context.getName()+": Previous sync block "+branchBlock.getHeader().getPrevious()+" of "+branchBlock.getHeader().toString()+" is not known");
										
										break;
									}
									
									syncBranch.push(prevBranchBlock.getHeader());
									branchBlock = prevBranchBlock;
								}
								
								if (prevBranchBlock == null)
									continue;
								
								long blockVotePower = syncBranch.getVotePower(syncBranch.getHigh().getHeight(), syncBranch.getHigh().getCertificate().getSigners());
								long blockVotePowerThreshold = syncBranch.getVotePowerThreshold(syncBranch.getHigh().getHeight());
								double blockVotePowerRatio = blockVotePower / (double) blockVotePowerThreshold;
								if (blockVotePower < blockVotePowerThreshold)
									continue;
								
								if (blockVotePowerRatio < bestBranchVoteRatio)
									continue;
								
								if (bestBranch == null || bestBranchVoteRatio < blockVotePowerRatio)
								{
									bestBranchVoteRatio = blockVotePowerRatio;
									bestBranch = syncBranch;
								}
							}
							
							if (bestBranch != null)
							{
								syncLog.info(SyncHandler.this.context.getName()+": Selected block "+bestBranch.getHigh()+" as sync candidate");
								
								// Ensure there is a branch to commit
								if (bestBranch.isCanonical() == false)
									syncLog.error(SyncHandler.this.context.getName()+": Selected branch terminating with block "+bestBranch.getHigh()+" does not link to "+SyncHandler.this.context.getLedger().getHead());
								else
								{
									try
									{
										commit(bestBranch);
									}
									catch (Exception ex)
									{
										// Dump the branch on any exception
										for (BlockHeader header : bestBranch.getHeaders())
											SyncHandler.this.blocks.remove(header.getHash());

										syncLog.error(SyncHandler.this.context.getName()+": Commit of branch with head "+bestBranch.getHigh()+" failed", ex);
									}
								}
								
								delay = Math.max(50, delay/2);
							}
							else 
								delay = Math.min(1000, delay*2);

							// Clean out inventories that are committed and maybe ask for some blocks //
							for (ConnectedPeer syncPeer : syncPeers)
							{
								try
								{
									if (SyncHandler.this.blockInventories.containsKey(syncPeer) == false)
										continue;
									
									SyncRequestPeerTask task = SyncHandler.this.requestTasks.get(syncPeer);
									if (task != null && task.remaining() > 0)
										continue;

									Iterator<Hash> inventoryIterator = SyncHandler.this.blockInventories.get(syncPeer).iterator();
									while(inventoryIterator.hasNext() == true)
									{
										Hash block = inventoryIterator.next();
										if (SyncHandler.this.blocks.containsKey(block) == true ||
											Longs.fromByteArray(block.toByteArray()) <= SyncHandler.this.context.getLedger().getHead().getHeight() || 
											SyncHandler.this.context.getLedger().getLedgerStore().has(new StateAddress(Block.class, block)) == CommitStatus.COMPLETED) 
											inventoryIterator.remove();
									}
								}
								catch (Exception ex)
								{
									syncLog.error(SyncHandler.this.context.getName()+": Inventory management of "+syncPeer+" failed", ex);
								}

								try
								{
									NavigableSet<Hash> syncPeerInventory = SyncHandler.this.blockInventories.get(syncPeer);
									if (syncPeerInventory != null && syncPeerInventory.isEmpty() == false)
									{
										for (Hash block : syncPeerInventory)
										{
											if (SyncHandler.this.blocksRequested.containsKey(block) == false && 
												SyncHandler.this.request(syncPeer, block) == true)
												break;
										}
									}
								}
								catch (Exception ex)
								{
									syncLog.error(SyncHandler.this.context.getName()+": Block request from "+syncPeer+" failed", ex);
								}
							}
							
							// Fetch some inventory? //
							for (ConnectedPeer syncPeer : syncPeers)
							{
								if (SyncHandler.this.context.getNode().isAheadOf(syncPeer.getNode(), 1) == true)
									continue;
								
								if (SyncHandler.this.inventoryTasks.containsKey(syncPeer) == false &&
									(SyncHandler.this.blockInventories.containsKey(syncPeer) == false || 
									 SyncHandler.this.blockInventories.get(syncPeer).isEmpty() == true || bestBranch == null))
								{
									SyncInventoryPeerTask syncInventoryPeerTask = null;
									Hash highestInventory = SyncHandler.this.context.getLedger().getHead().getHash();
									if (SyncHandler.this.blockInventories.containsKey(syncPeer) == true && SyncHandler.this.blockInventories.get(syncPeer).isEmpty() == false) 
										highestInventory = SyncHandler.this.blockInventories.get(syncPeer).first();

									try
									{
										GetSyncBlockInventoryMessage getSyncBlockInventoryMessage = new GetSyncBlockInventoryMessage(highestInventory);
										
										syncInventoryPeerTask = new SyncInventoryPeerTask(syncPeer, highestInventory, getSyncBlockInventoryMessage.getSeq());
										SyncHandler.this.inventoryTasks.put(syncPeer, syncInventoryPeerTask);
										Executor.getInstance().schedule(syncInventoryPeerTask);
										
										SyncHandler.this.context.getNetwork().getMessaging().send(getSyncBlockInventoryMessage, syncPeer);
										syncLog.info(SyncHandler.this.context.getName()+": Requested block inventory "+getSyncBlockInventoryMessage.getSeq()+":"+highestInventory+" from "+syncPeer);
									}
									catch (Exception ex)
									{
										if (syncInventoryPeerTask != null)
											syncInventoryPeerTask.cancel();
										
										syncLog.error(SyncHandler.this.context.getName()+": Unable to send GetSyncBlockInventoryMessage from "+highestInventory+" to "+syncPeer, ex);
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
		this.blocksRequested = Collections.synchronizedMap(new HashMap<Hash, Long>());
		this.requestTasks = Collections.synchronizedMap(new HashMap<ConnectedPeer, SyncRequestPeerTask>());
		this.inventoryTasks = Collections.synchronizedMap(new HashMap<ConnectedPeer, SyncInventoryPeerTask>());
		this.blockInventories = TreeMultimap.create(Ordering.natural(), Ordering.natural());
		this.blocks = Collections.synchronizedMap(new HashMap<Hash, Block>());
		this.atoms = Collections.synchronizedMap(new HashMap<Hash, PendingAtom>());
	}

	@Override
	public void start() throws StartupException
	{
		try
		{
			this.synced = false;
			
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
	
						if (SyncHandler.this.context.getLedger().getLedgerStore().has(new StateAddress(Block.class, syncBlockMessage.getBlock().getHeader().getHash())) == CommitStatus.COMPLETED)
						{
							syncLog.warn(SyncHandler.this.context.getName()+": Block is committed "+syncBlockMessage.getBlock().getHeader()+" from "+peer);
							return;
						}
	
						if (syncLog.hasLevel(Logging.DEBUG) == true)
							syncLog.debug(SyncHandler.this.context.getName()+": Block "+syncBlockMessage.getBlock().getHeader().getHash()+" from "+peer);
	
						// TODO need a block validation / verification processor
	
						SyncHandler.this.context.getLedger().getLedgerStore().store(SyncHandler.this.context.getLedger().getHead().getHeight(), syncBlockMessage.getBlock());
						SyncHandler.this.blocks.put(syncBlockMessage.getBlock().getHash(), syncBlockMessage.getBlock());
						SyncHandler.this.blocksRequested.remove(syncBlockMessage.getBlock().getHash());
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
									syncLog.debug(SyncHandler.this.context.getName()+": Block request for "+getSyncBlockMessage.getBlock()+" from "+peer);
								
								Block block = SyncHandler.this.context.getLedger().getLedgerStore().get(getSyncBlockMessage.getBlock(), Block.class);
								if (block == null)
								{
									if (syncLog.hasLevel(Logging.DEBUG) == true)
										syncLog.error(SyncHandler.this.context.getName()+": Requested block "+getSyncBlockMessage.getBlock()+" not found for "+peer);
										
									// TODO disconnect and ban?  Asking for blocks we don't have
									return;
								}
								
								try
								{
									SyncHandler.this.context.getNetwork().getMessaging().send(new SyncBlockMessage(block), peer);
								}
								catch (IOException ex)
								{
									syncLog.error(SyncHandler.this.context.getName()+": Unable to send SyncBlockMessage for "+getSyncBlockMessage.getBlock()+" to "+peer, ex);
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
									Hash next = SyncHandler.this.context.getLedger().getLedgerStore().getSyncBlock(Longs.fromByteArray(current.toByteArray())+1);
									if (next != null)
										inventory.add(next);
									
									current = next;
								}
								while(current != null && inventory.size() < InventoryMessage.MAX_INVENTORY);

								// Always respond, even if nothing to send
								SyncBlockInventoryMessage syncBlockInventoryMessage;
								if (inventory.isEmpty() == true)
									syncBlockInventoryMessage = new SyncBlockInventoryMessage(getSyncBlockInventoryMessage.getSeq());
								else
									syncBlockInventoryMessage = new SyncBlockInventoryMessage(getSyncBlockInventoryMessage.getSeq(), inventory);
									
								try
								{
									SyncHandler.this.context.getNetwork().getMessaging().send(syncBlockInventoryMessage, peer);
								}
								catch (IOException ex)
								{
									syncLog.error(SyncHandler.this.context.getName()+": Unable to send SyncBlockInventoryMessage "+getSyncBlockInventoryMessage.getSeq()+":"+getSyncBlockInventoryMessage.getHead()+" for "+inventory.size()+" blocks to "+peer, ex);
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
								if (syncBlockInventoryMessage.getInventory().isEmpty() == true)
									syncLog.warn(SyncHandler.this.context.getName()+": Received empty block inventory from " + peer);
								else if (syncLog.hasLevel(Logging.DEBUG) == true)
									syncLog.debug(SyncHandler.this.context.getName()+": Received block header inventory "+syncBlockInventoryMessage.getInventory()+" from " + peer);
								
								SyncInventoryPeerTask syncInventoryPeerTask = SyncHandler.this.inventoryTasks.get(peer);
								if (syncInventoryPeerTask == null || syncInventoryPeerTask.requestSeq != syncBlockInventoryMessage.getResponseSeq())
								{
									syncLog.error(SyncHandler.this.context.getName()+": Received unrequested inventory "+syncBlockInventoryMessage.getResponseSeq()+" with "+syncBlockInventoryMessage.getInventory().size()+" items from "+peer);
									peer.disconnect("Received unrequested inventory "+syncBlockInventoryMessage.getResponseSeq()+" with "+syncBlockInventoryMessage.getInventory().size()+" items from "+peer);
									return;
								}
								
								if (syncBlockInventoryMessage.getInventory().isEmpty() == false)
									SyncHandler.this.blockInventories.putAll(peer, syncBlockInventoryMessage.getInventory());
								SyncHandler.this.inventoryTasks.remove(peer, syncInventoryPeerTask);
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
			
			// SyncHandler starts as OOS, prepare the last known good state
			prepare();

			Thread syncProcessorThread = new Thread(this.syncProcessor);
			syncProcessorThread.setDaemon(true);
			syncProcessorThread.setName(this.context.getName()+" Sync Processor");
			syncProcessorThread.start();
		}
		catch (Throwable t)
		{
			throw new StartupException(t);
		}
	}

	@Override
	public void stop() throws TerminationException
	{
		this.syncProcessor.terminate(true);
		this.context.getEvents().unregister(this.peerListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	private void reset()
	{
		this.atoms.clear();
		this.blockInventories.clear();
		this.blocks.clear();
		this.blocksRequested.clear();
		
		Collection<SyncRequestPeerTask> requestTasks = new ArrayList<SyncRequestPeerTask>(this.requestTasks.values());
		for (SyncRequestPeerTask requestTask : requestTasks)
			requestTask.cancel();
		this.requestTasks.clear();
		
		Collection<SyncInventoryPeerTask> inventoryTasks = new ArrayList<SyncInventoryPeerTask>(this.inventoryTasks.values());
		for (SyncInventoryPeerTask inventoryTask : inventoryTasks)
			inventoryTask.cancel();
		this.inventoryTasks.clear();
	}
	
	private boolean request(final ConnectedPeer peer, final Hash block) throws IOException
	{
		final Entry<Hash, Long> blockRequest = new AbstractMap.SimpleEntry<>(block, ThreadLocalRandom.current().nextLong());
		SyncHandler.this.lock.lock();
		try
		{
			if (this.blocksRequested.containsKey(block) == true)
			{
				syncLog.warn(SyncHandler.this.context.getName()+": Block "+block+" is already requested when requesting from "+peer);
				return false;
			}
			
			SyncRequestPeerTask syncPeerTask = null;
			try
			{
				this.blocksRequested.put(blockRequest.getKey(), blockRequest.getValue());
					
				if (syncLog.hasLevel(Logging.DEBUG))
					syncLog.debug(SyncHandler.this.context.getName()+": Requesting block "+blockRequest.getKey()+" from "+peer);
	
				GetSyncBlockMessage getBlockMessage = new GetSyncBlockMessage(blockRequest.getKey()); 
				this.context.getNetwork().getMessaging().send(getBlockMessage, peer);
					
				syncPeerTask = new SyncRequestPeerTask(peer, blockRequest);
				this.requestTasks.put(peer, syncPeerTask);
				Executor.getInstance().schedule(syncPeerTask);
			}
			catch (Throwable t)
			{
				if (syncPeerTask != null)
				{
					if (syncPeerTask.cancel() == true)
						this.requestTasks.remove(peer, syncPeerTask);
				}
				
				this.blocksRequested.remove(blockRequest.getKey());
				throw t;
			}
		}
		finally
		{
			SyncHandler.this.lock.unlock();
		}
		
		return true;
	}

	private void commit(final SyncBranch branch) throws IOException, ValidationException, StateLockedException
	{
		// TODO pretty much everything, limited validation here currently, just accepts blocks and atoms almost on faith
		// TODO remove reference to ledger StateAccumulator and use a local instance with a push on sync
		List<Block> committedBlocks = new ArrayList<Block>();
		Iterator<BlockHeader> blockHeaderIterator = branch.getHeaders().iterator();
		while(blockHeaderIterator.hasNext() == true)
		{
			BlockHeader header = blockHeaderIterator.next();
			Block block = this.blocks.get(header.getHash());
			// Provision any missing atoms required by certificates
			for (AtomCertificate certificate : block.getCertificates())
			{
				PendingAtom pendingAtom = this.atoms.get(certificate.getAtom());
				if (pendingAtom != null)
					continue;
				
				Commit atomCommit = this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, certificate.getAtom()));
				if (atomCommit == null)
					throw new ValidationException(this.context.getName()+": Atom "+certificate.getAtom()+" required for provisioning by certificate "+certificate.getHash()+" not committed");
				
				Atom atomToProvision = this.context.getLedger().getLedgerStore().get(atomCommit.getPath().get(Elements.ATOM), Atom.class);
				if (atomToProvision == null)
					throw new ValidationException(this.context.getName()+": Atom "+certificate.getAtom()+" required for provisioning by certificate "+certificate.getHash()+" committed but not found");
				
				BlockHeader atomBlockHeader = this.context.getLedger().getLedgerStore().get(atomCommit.getPath().get(Elements.BLOCK), BlockHeader.class);
				if (atomBlockHeader == null)
					throw new ValidationException(this.context.getName()+": Atom "+certificate.getAtom()+" required for provisioning by certificate "+certificate.getHash()+" committed but block "+atomCommit.getPath().get(Elements.BLOCK)+" not found");
				
				pendingAtom = PendingAtom.create(this.context, atomToProvision);
				this.atoms.put(pendingAtom.getHash(), pendingAtom);
				pendingAtom.prepare();
				this.context.getLedger().getStateAccumulator().lock(pendingAtom);
				pendingAtom.accepted();
				pendingAtom.provision(atomBlockHeader);
			}

			// Provision atoms contained in block 
			for (Atom atom : block.getAtoms())
			{
				PendingAtom pendingAtom = PendingAtom.create(this.context, atom);
				this.atoms.put(pendingAtom.getHash(), pendingAtom);
				pendingAtom.prepare();
				this.context.getLedger().getStateAccumulator().lock(pendingAtom);
				pendingAtom.accepted();
				pendingAtom.provision(block.getHeader());
			}
			
			this.context.getLedger().getLedgerStore().commit(block);
			committedBlocks.add(block);

			//  Process certificates contained in block
			final List<CommitOperation> atomCommitOperations = new ArrayList<CommitOperation>();
			for (AtomCertificate certificate : block.getCertificates())
			{
				PendingAtom pendingAtom = this.atoms.get(certificate.getAtom());
				if (pendingAtom == null)
					throw new ValidationException(this.context.getName()+": Pending atom "+certificate.getAtom()+" not found for certificate "+certificate.getHash()+" in block "+block.getHash());
				
				for (StateCertificate stateCertificate : certificate.getAll())
					pendingAtom.provision(new StateInput(pendingAtom.getHash(), stateCertificate.getState(), stateCertificate.getInput()));
				pendingAtom.execute();
				
				pendingAtom.setCertificate(certificate);
				
				this.context.getLedger().getLedgerStore().store(block.getHeader().getHeight(), certificate);
				for (StateCertificate stateCertificate : certificate.getAll())
				{
					this.context.getLedger().getLedgerStore().store(block.getHeader().getHeight(), stateCertificate);
					StateInput stateInput = new StateInput(stateCertificate.getAtom(), stateCertificate.getState(), stateCertificate.getInput());
					if (this.context.getLedger().getLedgerStore().has(stateInput.getHash()) == false)
						this.context.getLedger().getLedgerStore().store(block.getHeader().getHeight(), stateInput);
				}
				
				atomCommitOperations.add(pendingAtom.getCommitOperation());
				this.context.getLedger().getStateAccumulator().unlock(pendingAtom);
				this.atoms.remove(pendingAtom.getHash());
			}
			if (atomCommitOperations.isEmpty() == false)
				this.context.getLedger().getLedgerStore().commit(atomCommitOperations);
			
			// Commit timeouts
			Iterator<PendingAtom> atomsIterator = this.atoms.values().iterator();
			while(atomsIterator.hasNext() == true)
			{
				PendingAtom pendingAtom = atomsIterator.next();

				if (block.getHeader().getHeight() <= pendingAtom.getCommitBlockTimeout())
					continue;
			
				this.context.getLedger().getStateAccumulator().unlock(pendingAtom);
				this.context.getLedger().getLedgerStore().commitTimedOut(pendingAtom.getHash());
				atomsIterator.remove();
			}
		}
		
		for (Block committedBlock : committedBlocks)
		{
			SyncBlockEvent syncBlockEvent = new SyncBlockEvent(committedBlock);
			SyncHandler.this.context.getEvents().post(syncBlockEvent); // TODO Might need to catch exceptions on this from synchronous listeners
		}
	}
	
	private boolean checkSynced() throws IOException, ValidationException, StateLockedException
	{
		if (this.context.getConfiguration().has("singleton") == true)
		{
			if (isSynced() == false)
				setSynced(true);

			return true;
		}

		long numShardGroups = this.context.getLedger().numShardGroups();
		long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		StandardPeerFilter standardPeerFilter = StandardPeerFilter.build(this.context).setShardGroup(localShardGroup).setStates(PeerState.CONNECTED);
		List<ConnectedPeer> syncPeers = this.context.getNetwork().get(standardPeerFilter); 
		if (syncPeers.isEmpty() == true)
		{
			if (isSynced() == true)
			{
				setSynced(false);
				syncLog.info(this.context.getName()+": Out of sync state triggered due to no sync peers available");
			}

			return false;
		}
	
		// Out of sync?
		// TODO need to deal with forks that don't converge with local chain due to safety break
		boolean unsynced = syncPeers.stream().anyMatch(sp -> {
			if (sp.getNode().isAheadOf(this.context.getNode(), Node.OOS_TRIGGER_LIMIT) == true)
				return true;
			
			if (this.context.getLedger().getHead().getHash().equals(Universe.getDefault().getGenesis().getHash()) == true && 
				sp.getNode().isAheadOf(this.context.getNode(), Node.OOS_RESOLVED_LIMIT) == true)
				return true;
			
			return false;
		});
		
		if (isSynced() == true && unsynced == true)
		{
			setSynced(false);
			syncLog.info(this.context.getName()+": Out of sync state detected with OOS_TRIGGER limit of "+Node.OOS_TRIGGER_LIMIT);
			
			prepare();
		}
		else if (isSynced() == false)
		{
			// Can consider in sync? //
			boolean syncAcquired = false;
			ConnectedPeer strongestPeer = null;
			for (ConnectedPeer syncPeer : syncPeers)
			{
				if (strongestPeer == null || 
					syncPeer.getNode().getHead().getHeight() > strongestPeer.getNode().getHead().getHeight())
					strongestPeer = syncPeer;
			}
			
			if (strongestPeer != null && this.context.getNode().isAheadOf(strongestPeer.getNode(), 0) == true)
				syncAcquired = true;
			
			// Is block processing completed and local replica considered in sync?
			if (this.blocksRequested.isEmpty() == true && 
				this.blocks.isEmpty() == true && syncAcquired == true)
			{
				// TODO May be better to do the sync status change via events to the modules which carry the 
				//		pendingAtoms left mid-phase here rather than calling direct into services.
				//		Two events would be needed I think, one to prepare for the sync status change (load state)
				//		and another to then execute that state on an actual sync status change event.
				
				// Take a copy of atoms mid-phase as setSynced() calls reset
				List<PendingAtom> pendingAtoms = new ArrayList<PendingAtom>(this.atoms.values());
				// Inject remaining atoms that may be in a pending consensus phase
				for (PendingAtom pendingAtom : pendingAtoms)
				{
					this.context.getLedger().getAtomHandler().push(pendingAtom);
					this.context.getLedger().getStateHandler().push(pendingAtom);
				}
				
				// Determine which pending atoms have been provisioned and can be executed
				for (PendingAtom pendingAtom : pendingAtoms)
				{
					for (StateKey<?,?> stateKey : pendingAtom.getStateKeys())
					{
						final StateInput stateInput = this.context.getLedger().getLedgerStore().get(StateInput.getHash(pendingAtom.getHash(), stateKey), StateInput.class);
						if (stateInput == null)
							continue;

						pendingAtom.provision(stateInput);
					}
					
					if (pendingAtom.isProvisioned() == true)
						pendingAtom.execute();
				}

				setSynced(true);
				
				// Finally signal provisioning of atom state inputs
				for (PendingAtom pendingAtom : pendingAtoms)
				{
					boolean provisioned = pendingAtom.isProvisioned();
					if (provisioned == true)
						continue;

					// Atom certificates may have been loaded on sync event change
					if (pendingAtom.getCertificate() != null)
					{
						for (StateCertificate stateCertificate : pendingAtom.getCertificate().getAll())
						{
							if (this.context.getLedger().getLedgerStore().has(stateCertificate.getHash()) == false)
								this.context.getLedger().getLedgerStore().store(this.context.getLedger().getHead().getHeight(), stateCertificate);
							
							StateInput stateInput = new StateInput(stateCertificate.getAtom(), stateCertificate.getState(), stateCertificate.getInput());
							if (this.context.getLedger().getLedgerStore().has(stateInput.getHash()) == false)
								this.context.getLedger().getLedgerStore().store(this.context.getLedger().getHead().getHeight(), stateInput);

							provisioned = pendingAtom.provision(new StateInput(stateCertificate.getAtom(), stateCertificate.getState(), stateCertificate.getInput()));
						}
						
						pendingAtom.execute();
						continue;
					}
					
					final Set<StateKey<?, ?>> stateKeysToProvision = new HashSet<StateKey<?, ?>>();
					for (StateKey<?,?> stateKey : pendingAtom.getStateKeys())
					{
						if (pendingAtom.getInput(stateKey) != null)
							continue; 
						
						StateCertificate stateCertificate = pendingAtom.getCertificate(stateKey);
						if (stateCertificate == null)
							stateKeysToProvision.add(stateKey);
						else
							provisioned = pendingAtom.provision(new StateInput(pendingAtom.getHash(), stateKey, stateCertificate.getInput()));
					}
					
					if (provisioned == false)
						this.context.getLedger().getStateHandler().provision(pendingAtom, stateKeysToProvision);
				}

				// Tell all peers we're synced
				for (ConnectedPeer connectedPeer : this.context.getNetwork().get(StandardPeerFilter.build(this.context).setStates(PeerState.CONNECTED)))
				{
					try
					{
						NodeMessage nodeMessage = new NodeMessage(SyncHandler.this.context.getNode());
						this.context.getNetwork().getMessaging().send(nodeMessage, connectedPeer);
					}
					catch (IOException ioex)
					{
						syncLog.error("Could not send synced declaration to "+connectedPeer, ioex);
					}
				}
				
				// Requests for current block pool, atom pool and state consensus primitives
				for (ConnectedPeer syncPeer : syncPeers)
				{
					try
					{
						this.context.getNetwork().getMessaging().send(new SyncAcquiredMessage(SyncHandler.this.context.getLedger().getHead()), syncPeer);
					}
					catch (Exception ex)
					{
						syncLog.error(this.context.getName()+": Unable to send SyncAcquiredMessage from "+this.context.getLedger().getHead().getHash()+" to "+syncPeer, ex);
					}
				}

				syncLog.info(this.context.getName()+": Synced state reaquired");
			}
		}				
		
		return isSynced();
	}
	
	/**
	 * Prepares the SyncHandler for a sync attempt, loading the latest known snapshot of state
	 * 
	 * @throws IOException 
	 * @throws ValidationException 
	 * @throws StateLockedException 
	 */
	private void prepare() throws IOException, ValidationException, StateLockedException
	{
		// Nothing to prepare if at genesis head
		if (this.context.getLedger().getHead().getHash().equals(Universe.getDefault().getGenesis().getHash()) == true)
			return;
		
		// Collect the recent blocks within the commit timeout window from the head 
		final LinkedList<Block> recentBlocks = new LinkedList<Block>();
		Block current = this.context.getLedger().getLedgerStore().get(this.context.getLedger().getHead().getHash(), Block.class);
		do
		{
			recentBlocks.add(current);
			current = this.context.getLedger().getLedgerStore().get(current.getHeader().getPrevious(), Block.class);
		}
		while (current.getHash().equals(Universe.getDefault().getGenesis().getHash()) == false && 
			   current.getHeader().getHeight() >= this.context.getLedger().getHead().getHeight() - PendingAtom.ATOM_COMMIT_TIMEOUT_BLOCKS);
		
		Collections.reverse(recentBlocks);
		
		for (Block block : recentBlocks)
		{
			//  Process certificates contained in block
			for (AtomCertificate certificate : block.getCertificates())
			{
				PendingAtom pendingAtom = this.atoms.get(certificate.getAtom());
				if (pendingAtom == null)
				{
					if (this.context.getLedger().getLedgerStore().has(certificate.getAtom()) == true)
						continue;
					
					throw new ValidationException(this.context.getName()+": Pending atom "+certificate.getAtom()+" not found for certificate "+certificate.getHash()+" in block "+block.getHash());
				}
				
				pendingAtom.setCertificate(certificate);
				this.context.getLedger().getStateAccumulator().unlock(pendingAtom);
				this.atoms.remove(pendingAtom.getHash());
			}

			// Process the atoms contained in block
			for (Atom atom : block.getAtoms())
			{
				PendingAtom pendingAtom = new PendingAtom(this.context, atom, block.getHeader().getTimestamp());
				this.atoms.put(pendingAtom.getHash(), pendingAtom);
				pendingAtom.prepare();
				this.context.getLedger().getStateAccumulator().lock(pendingAtom);
				pendingAtom.accepted();
				pendingAtom.provision(block.getHeader());
			}
		}
	}
	
    boolean isSynced()
	{
		return this.synced;
	}

	void setSynced(boolean synced)
	{
		// TODO what happens with an exception here?
		this.context.getNode().setSynced(synced);
		this.context.getEvents().post(new SyncStatusChangeEvent(synced));
		this.synced = synced;
		reset();
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
   			SyncHandler.this.lock.lock();
    		try
    		{
    			SyncHandler.this.blockInventories.removeAll(event.getPeer());
    			SyncRequestPeerTask requestTask = SyncHandler.this.requestTasks.remove(event.getPeer());
    			if (requestTask != null)
    			{
    				try
    				{
    					if (requestTask.isCancelled() == false)
    						requestTask.cancel();

    					syncLog.info(SyncHandler.this.context.getName()+": Cancelled block sync task of "+requestTask.requestedBlock.getKey()+" of blocks from "+event.getPeer());
    				}
    	    		catch (Throwable t)
    	    		{
    	    			syncLog.error(SyncHandler.this.context.getName()+": Failed to cancel block sync task of "+requestTask.requestedBlock.getKey()+" of blocks from "+event.getPeer());
    	    		}
    			}
    			
    			SyncInventoryPeerTask inventoryTask = SyncHandler.this.inventoryTasks.remove(event.getPeer());
    			if (inventoryTask != null)
    			{
    				try
    				{
    					if (inventoryTask.isCancelled() == false)
    						inventoryTask.cancel();

    					syncLog.info(SyncHandler.this.context.getName()+": Cancelled inventory sync task of "+inventoryTask.requestSeq+":"+inventoryTask.block+" of blocks from "+event.getPeer());
    				}
    	    		catch (Throwable t)
    	    		{
    	    			syncLog.error(SyncHandler.this.context.getName()+": Failed to cancel inventory sync task of "+inventoryTask.requestSeq+":"+inventoryTask.block+" of blocks from "+event.getPeer());
    	    		}
    			}
    		}
    		finally
    		{
    			SyncHandler.this.lock.unlock();
    		}
		}
    };
}
