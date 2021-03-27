package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.collections.MappedBlockingQueue;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.events.EventListener;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.PendingBranch.Type;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.events.SyncStatusChangeEvent;
import org.fuserleer.ledger.messages.BlockMessage;
import org.fuserleer.ledger.messages.GetBlockMessage;
import org.fuserleer.ledger.messages.SyncAcquiredMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.GossipFetcher;
import org.fuserleer.network.GossipFilter;
import org.fuserleer.network.GossipInventory;
import org.fuserleer.network.GossipReceiver;
import org.fuserleer.network.messages.BroadcastInventoryMessage;
import org.fuserleer.network.messages.GetInventoryItemsMessage;
import org.fuserleer.network.messages.SyncInventoryMessage;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.node.Node;
import org.fuserleer.time.Time;
import org.fuserleer.utils.MathUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public class BlockHandler implements Service
{
	private static final Logger blocksLog = Logging.getLogger("blocks");

	public static boolean withinRange(long location, long point, long range)
	{
		long distance = MathUtils.ringDistance64(point, location);
		if (distance > range)
			return true;
		
		return false;
	}
	
	public static long getDistance(long location, long point, long range)
	{
		long distance = MathUtils.ringDistance64(point, location);
		if (distance > range)
			return distance;
		
		return 0l;
	}
	
	private Executable blockProcessor = new Executable()
	{
		private long generatedCount = 0;
		private long generatedTimeTotal = 0;
		private long committedCount = 0;
		private long committedTimeTotal = 0;
		
		@Override
		public void execute()
		{
			try 
			{
				long lastIteration = System.currentTimeMillis();
				while (this.isTerminated() == false)
				{
					try
					{
						long delay = 1000 - (System.currentTimeMillis() - lastIteration);
						if (delay > 0)
							Thread.sleep(delay);
						
						lastIteration = System.currentTimeMillis();
						if (BlockHandler.this.context.getLedger().isSynced() == false)
							continue;
						
						// TODO do clean up of old pending blocks / committed / orphans etc
						
						// Find the best block candidate to build on top of
						BlockHeader buildCandidate = null;
						
						BlockHandler.this.lock.writeLock().lock();
						try
						{
							updateBlocks();
							updateBranches();
							BlockHandler.this.bestBranch = discoverBestBranch();
							
							if (BlockHandler.this.bestBranch != null)
								buildCandidate = BlockHandler.this.bestBranch.getHigh().getHeader();
							else
								buildCandidate = BlockHandler.this.context.getLedger().getHead();

							// Vote on best branch?
							// If branches are getting long, then possible that blocks are being generated too fast and being voted on by the producing nodes.
							// A race condition can also occur with fast block production such that producers will generate the strongest branch they can see and always vote on it.
							// Defer the vote based on the size of the branch, giving generated block headers some time to propagate. 
							if (BlockHandler.this.bestBranch != null && BlockHandler.this.voteClock.get() < (buildCandidate.getHeight() - Math.log(BlockHandler.this.bestBranch.size())))
								vote(BlockHandler.this.bestBranch);
						}
						finally
						{
							BlockHandler.this.lock.writeLock().unlock();
						}
						
						PendingBlock generatedBlock = null;
						long generationStart = Time.getSystemTime();
						// Safe to attempt to build a block outside of a lock
						if (buildCandidate.getHeight() >= BlockHandler.this.buildClock.get())
							generatedBlock = BlockHandler.this.build(buildCandidate, BlockHandler.this.bestBranch);
						
						BlockHandler.this.lock.writeLock().lock();
						try
						{
							if (generatedBlock != null && BlockHandler.this.upsertBlock(generatedBlock) == true)
							{
								BlockHandler.this.buildClock.set(generatedBlock.getHeader().getHeight());
								this.generatedCount++;
								this.generatedTimeTotal += (Time.getSystemTime()-generationStart);
								blocksLog.info(BlockHandler.this.context.getName()+": Generated block "+generatedBlock.getHeader()+" in "+(Time.getSystemTime()-generationStart)+"ms / "+(this.generatedTimeTotal/this.generatedCount)+" ms average");
								BlockHandler.this.context.getNetwork().getGossipHandler().broadcast(generatedBlock.getHeader());
							}

							if (BlockHandler.this.bestBranch != null)
							{
								long commitStart = Time.getSystemTime();
								Collection<PendingBlock> committedBlocks = null;
								PendingBlock committable = BlockHandler.this.bestBranch.commitable();
								if (committable != null)
								{
									committedBlocks = commit(committable, BlockHandler.this.bestBranch);
									if (committedBlocks.isEmpty() == false)
									{
										this.committedCount += committedBlocks.size();
										this.committedTimeTotal += (Time.getSystemTime()-commitStart);
										blocksLog.info(BlockHandler.this.context.getName()+": Committed "+committedBlocks.size()+" blocks in "+(Time.getSystemTime()-commitStart)+"ms / "+(this.committedTimeTotal/this.committedCount)+" ms average");
									}
								}
							}
						}
						finally
						{
							BlockHandler.this.lock.writeLock().unlock();
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
				blocksLog.fatal(BlockHandler.this.context.getName()+": Error processing blocks", throwable);
			}
		}
	};
	
	private final Semaphore voteProcessorSemaphore = new Semaphore(0);
	private final MappedBlockingQueue<Hash, BlockVote> votesToCountQueue;
	private Executable voteProcessor = new Executable()
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
						// TODO convert to a wait / notify
						if (BlockHandler.this.voteProcessorSemaphore.tryAcquire(1, TimeUnit.SECONDS) == false)
							continue;

						if (BlockHandler.this.context.getLedger().isSynced() == false)
							continue;

						if (BlockHandler.this.votesToSyncQueue.isEmpty() == false)
						{
							Entry<Hash, BlockVote> blockVote;
							while((blockVote = BlockHandler.this.votesToSyncQueue.peek()) != null)
							{
								try
								{
									process(blockVote.getValue());
								}
								catch (Exception ex)
								{
									blocksLog.error(BlockHandler.this.context.getName()+": Error syncing block vote "+blockVote, ex);
								}
								finally
								{
									if (BlockHandler.this.votesToSyncQueue.remove(blockVote.getKey(), blockVote.getValue()) == false)
										throw new IllegalStateException("Block vote peek/remove failed for "+blockVote.getValue());
								}
							}
						}

						List<BlockVote> blockVotesToBroadcast = new ArrayList<BlockVote>();
						if (BlockHandler.this.votesToCountQueue.isEmpty() == false)
						{
							Entry<Hash, BlockVote> blockVote;
							while((blockVote = BlockHandler.this.votesToCountQueue.peek()) != null)
							{
								try
								{
									if (BlockHandler.this.context.getLedger().getLedgerStore().store(blockVote.getValue()).equals(OperationStatus.SUCCESS) == false)
									{
										blocksLog.warn(BlockHandler.this.context.getName()+": Received already seen block vote of "+blockVote.getValue()+" for "+blockVote.getValue().getOwner());
										continue;
									}
									
									if (process(blockVote.getValue()) == true)
										blockVotesToBroadcast.add(blockVote.getValue());
								}
								catch (Exception ex)
								{
									blocksLog.error(BlockHandler.this.context.getName()+": Error counting block vote for "+blockVote.getValue(), ex);
								}
								finally
								{
									if (BlockHandler.this.votesToCountQueue.remove(blockVote.getKey(), blockVote.getValue()) == false)
										throw new IllegalStateException("Block vote peek/remove failed for "+blockVote.getValue());
								}
							}
						}
						
						try
						{
							if (blockVotesToBroadcast.isEmpty() == false)
								BlockHandler.this.context.getNetwork().getGossipHandler().broadcast(BlockVote.class, blockVotesToBroadcast);
						}
						catch (Exception ex)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": Error broadcasting block votes "+blockVotesToBroadcast, ex);
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
				blocksLog.fatal(BlockHandler.this.context.getName()+": Error processing block vote queue", throwable);
			}
		}
	};

	private final Context context;

	private final VotePowerHandler 				votePowerHandler;
	private final BlockRegulator				blockRegulator;

	private final Map<Hash, PendingBlock>		pendingBlocks;
	private final Set<PendingBranch>			pendingBranches;
	
	private final AtomicLong					voteClock;
	private final AtomicLong					buildClock;
	private final AtomicReference<BlockHeader> 	currentVote;
	private PendingBranch 						bestBranch;

	private final MappedBlockingQueue<Hash, BlockVote> votesToSyncQueue;

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

	BlockHandler(Context context, VotePowerHandler votePowerHandler)
	{
		this.context = Objects.requireNonNull(context);
		this.pendingBlocks = Collections.synchronizedMap(new HashMap<Hash, PendingBlock>());
		this.pendingBranches = Collections.synchronizedSet(new HashSet<PendingBranch>());
		this.votePowerHandler = Objects.requireNonNull(votePowerHandler, "Vote power handler is null");
		this.votesToSyncQueue = new MappedBlockingQueue<Hash, BlockVote>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.votesToCountQueue = new MappedBlockingQueue<Hash, BlockVote>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.blockRegulator = new BlockRegulator(context);
		this.voteClock = new AtomicLong(0);
		this.buildClock = new AtomicLong(0);
		this.currentVote = new AtomicReference<BlockHeader>();
		this.bestBranch = null;
	}

	@Override
	public void start() throws StartupException
	{
		this.voteClock.set(this.context.getLedger().getHead().getHeight());
		this.blockRegulator.start();

		// BLOCK HEADER GOSSIP //
		this.context.getNetwork().getGossipHandler().register(BlockHeader.class, new GossipFilter(this.context) 
		{
			@Override
			public Set<Long> filter(Primitive blockHeader) throws IOException
			{
				long blockShardGroup = ShardMapper.toShardGroup(((BlockHeader)blockHeader).getOwner(), BlockHandler.this.context.getLedger().numShardGroups(((BlockHeader)blockHeader).getHeight()));;
				long localShardGroup = ShardMapper.toShardGroup(BlockHandler.this.context.getNode().getIdentity(), BlockHandler.this.context.getLedger().numShardGroups(((BlockHeader)blockHeader).getHeight()));
				if (blockShardGroup != localShardGroup)
				{
					blocksLog.warn(BlockHandler.this.context.getName()+": Block header is for shard group "+blockShardGroup+" but expected local shard group "+localShardGroup);
					// TODO disconnect and ban;
					return Collections.emptySet();
				}
				
				return Collections.singleton(localShardGroup);
			}
		});

		this.context.getNetwork().getGossipHandler().register(BlockHeader.class, new GossipInventory() 
		{
			@Override
			public int requestLimit()
			{
				return 8;
			}

			@Override
			public Collection<Hash> required(Class<? extends Primitive> type, Collection<Hash> items) throws Throwable
			{
				if (type.equals(BlockHeader.class) == false)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": Block header type expected but got "+type);
					return Collections.emptyList();
				}
				
				BlockHandler.this.lock.readLock().lock();
				try
				{
					Set<Hash> required = new HashSet<Hash>();
					for (Hash item : items)
					{
						PendingBlock pendingBlock = BlockHandler.this.pendingBlocks.get(item);
						if ((pendingBlock != null && pendingBlock.getHeader() != null) || 
							BlockHandler.this.context.getLedger().getLedgerStore().has(item) == true)
							continue;
						
						required.add(item);
					}

					return required;
				}
				finally
				{
					BlockHandler.this.lock.readLock().unlock();
				}
			}
		});

		this.context.getNetwork().getGossipHandler().register(BlockHeader.class, new GossipReceiver() 
		{
			@Override
			public void receive(Primitive object) throws IOException
			{
				BlockHeader blockHeader = (BlockHeader) object;
				BlockHandler.this.lock.writeLock().lock();
				try
				{
					if (push(blockHeader) == true)
						BlockHandler.this.context.getNetwork().getGossipHandler().broadcast(blockHeader);
				}
				finally
				{
					BlockHandler.this.lock.writeLock().unlock();
				}
			}
		});
		
		this.context.getNetwork().getGossipHandler().register(BlockHeader.class, new GossipFetcher() 
		{
			@Override
			public Collection<BlockHeader> fetch(Collection<Hash> items) throws IOException
			{
				BlockHandler.this.lock.readLock().lock();
				try
				{
					Set<BlockHeader> fetched = new HashSet<BlockHeader>();
					for (Hash item : items)
					{
						BlockHeader blockHeader = null;
						PendingBlock pendingBlock = BlockHandler.this.pendingBlocks.get(item);
						if (pendingBlock != null)
							blockHeader = pendingBlock.getHeader();
						
						if (blockHeader == null)
							blockHeader = BlockHandler.this.context.getLedger().getLedgerStore().get(item, BlockHeader.class);
						
						if (blockHeader == null)
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Requested block header not found "+item);
							
							continue;
						}
						fetched.add(blockHeader);
					}
					return fetched;
				}
				finally
				{
					BlockHandler.this.lock.readLock().unlock();
				}
			}
		});
		
		// BLOCK VOTE GOSSIP //
		this.context.getNetwork().getGossipHandler().register(BlockVote.class, new GossipFilter(this.context) 
		{
			@Override
			public Set<Long> filter(Primitive blockVote) throws IOException
			{
				long blockShardGroup = ShardMapper.toShardGroup(((BlockVote)blockVote).getOwner(), BlockHandler.this.context.getLedger().numShardGroups(((BlockVote)blockVote).getHeight()));;
				long localShardGroup = ShardMapper.toShardGroup(BlockHandler.this.context.getNode().getIdentity(), BlockHandler.this.context.getLedger().numShardGroups(((BlockVote)blockVote).getHeight()));
				if (blockShardGroup != localShardGroup)
				{
					blocksLog.warn(BlockHandler.this.context.getName()+": Block vote is for shard group "+blockShardGroup+" but expected local shard group "+localShardGroup);
					// TODO disconnect and ban;
					return Collections.emptySet();
				}
				
				return Collections.singleton(localShardGroup);
			}
		});

		this.context.getNetwork().getGossipHandler().register(BlockVote.class, new GossipInventory() 
		{
			@Override
			public int requestLimit()
			{
				return GetInventoryItemsMessage.MAX_ITEMS;
			}

			@Override
			public Collection<Hash> required(Class<? extends Primitive> type, Collection<Hash> items) throws IOException
			{
				if (type.equals(BlockVote.class) == false)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": Block vote type expected but got "+type);
					return Collections.emptyList();
				}
		
				BlockHandler.this.lock.readLock().lock();
				try
				{
					Set<Hash> required = new HashSet<Hash>();
					for (Hash item : items)
					{
						if (BlockHandler.this.votesToCountQueue.contains(item) == true || 
							BlockHandler.this.context.getLedger().getLedgerStore().has(item) == true)
							continue;
						
						required.add(item);
					}
					return required;
				}
				finally
				{
					BlockHandler.this.lock.readLock().unlock();
				}
			}
		});
		
		this.context.getNetwork().getGossipHandler().register(BlockVote.class, new GossipReceiver() 
		{
			@Override
			public void receive(Primitive object) throws IOException, ValidationException
			{
				BlockVote blockVote = (BlockVote) object;
				if (blocksLog.hasLevel(Logging.DEBUG) == true)
					blocksLog.debug(BlockHandler.this.context.getName()+": Block vote received "+blockVote+" for "+blockVote.getOwner());

				long numShardGroups = BlockHandler.this.context.getLedger().numShardGroups();
				long localShardGroup = ShardMapper.toShardGroup(BlockHandler.this.context.getNode().getIdentity(), numShardGroups); 
				long blockVoteShardGroup = ShardMapper.toShardGroup(blockVote.getOwner(), numShardGroups);
				if (localShardGroup != blockVoteShardGroup)
				{
					blocksLog.warn(BlockHandler.this.context.getName()+": Block vote "+blockVote.getHash()+" for "+blockVote.getOwner()+" is for shard group "+blockVoteShardGroup+" but expected local shard group "+localShardGroup);
					// TODO disconnect and ban;
					return;
				}
				
				BlockHandler.this.votesToCountQueue.put(blockVote.getHash(), blockVote);
				BlockHandler.this.voteProcessorSemaphore.release();
			}
		});
		
		this.context.getNetwork().getGossipHandler().register(BlockVote.class, new GossipFetcher() 
		{
			public Collection<BlockVote> fetch(Collection<Hash> items) throws IOException
			{
				BlockHandler.this.lock.readLock().lock();
				try
				{
					Set<BlockVote> fetched = new HashSet<BlockVote>();
					for (Hash item : items)
					{
						BlockVote blockVote = BlockHandler.this.votesToCountQueue.get(item);
						if (blockVote == null)
							blockVote = BlockHandler.this.context.getLedger().getLedgerStore().get(item, BlockVote.class);

						if (blockVote == null)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": Requested block vote "+item+" not found");
							continue;
						}
					
						fetched.add(blockVote);
					}
					return fetched;
				}
				finally
				{
					BlockHandler.this.lock.readLock().unlock();
				}
			}
		});

		this.context.getNetwork().getMessaging().register(GetBlockMessage.class, this.getClass(), new MessageProcessor<GetBlockMessage>()
		{
			@Override
			public void process(final GetBlockMessage getBlockMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						BlockHandler.this.lock.readLock().lock();
						try
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Block request for "+getBlockMessage.getBlock()+" from "+peer);
							
							Block block = BlockHandler.this.context.getLedger().getLedgerStore().get(getBlockMessage.getBlock(), Block.class);
							if (block == null)
							{
								if (blocksLog.hasLevel(Logging.DEBUG) == true)
									blocksLog.debug(BlockHandler.this.context.getName()+": Requested block "+getBlockMessage.getBlock()+" not found for "+peer);
									
								return;
							}
							
							try
							{
								BlockHandler.this.context.getNetwork().getMessaging().send(new BlockMessage(block), peer);
							}
							catch (IOException ex)
							{
								blocksLog.error(BlockHandler.this.context.getName()+": Unable to send BlockMessage for "+getBlockMessage.getBlock()+" to "+peer, ex);
							}
						}
						catch (Exception ex)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block.get " + peer, ex);
						}
						finally
						{
							BlockHandler.this.lock.readLock().unlock();
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
						BlockHandler.this.lock.readLock().lock();
						try
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Block pool inventory request from "+peer);

							
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Broadcasting about "+BlockHandler.this.pendingBlocks.size()+" pool blocks to "+peer);

							final Set<PendingBlock> pendingBlocks = new HashSet<PendingBlock>(BlockHandler.this.pendingBlocks.values());
							final Set<Hash> pendingBlockInventory = new HashSet<Hash>();
							final Set<Hash> blockVoteInventory = new HashSet<Hash>();
							
							for (PendingBlock pendingBlock : pendingBlocks)
							{
								pendingBlockInventory.add(pendingBlock.getHash());
								
								for (BlockVote blockVote : pendingBlock.votes())
									blockVoteInventory.add(blockVote.getHash());
							}
							
							long height = BlockHandler.this.context.getLedger().getHead().getHeight();
							while (height > syncAcquiredMessage.getHead().getHeight())
							{
								pendingBlockInventory.addAll(BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, BlockHeader.class));
								blockVoteInventory.addAll(BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, BlockVote.class));
								height--;
							}
							
							int offset = 0;
							while(offset < pendingBlockInventory.size())
							{
								SyncInventoryMessage pendingBlockInventoryMessage = new SyncInventoryMessage(pendingBlockInventory, offset, Math.min(offset+BroadcastInventoryMessage.MAX_ITEMS, pendingBlockInventory.size()), BlockHeader.class);
								BlockHandler.this.context.getNetwork().getMessaging().send(pendingBlockInventoryMessage, peer);
								offset += BroadcastInventoryMessage.MAX_ITEMS; 
							}

							offset = 0;
							while(offset < blockVoteInventory.size())
							{
								SyncInventoryMessage blockVoteInventoryMessage = new SyncInventoryMessage(blockVoteInventory, offset, Math.min(offset+BroadcastInventoryMessage.MAX_ITEMS, blockVoteInventory.size()), BlockVote.class);
								BlockHandler.this.context.getNetwork().getMessaging().send(blockVoteInventoryMessage, peer);
								offset += BroadcastInventoryMessage.MAX_ITEMS; 
							}
						}
						catch (Exception ex)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block.get.pool " + peer, ex);
						}
						finally
						{
							BlockHandler.this.lock.readLock().unlock();
						}
					}
				});
			}
		});

		this.context.getEvents().register(this.syncChangeListener);
		this.context.getEvents().register(this.asyncBlockListener);

		Thread blockProcessorThread = new Thread(this.blockProcessor);
		blockProcessorThread.setDaemon(true);
		blockProcessorThread.setName(this.context.getName()+" Block Processor");
		blockProcessorThread.start();
		
		Thread voteProcessorThread = new Thread(this.voteProcessor);
		voteProcessorThread.setDaemon(true);
		voteProcessorThread.setName(this.context.getName()+" Block Vote Processor");
		voteProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException
	{
		this.voteProcessor.terminate(true);
		this.blockProcessor.terminate(true);
		this.context.getEvents().unregister(this.asyncBlockListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
		
		this.blockRegulator.stop();
	}
	
	public int size()
	{
		return this.pendingBlocks.size();
	}
	
	private boolean push(final BlockHeader header) throws IOException
	{
		Objects.requireNonNull(header, "Block header is null");
		BlockHandler.this.lock.writeLock().lock();
		try
		{
			if (header.getHeight() <= BlockHandler.this.context.getLedger().getHead().getHeight())
			{
				blocksLog.warn(BlockHandler.this.context.getName()+": Block header is old "+header);
				return false;
			}
			
			long blockShardGroup = ShardMapper.toShardGroup(header.getOwner(), BlockHandler.this.context.getLedger().numShardGroups(header.getHeight()));;
			long localShardGroup = ShardMapper.toShardGroup(BlockHandler.this.context.getNode().getIdentity(), BlockHandler.this.context.getLedger().numShardGroups(header.getHeight()));
			if (blockShardGroup != localShardGroup)
			{
				blocksLog.warn(BlockHandler.this.context.getName()+": Block header is for shard group "+blockShardGroup+" but expected local shard group "+localShardGroup);
				// TODO disconnect and ban;
				return false;
			}
					
			if (blocksLog.hasLevel(Logging.DEBUG) == true)
				blocksLog.debug(BlockHandler.this.context.getName()+": Block header "+header.getHash());
						
			return BlockHandler.this.upsertBlock(header);
		}
		finally
		{
			BlockHandler.this.lock.writeLock().unlock();
		}
	}
	
	private boolean process(final BlockVote blockVote) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(blockVote, "Block vote is null");
		
		if (blockVote.verify(blockVote.getOwner()) == false)
		{
			blocksLog.error(BlockHandler.this.context.getName()+": Block vote failed verification for "+blockVote.getOwner());
			return false;
		}

		// If the block is already committed as state then just skip
		final StateAddress blockStateAddress = new StateAddress(Block.class, blockVote.getBlock());
		if (BlockHandler.this.context.getLedger().getLedgerStore().search(blockStateAddress) == null)
		{
			BlockHandler.this.upsertBlock(blockVote.getBlock());
			PendingBlock pendingBlock = BlockHandler.this.getBlock(blockVote.getBlock());
			// If already have a vote recorded for the identity, then vote was already seen possibly selected and broadcast
			if (pendingBlock.voted(blockVote.getOwner()) == false)
			{
				if (blocksLog.hasLevel(Logging.DEBUG) == true)
					blocksLog.debug(BlockHandler.this.context.getName()+": Block vote "+blockVote.getHash()+"/"+blockVote.getBlock()+" for "+blockVote.getOwner());

				// TODO using pendingBlock.getHeader().getHeight() as the vote power timestamp possibly makes this weakly subjective and may cause issue in long branches
				pendingBlock.vote(blockVote, BlockHandler.this.votePowerHandler.getVotePower(Math.max(0, pendingBlock.getHeight() - VotePowerHandler.VOTE_POWER_MATURITY), blockVote.getOwner()));
			}
			
			return true;
		}
		
		return false;
	}
	
	private List<BlockVote> vote(final PendingBranch branch) throws IOException, CryptoException, ValidationException
	{
		// TODO using pendingBlock.getHeader().getHeight() as the vote power timestamp possibly makes this weakly subjective and may cause issue in long branches
		long votePower = BlockHandler.this.votePowerHandler.getVotePower(Math.max(0, branch.getHigh().getHeight() - VotePowerHandler.VOTE_POWER_MATURITY), BlockHandler.this.context.getNode().getIdentity());

		List<BlockVote> branchVotes = new ArrayList<BlockVote>();
		synchronized(BlockHandler.this.voteClock)
		{
			Iterator<PendingBlock> blockIterator = branch.getBlocks().iterator();
			while(blockIterator.hasNext())
			{
				PendingBlock pendingBlock = blockIterator.next();
				if (pendingBlock.getBlock() == null)
					break;
				
				if (BlockHandler.this.voteClock.get() < pendingBlock.getHeader().getHeight())
				{
					BlockHandler.this.voteClock.set(pendingBlock.getHeader().getHeight());
					BlockHandler.this.currentVote.set(pendingBlock.getHeader());
					
					if (votePower > 0)
					{
						BlockVote blockHeaderVote = new BlockVote(pendingBlock.getHeader().getHash(), BlockHandler.this.voteClock.get(), BlockHandler.this.context.getNode().getIdentity());
						blockHeaderVote.sign(BlockHandler.this.context.getNode().getKey());
						pendingBlock.vote(blockHeaderVote, votePower);
						this.context.getLedger().getLedgerStore().store(blockHeaderVote);
						this.context.getNetwork().getGossipHandler().broadcast(blockHeaderVote);
						
						branchVotes.add(blockHeaderVote);
						
						if (blocksLog.hasLevel(Logging.DEBUG) == true)
						{
							if (pendingBlock.getHeader().getOwner().equals(BlockHandler.this.context.getNode().getIdentity()) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Voted on own block "+pendingBlock+" "+blockHeaderVote.getHash());
							else
								blocksLog.debug(BlockHandler.this.context.getName()+": Voted on block "+pendingBlock+" "+blockHeaderVote.getHash());
						}
					}
				}
			}
		}
		
		return branchVotes;
	}
	
	private PendingBlock build(final BlockHeader head, final PendingBranch branch) throws IOException
	{
		final Set<Hash> branchCertificateExclusions = (branch == null || branch.isEmpty() == true) ? Collections.emptySet() : new HashSet<Hash>();
		if (branch != null && branch.isEmpty() == false)
		{
			for (PendingBlock block : branch.getBlocks())
				branchCertificateExclusions.addAll(block.getHeader().getInventory(InventoryType.CERTIFICATES));
			
			if (branch != null && branch.isEmpty() == false && head.equals(branch.getHigh().getHeader()) == false)
				throw new IllegalArgumentException("Head is not top of branch "+head);
		}
		
		final BlockHeader previous = head;
		final long initialAtomTarget = (this.context.getNode().getIdentity().asHash().asLong()+previous.getHash().asLong()); 

		PendingBlock strongestBlock = null;
		for (int i=0 ; i < this.context.getConfiguration().get("ledger.accumulator.iterations", 1) ; i++)
		{
			this.context.getMetaData().increment("ledger.accumulator.iterations");

			StateAccumulator accumulator = branch != null ? branch.getStateAccumulator().shadow() : this.context.getLedger().getStateAccumulator().shadow();
			Set<Hash> atomExclusions = accumulator.getPendingAtoms().stream().map(pa -> pa.getHash()).collect(Collectors.toSet());
			List<Hash> certificateExclusions = new ArrayList<Hash>(branchCertificateExclusions);
			final long timestamp = Time.getSystemTime();
			final List<PendingAtom> seedAtoms = this.context.getLedger().getAtomPool().get(initialAtomTarget-Long.MIN_VALUE, initialAtomTarget, BlockRegulator.BASELINE_DISTANCE_TARGET, 8, atomExclusions);
			final Collection<AtomCertificate> candidateCertificates = this.context.getLedger().getStateHandler().get(BlockHeader.MAX_ATOMS, certificateExclusions);
			long nextAtomTarget = initialAtomTarget;
			Hash stepHash = Hash.from(Hash.from(previous.getHeight()+1), previous.getHash(), this.context.getNode().getIdentity().asHash(), Hash.from(timestamp));

			final Map<Hash, PendingAtom> candidateAtoms = new LinkedHashMap<Hash, PendingAtom>();
			Collections.shuffle(seedAtoms);
			List<PendingAtom> atoms = seedAtoms;
			boolean foundAtom = false;
			do
			{
				if (nextAtomTarget != initialAtomTarget)
				{
					// Should only select atoms from the pool that have 2/3 agreement
					atoms = this.context.getLedger().getAtomPool().get(nextAtomTarget-Long.MIN_VALUE, nextAtomTarget, BlockRegulator.BASELINE_DISTANCE_TARGET, 16, atomExclusions);
					if (atoms.isEmpty() == true)
						break;
					
					// Sort by aged
					atoms.sort(new Comparator<PendingAtom>() 
					{
						@Override
						public int compare(PendingAtom arg0, PendingAtom arg1)
						{
							if (arg0.getCertificates().size() > arg1.getCertificates().size())
								return -1;

							if (arg0.getCertificates().size() < arg1.getCertificates().size())
								return 1;
							
							return (int) (arg0.getWitnessed() - arg1.getWitnessed());
						}
					});
				}

				foundAtom = false;
				for (PendingAtom atom : atoms)
				{
					// Discover an atom in range of nextTarget
					if (withinRange(atom.getHash().asLong(), nextAtomTarget, BlockRegulator.BASELINE_DISTANCE_TARGET) == true)
					{
						try
						{
							accumulator.lock(atom);
							atomExclusions.add(atom.getHash());
							candidateAtoms.put(atom.getHash(), atom);
							nextAtomTarget = atom.getHash().asLong();
							foundAtom = true;
							
							stepHash = new Hash(stepHash, atom.getHash(), Mode.STANDARD);
							long blockStep = MathUtils.ringDistance64(previous.getHash().asLong(), stepHash.asLong());
							long blockTarget = this.blockRegulator.computeTarget(head, branch); 
							// Try to build a block // TODO inefficient
							if (blockStep >= blockTarget)
							{
								Block discoveredBlock = new Block(previous.getHeight()+1, previous.getHash(), blockTarget, previous.getStepped(), previous.getNextIndex(), timestamp, this.context.getNode().getIdentity(), 
																  candidateAtoms.values().stream().map(pa -> pa.getAtom()).collect(Collectors.toList()), candidateCertificates);
								
								if (discoveredBlock.getHeader().getStep() != blockStep)
									throw new IllegalStateException("Step of generated block is "+discoveredBlock.getHeader().getStep()+" expected "+blockStep);
								
								if (strongestBlock == null || strongestBlock.getHeader().getInventory(InventoryType.ATOMS).size() < discoveredBlock.getHeader().getInventory(InventoryType.ATOMS).size())
									strongestBlock = new PendingBlock(BlockHandler.this.context, discoveredBlock.getHeader(), candidateAtoms.values(), candidateCertificates);
							}
							
							break;
						}
						catch (Exception ex)
						{
							// Skip the reporting of these here, they are reported on a discard event
							if ((ex instanceof StateLockedException) == false)
								blocksLog.error(this.context.getName(), ex);
										
							this.context.getEvents().post(new AtomExceptionEvent(atom, ex));
						}
					}
				}
			}
			while(foundAtom == true && candidateAtoms.size() < BlockHeader.MAX_ATOMS);
		}
		
		if (strongestBlock != null)
			this.context.getLedger().getLedgerStore().store(strongestBlock.getBlock());
		
		return strongestBlock;
	}
	
	private Collection<PendingBlock> commit(final PendingBlock block, final PendingBranch branch) throws IOException, StateLockedException
	{
		Objects.requireNonNull(block, "Pending block is null");
		Objects.requireNonNull(branch, "Pending branch is null");
		
		BlockHandler.this.lock.writeLock().lock();
		try
		{
			final LinkedList<PendingBlock> committedBlocks = branch.commit(block);
			if (committedBlocks.isEmpty() == false)
			{
				for (PendingBlock committedBlock : committedBlocks)
					blocksLog.info(BlockHandler.this.context.getName()+": Committed block "+committedBlock.getHeader());
	
				if (this.bestBranch != null && this.bestBranch.isEmpty() == true)
					this.bestBranch = null;
				
				// Signal the commit
				for (PendingBlock committedBlock : committedBlocks)
				{
					BlockCommittedEvent blockCommittedEvent = new BlockCommittedEvent(committedBlock.getBlock());
					BlockHandler.this.context.getEvents().post(blockCommittedEvent); // TODO Might need to catch exceptions on this from synchronous listeners
				}
			}
			
			return committedBlocks;
		}
		finally
		{
			BlockHandler.this.lock.writeLock().unlock();
		}
	}
	
	public Collection<PendingBranch> getPendingBranches()
	{
		this.lock.readLock().lock();
		try
		{
			List<PendingBranch> pendingBranches = new ArrayList<PendingBranch>(this.pendingBranches);
			pendingBranches.sort(new Comparator<PendingBranch>() 
			{
				@Override
				public int compare(PendingBranch arg0, PendingBranch arg1)
				{
					return arg0.getHigh().getHash().compareTo(arg1.getHigh().getHash());
				}
			});
			return pendingBranches;
		}
		finally
		{
			this.lock.readLock().unlock();
		}			
	}
	
	private void updateBlocks()
	{
		this.lock.writeLock().lock();
		try
		{
			// Not very efficient, but simple
			Iterator<PendingBlock> pendingBlockIterator = this.pendingBlocks.values().iterator();
			while (pendingBlockIterator.hasNext() == true)
			{
				PendingBlock pendingBlock  = pendingBlockIterator.next();
				if (pendingBlock.getHeader() == null)
					continue;
					
				if (pendingBlock.getBlock() == null)
				{
					try
					{
						for (Hash atomHash : pendingBlock.getHeader().getInventory(InventoryType.ATOMS))
						{
							if (pendingBlock.containsAtom(atomHash) == false)
							{
								PendingAtom pendingAtom = BlockHandler.this.context.getLedger().getAtomPool().get(atomHash);
								if (pendingAtom != null)
									pendingBlock.putAtom(pendingAtom);
							}
						}
		
						for (Hash certificateHash : pendingBlock.getHeader().getInventory(InventoryType.CERTIFICATES))
						{
							if (pendingBlock.containsCertificate(certificateHash) == false)
							{
								AtomCertificate certificate = BlockHandler.this.context.getLedger().getStateHandler().getCertificate(certificateHash, AtomCertificate.class);
								if (certificate != null)
									pendingBlock.putCertificate(certificate);
							}
						}
	
						pendingBlock.constructBlock();
						if (pendingBlock.getBlock() != null)
							BlockHandler.this.context.getLedger().getLedgerStore().store(pendingBlock.getBlock());
					}
					catch (Exception e)
					{
						blocksLog.error(BlockHandler.this.context.getName()+": Block update of "+pendingBlock+" failed", e);
						pendingBlockIterator.remove();
					}
				}
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	@VisibleForTesting
	boolean upsertBlock(final Hash block)
	{
		Objects.requireNonNull(block, "Block hash is null");
		Hash.notZero(block, "Block hash is ZERO");
		
		this.lock.writeLock().lock();
		try
		{
			if (this.pendingBlocks.containsKey(block) == false)
			{
				PendingBlock pendingBlock = new PendingBlock(BlockHandler.this.context, block);
				this.pendingBlocks.put(block, pendingBlock);
				return true;
			}
			
			return false;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	@VisibleForTesting
	boolean upsertBlock(final BlockHeader header) throws IOException
	{
		Objects.requireNonNull(header, "Pending block is null");
		
		this.lock.writeLock().lock();
		try
		{
			boolean upsert = false;
			PendingBlock pendingBlock = this.pendingBlocks.get(header.getHash());
			if (pendingBlock == null)
			{
				pendingBlock = new PendingBlock(BlockHandler.this.context, header.getHash());
				this.pendingBlocks.put(header.getHash(), pendingBlock);
				upsert = true;
			}
			
			if (pendingBlock.getHeader() == null)
			{
				pendingBlock.setHeader(header);
				BlockHandler.this.context.getLedger().getLedgerStore().store(pendingBlock.getHeader());
				upsert = true;
			}

			return upsert;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	@VisibleForTesting
	boolean upsertBlock(final PendingBlock pendingBlock)
	{
		Objects.requireNonNull(pendingBlock, "Pending block is null");
		if (pendingBlock.getHeader() == null)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" does not have a header");
		
		this.lock.writeLock().lock();
		try
		{
			if (this.pendingBlocks.containsKey(pendingBlock.getHash()) == false)
			{
				this.pendingBlocks.put(pendingBlock.getHash(), pendingBlock);
				return true;
			}

			return false;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	@VisibleForTesting
	PendingBlock getBlock(Hash block)
	{
		Objects.requireNonNull(block, "Block hash is null");
		Hash.notZero(block, "Block hash is ZERO");

		this.lock.readLock().lock();
		try
		{
			return this.pendingBlocks.get(block);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	@VisibleForTesting
	void updateBranches()
	{
		this.lock.writeLock().lock();
		try
		{
			// FIXME Next section WAS in the commit function, but suspect that upsertBlock() is waiting on the lock and inserting old blocks AFTER the commit happened.
			//		 This causes a "Branch doesn't attach to ledger" issue which breaks liveness ... moving here to test the case is true.  
			//		 Constantly checking and maintaining the old branch segments is crude, would like a proper fix
			final BlockHeader head = this.context.getLedger().getHead();
			// Clean or trim pending branches as a result of this commit
			Iterator<PendingBranch> pendingBranchIterator = BlockHandler.this.pendingBranches.iterator();
			while(pendingBranchIterator.hasNext() == true)
			{
				PendingBranch pendingBranch = pendingBranchIterator.next();
				
//				if (pendingBranch.equals(branch) == false)
//					pendingBranch.trimTo(committedBlocks.getLast());

				if (pendingBranch.isEmpty() == false && pendingBranch.getLow().getHeight() <= head.getHeight())
					pendingBranch.trimTo(head);
				
				if (pendingBranch.isEmpty() == true)
					pendingBranchIterator.remove();
			}
			
			if (this.bestBranch != null && this.bestBranch.isEmpty() == true)
				this.bestBranch = null;
			
			// Clear out pending of blocks that may not be in a branch and can't be committed due to this commit
			Iterator<Entry<Hash, PendingBlock>> pendingBlocksIterator = BlockHandler.this.pendingBlocks.entrySet().iterator();
			while(pendingBlocksIterator.hasNext() == true)
			{
				Entry<Hash, PendingBlock> pendingBlockEntry = pendingBlocksIterator.next();
				if (pendingBlockEntry.getValue().getHeight() <= head.getHeight())
					pendingBlocksIterator.remove();
			}

			for (PendingBlock pendingBlock : this.pendingBlocks.values())
			{
				if (pendingBlock.getHeader() != null && pendingBlock.isUnbranched() == true)
					updateBranches(pendingBlock);
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	@VisibleForTesting
	void updateBranches(final PendingBlock pendingBlock)
	{
		Objects.requireNonNull(pendingBlock, "Pending block is null");
		if (pendingBlock.getHeader() == null)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" does not have a header");
		
		if (pendingBlock.isUnbranched() == false)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" is already in a branch");

		this.lock.writeLock().lock();
		try
		{
			PendingBlock current = pendingBlock;
			LinkedList<PendingBlock> branch = new LinkedList<PendingBlock>();
			while(current != null)
			{
				branch.add(current);
				if (current.getHeader().getPrevious().equals(this.context.getLedger().getHead().getHash()) == true)
					break;
				
				PendingBlock previous = this.pendingBlocks.get(current.getHeader().getPrevious());
				if (previous != null && previous.getHeader() != null && previous.getBlock() != null)
					current = previous;
				else
					current = null;
			}

			if (branch.getLast().getHeader().getPrevious().equals(this.context.getLedger().getHead().getHash()) == false)
			{
				if (blocksLog.hasLevel(Logging.DEBUG) == true)
					blocksLog.debug(BlockHandler.this.context.getName()+": Branch for pending block "+pendingBlock.getHeader()+" does not terminate at ledger head "+this.context.getLedger().getHead().getHash()+" but at "+branch.getFirst().getHeader());
				
				return;
			}

			if (branch.isEmpty() == true)
				return;
			
			Collections.reverse(branch);
			
			try
			{
				for (PendingBranch pendingBranch : this.pendingBranches)
				{
					if (pendingBranch.merge(branch) == true)
						return;
				}

				for (PendingBranch pendingBranch : this.pendingBranches)
				{
					PendingBranch forkedBranch = pendingBranch.fork(branch);
					if (forkedBranch != null)
					{
						this.pendingBranches.add(forkedBranch);
						return;
					}
				}
					
				if (branch.getFirst().getHeader().getPrevious().equals(this.context.getLedger().getHead().getHash()) == true)
				{
					PendingBranch newBranch = new PendingBranch(this.context, Type.NONE, this.context.getLedger().getHead(), this.context.getLedger().getStateAccumulator().shadow(), branch);
					this.pendingBranches.add(newBranch);
				}
			}
/*			catch (ValidationException e)
			{
				blocksLog.error(BlockHandler.this.context.getName()+": Branch validation block "+pendingBlock+" failed", e);
			}*/
			catch (Exception e)
			{
				blocksLog.error(BlockHandler.this.context.getName()+": Branch injection / maintenence for block "+pendingBlock+" failed", e);
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	PendingBranch getBestBranch()
	{
		this.lock.readLock().lock();
		try
		{
			return this.bestBranch;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	private PendingBranch discoverBestBranch() throws IOException
	{
		this.lock.readLock().lock();
		try
		{
			PendingBranch bestBranch = null;
			Map<PendingBlock, PendingBranch> committable = new HashMap<PendingBlock, PendingBranch>();
			for (PendingBranch pendingBranch : this.pendingBranches)
			{
				if (pendingBranch.getLow().getHeader().getPrevious().equals(this.context.getLedger().getHead().getHash()) == false)
				{
					if (blocksLog.hasLevel(Logging.DEBUG) == true)
						blocksLog.debug(this.context.getName()+": Branch doesn't attach to ledger "+pendingBranch.getLow());
					
					continue;
				}

				// Short circuit on any branch that has a commit possible
				PendingBlock committableBlock = pendingBranch.commitable();
				if (committableBlock != null)
					committable.put(committableBlock, pendingBranch);
				
				// TODO need a lower probability tiebreaker here
				// TODO what happens if the branch has many un-constructable blocks which satisfy the below condition?
				//      possible situation for a liveness break?  is this an attack surface that needs to be countered?
				if (bestBranch == null || 
					bestBranch.getHigh().getHeader().getAverageStep() < pendingBranch.getHigh().getHeader().getAverageStep() ||
					(bestBranch.getHigh().getHeader().getAverageStep() == pendingBranch.getHigh().getHeader().getAverageStep() && bestBranch.getHigh().getHeader().getStep() < pendingBranch.getHigh().getHeader().getStep()))
				{
					bestBranch = pendingBranch;
					blocksLog.debug(BlockHandler.this.context.getName()+": Preselected branch "+bestBranch.getHigh().getHeader());
				}
			}

			// Have one or more committable branches.  Select the branch with the highest committable block.
			// TODO verify this can not be gamed!
			if (committable.isEmpty() == false)
			{
				blocksLog.debug(BlockHandler.this.context.getName()+": Discovered "+committable.size()+" committable branches "+committable);

				bestBranch = null;
				PendingBlock bestBlock = null;
				for (Entry<PendingBlock, PendingBranch> c : committable.entrySet())
				{
					// TODO need a lower probability tiebreaker here
					if (bestBlock == null || c.getKey().getHeight() > bestBlock.getHeight() || 
						(c.getKey().getHeight() == bestBlock.getHeight() && c.getKey().getHeader().getAverageStep() < bestBlock.getHeader().getAverageStep()))
					{
						bestBlock = c.getKey();
						bestBranch = c.getValue();
					}
				}
			}
	
			if (bestBranch != null)
			{
				if (blocksLog.hasLevel(Logging.DEBUG) == true)
					blocksLog.debug(BlockHandler.this.context.getName()+": Selected branch "+bestBranch.getHigh().getHeader().getAverageStep()+":"+bestBranch.getHigh().weight()+"/"+this.votePowerHandler.getTotalVotePower(Math.max(0, bestBranch.getHigh().getHeight() - VotePowerHandler.VOTE_POWER_MATURITY), ShardMapper.toShardGroup(BlockHandler.this.context.getNode().getIdentity(), BlockHandler.this.context.getLedger().numShardGroups()))+" "+bestBranch.getBlocks().stream().map(pb -> pb.getHash().toString()).collect(Collectors.joining(" -> ")));
			}
			
			return bestBranch;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	// ASYNC BLOCK LISTENER //
	private EventListener asyncBlockListener = new EventListener()
	{
		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent)
		{
			if (BlockHandler.this.voteClock.get() < blockCommittedEvent.getBlock().getHeader().getHeight())
			{
				BlockHandler.this.buildClock.set(blockCommittedEvent.getBlock().getHeader().getHeight());
				BlockHandler.this.voteClock.set(blockCommittedEvent.getBlock().getHeader().getHeight());
				BlockHandler.this.currentVote.set(null);
			}
		}
	};
	
	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncStatusChangeEvent event) 
		{
			BlockHandler.this.lock.writeLock().lock();
			try
			{
				if (event.isSynced() == true)
				{
					blocksLog.info(BlockHandler.this.context.getName()+": Sync status changed to "+event.isSynced()+", loading known block handler state");
					for (long height = Math.max(0, BlockHandler.this.context.getLedger().getHead().getHeight() - Node.OOS_TRIGGER_LIMIT) ; height <  BlockHandler.this.context.getLedger().getHead().getHeight() ; height++)
					{
						try
						{
							Collection<Hash> items = BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, BlockHeader.class);
							for (Hash item : items)
							{
								BlockHeader blockHeader = BlockHandler.this.context.getLedger().getLedgerStore().get(item, BlockHeader.class);
								if (blockHeader.getHeight() <=  BlockHandler.this.context.getLedger().getHead().getHeight())
									continue;
								
								BlockHandler.this.push(blockHeader);
							}
							
							items = BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, BlockVote.class);
							for (Hash item : items)
							{
								BlockVote blockVote = BlockHandler.this.context.getLedger().getLedgerStore().get(item, BlockVote.class);
								if (blockVote.getHeight() <= BlockHandler.this.context.getLedger().getHead().getHeight())
									continue;
								
								BlockHandler.this.votesToSyncQueue.put(blockVote.getHash(), blockVote);
							}
						}
						catch (IOException ex)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": Failed to load state for block handler at height "+height, ex);
						}
					}
				}
				else
				{
					blocksLog.info(BlockHandler.this.context.getName()+": Sync status changed to "+event.isSynced()+", flushing block handler");
					BlockHandler.this.bestBranch = null;
					BlockHandler.this.pendingBlocks.clear();
					BlockHandler.this.pendingBranches.clear();
					BlockHandler.this.votesToSyncQueue.clear();
					BlockHandler.this.votesToCountQueue.clear();
				}
			}
			finally
			{
				BlockHandler.this.lock.writeLock().unlock();
			}
		}
	};
}
