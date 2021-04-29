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
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
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
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.PendingBranch.Type;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.BlockCommitEvent;
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
import org.fuserleer.network.messages.SyncInventoryMessage;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.node.Node;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;
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
							processBranches();
							
							if (BlockHandler.this.buildBranch != null)
								buildCandidate = BlockHandler.this.buildBranch.getHigh().getHeader();
							else
								buildCandidate = BlockHandler.this.context.getLedger().getHead();

							// Vote on best branch?
							// If branches are getting long, then possible that blocks are being generated too fast and being voted on by the producing nodes.
							// A race condition can also occur with fast block production such that producers will generate the strongest branch they can see and always vote on it.
							// Defer the vote based on the size of the branch, giving generated block headers some time to propagate. 
							if (BlockHandler.this.buildBranch != null && BlockHandler.this.voteClock.get() < (buildCandidate.getHeight() - Math.log(BlockHandler.this.buildBranch.size())))
								vote(BlockHandler.this.buildBranch);
						}
						finally
						{
							BlockHandler.this.lock.writeLock().unlock();
						}
						
						PendingBlock generatedBlock = null;
						long generationStart = Time.getSystemTime();
						// Safe to attempt to build a block outside of a lock
						if (buildCandidate.getHeight() >= BlockHandler.this.buildClock.get())
							generatedBlock = BlockHandler.this.build(buildCandidate, BlockHandler.this.buildBranch);
						
						BlockHandler.this.lock.writeLock().lock();
						try
						{
							if (generatedBlock != null)
							{
								BlockHandler.this.insertBlock(generatedBlock);
								for (PendingAtom pendingAtom : generatedBlock.getAtoms())
									pendingAtom.lock();
								
								BlockHandler.this.buildClock.set(generatedBlock.getHeader().getHeight());
								this.generatedCount++;
								this.generatedTimeTotal += (Time.getSystemTime()-generationStart);
								blocksLog.info(BlockHandler.this.context.getName()+": Generated block "+generatedBlock.getHeader()+" in "+(Time.getSystemTime()-generationStart)+"ms / "+(this.generatedTimeTotal/this.generatedCount)+" ms average");
								BlockHandler.this.context.getNetwork().getGossipHandler().broadcast(generatedBlock.getHeader());
							}

							if (BlockHandler.this.commitBranch != null)
							{
								long commitStart = Time.getSystemTime();
								Collection<PendingBlock> committedBlocks = null;
								PendingBlock committable = BlockHandler.this.commitBranch.commitable();
								if (committable != null)
								{
									committedBlocks = commit(committable, BlockHandler.this.commitBranch);
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
						synchronized(BlockHandler.this.voteProcessor)
						{
							BlockHandler.this.voteProcessor.wait(TimeUnit.SECONDS.toMillis(1));
						}

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
									if (BlockHandler.this.context.getLedger().getLedgerStore().store(BlockHandler.this.context.getLedger().getHead().getHeight(), blockVote.getValue()).equals(OperationStatus.SUCCESS) == false)
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

	private final BlockRegulator				blockRegulator;

	private final Map<Hash, PendingBlock>		pendingBlocks;
	private final Set<PendingBranch>			pendingBranches;
	
	private final AtomicLong					voteClock;
	private final AtomicLong					buildClock;
	private final AtomicReference<BlockHeader> 	currentVote;
	private PendingBranch 						buildBranch;
	private PendingBranch 						commitBranch;

	private final MappedBlockingQueue<Hash, BlockVote> votesToSyncQueue;

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

	BlockHandler(Context context)
	{
		this.context = Objects.requireNonNull(context);
		this.pendingBlocks = Collections.synchronizedMap(new HashMap<Hash, PendingBlock>());
		this.pendingBranches = Collections.synchronizedSet(new HashSet<PendingBranch>());
		this.votesToSyncQueue = new MappedBlockingQueue<Hash, BlockVote>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.votesToCountQueue = new MappedBlockingQueue<Hash, BlockVote>(this.context.getConfiguration().get("ledger.state.queue", 1<<16));
		this.blockRegulator = new BlockRegulator(context);
		this.voteClock = new AtomicLong(0);
		this.buildClock = new AtomicLong(0);
		this.currentVote = new AtomicReference<BlockHeader>();
		this.buildBranch = null;
		this.commitBranch = null;
		
//		blocksLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
		blocksLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
//		blocksLog.setLevels(Logging.ERROR | Logging.FATAL);
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
				catch (ValidationException | CryptoException ex)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": Validation of block header "+blockHeader+" failed", ex);
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
				
				// Check existence of BlockVote ... primary cause of this evaluating to true is that 
				// the received BlockVote is the local nodes.
				// Syncing from a clean slate may result in the local node voting for a block in 
				// the pool, not knowing it already voted previously until it receives the vote from
				// a sync peer.  The duplicate will get caught in the votesToCountQueue processor
				// outputting a lot of warnings which is undesirable.
				if (BlockHandler.this.votesToCountQueue.contains(blockVote.getHash()) == true || 
					BlockHandler.this.context.getLedger().getLedgerStore().has(blockVote.getHash()) == true)
					return;

				BlockHandler.this.votesToCountQueue.put(blockVote.getHash(), blockVote);
				synchronized(BlockHandler.this.voteProcessor)
				{
					BlockHandler.this.voteProcessor.notify();
				}
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
							final Set<Hash> pendingBlockInventory = new LinkedHashSet<Hash>();
							final Set<Hash> blockVoteInventory = new LinkedHashSet<Hash>();
							
							for (PendingBlock pendingBlock : pendingBlocks)
							{
								pendingBlockInventory.add(pendingBlock.getHash());
								
								for (BlockVote blockVote : pendingBlock.votes())
									blockVoteInventory.add(blockVote.getHash());
							}
							
							long height = BlockHandler.this.context.getLedger().getHead().getHeight();
							while (height >= Math.max(0, syncAcquiredMessage.getHead().getHeight() - Node.OOS_RESOLVED_LIMIT))
							{
								pendingBlockInventory.addAll(BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, BlockHeader.class));
								blockVoteInventory.addAll(BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, BlockVote.class));
								height--;
							}
							
							while(pendingBlockInventory.isEmpty() == false)
							{
								SyncInventoryMessage pendingBlockInventoryMessage = new SyncInventoryMessage(pendingBlockInventory, 0, Math.min(BroadcastInventoryMessage.MAX_ITEMS, pendingBlockInventory.size()), BlockHeader.class);
								BlockHandler.this.context.getNetwork().getMessaging().send(pendingBlockInventoryMessage, peer);
								pendingBlockInventory.removeAll(pendingBlockInventoryMessage.getItems());
							}

							while(blockVoteInventory.isEmpty() == false)
							{
								SyncInventoryMessage blockVoteInventoryMessage = new SyncInventoryMessage(blockVoteInventory, 0, Math.min(BroadcastInventoryMessage.MAX_ITEMS, blockVoteInventory.size()), BlockVote.class);
								BlockHandler.this.context.getNetwork().getMessaging().send(blockVoteInventoryMessage, peer);
								blockVoteInventory.removeAll(blockVoteInventoryMessage.getItems());
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
	
	private boolean push(final BlockHeader header) throws IOException, ValidationException, CryptoException
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
				blocksLog.warn(this.context.getName()+": Block header is for shard group "+blockShardGroup+" but expected local shard group "+localShardGroup);
				// TODO disconnect and ban;
				return false;
			}
			
			if (header.verify(header.getOwner()) == false)
				throw new ValidationException("Signature is invalid for block header "+header);

			// Validate
			for (Hash atomHash : header.getInventory(InventoryType.ATOMS))
			{
				Commit commit = this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, atomHash));
				if (commit == null)
					continue;
				
				if (commit.isCommitTimedout() == true)
					throw new ValidationException("Atom "+atomHash+" is commit timed out in block "+header);
				if (commit.getPath().get(Elements.BLOCK) != null)
					throw new ValidationException("Atom "+atomHash+" is already accepted into block "+commit.getPath().get(Elements.BLOCK)+" but referenced in block "+header);
				if (commit.getPath().get(Elements.CERTIFICATE) != null)
					throw new ValidationException("Atom "+atomHash+" is already committed with certificate "+commit.getPath().get(Elements.CERTIFICATE)+" but referenced in block "+header);
			}

			// TODO validate certificates and referenced atoms
			/*
			for (Hash certificateHash : header.getInventory(InventoryType.CERTIFICATES))
			{
				AtomCertificate certificate = BlockHandler.this.context.getLedger().getStateHandler().getCertificate(certificateHash, AtomCertificate.class);
				if (certificate != null)
					continue;

				PendingAtom pendingAtom = this.context.getLedger().getAtomHandler().get(certificate.g);
				if (pendingAtom != null)
					continue;
				
				Commit commit = this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, atomHash));
				if (commit.isTimedout() == true)
					throw new ValidationException("Atom "+atomHash+" is timed out in block "+header);
				if (commit.getPath().get(Elements.BLOCK) != null)
					throw new ValidationException("Atom "+atomHash+" is already accepted into block "+commit.getPath().get(Elements.BLOCK)+" but referenced in block "+header);
				if (commit.getPath().get(Elements.CERTIFICATE) != null)
					throw new ValidationException("Atom "+atomHash+" is already committed with certificate "+commit.getPath().get(Elements.CERTIFICATE)+" but referenced in block "+header);
			}*/
					
			if (blocksLog.hasLevel(Logging.DEBUG) == true)
				blocksLog.debug(this.context.getName()+": Block header "+header.getHash());
						
			return this.upsertBlock(header);
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

		// If the block is already committed as state then just skip and silently return true
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

				pendingBlock.vote(blockVote);

				return true;
			}
			
			return false;
		}
		
		return true;
	}
	
	private List<BlockVote> vote(final PendingBranch branch) throws IOException, CryptoException, ValidationException
	{
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
					
					long votePower = branch.getVotePower(pendingBlock.getHeader().getHeight(), BlockHandler.this.context.getNode().getIdentity());
					if (votePower > 0)
					{
						BlockVote blockHeaderVote = new BlockVote(pendingBlock.getHeader().getHash(), BlockHandler.this.voteClock.get(), BlockHandler.this.context.getNode().getIdentity());
						blockHeaderVote.sign(BlockHandler.this.context.getNode().getKeyPair());
						pendingBlock.vote(blockHeaderVote);
						this.context.getLedger().getLedgerStore().store(BlockHandler.this.context.getLedger().getHead().getHeight(), blockHeaderVote);
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
	
	private PendingBlock build(final BlockHeader head, final PendingBranch branch) throws IOException, CryptoException
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

			final StateAccumulator accumulator = branch != null ? branch.getStateAccumulator().shadow() : this.context.getLedger().getStateAccumulator().shadow();
			final Set<Hash> atomExclusions = branch == null ? new HashSet<Hash>() : branch.getBlocks().stream().flatMap(pb -> pb.getHeader().getInventory(InventoryType.ATOMS).stream()).collect(Collectors.toSet());
			atomExclusions.addAll(accumulator.getPendingAtoms().stream().map(pa -> pa.getHash()).collect(Collectors.toSet()));
			
			final List<Hash> certificateExclusions = new ArrayList<Hash>(branchCertificateExclusions);
			final long timestamp = Time.getSystemTime();
			final List<PendingAtom> seedAtoms = this.context.getLedger().getAtomPool().get(initialAtomTarget-Long.MIN_VALUE, initialAtomTarget, BlockRegulator.BASELINE_DISTANCE_TARGET, 8, atomExclusions);
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
							
							return (int) (arg0.getWitnessedAt() - arg1.getWitnessedAt());
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
							// Hack as no current block header, but accumulator here is ephemeral anyway
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
								Block discoveredBlock;
								int blockSize = 0;
								Collection<AtomCertificate> candidateCertificates;
								int numCertificatesToInclude = BlockHeader.MAX_ATOMS;
								do
								{
									candidateCertificates = this.context.getLedger().getStateHandler().get(numCertificatesToInclude, certificateExclusions);
									discoveredBlock = new Block(previous.getHeight()+1, previous.getHash(), blockTarget, previous.getStepped(), previous.getNextIndex(), timestamp, this.context.getNode().getIdentity(), 
																candidateAtoms.values().stream().map(pa -> pa.getAtom()).collect(Collectors.toList()), candidateCertificates);
								
									if (discoveredBlock.getHeader().getStep() != blockStep)
										throw new IllegalStateException("Step of generated block is "+discoveredBlock.getHeader().getStep()+" expected "+blockStep);
									
									// FIXME bit of hack to keep block sizes below message limit until compressed / threshold signatures are in
									byte[] bytes = Serialization.getInstance().toDson(discoveredBlock, Output.WIRE);
									blockSize = bytes.length;
									if (blockSize > Message.MAX_MESSAGE_SIZE)
									{
										numCertificatesToInclude /= 2;
										blockSize = 0;
										blocksLog.warn(this.context.getName()+": Generated block with size "+bytes.length+" which exceeds maximum of "+Message.MAX_MESSAGE_SIZE+", adjusting included certificates");
									}
								}
								while(blockSize == 0);
								
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
		{
			strongestBlock.getHeader().sign(this.context.getNode().getKeyPair());
			this.context.getLedger().getLedgerStore().store(this.context.getLedger().getHead().getHeight(), strongestBlock.getBlock());
		}
		
		return strongestBlock;
	}
	
	private Collection<PendingBlock> commit(final PendingBlock block, final PendingBranch branch) throws IOException, StateLockedException, CryptoException
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
				{
					blocksLog.info(BlockHandler.this.context.getName()+": Committed block "+committedBlock.getHeader());
					
					// FIXME temporary for profiling BLS state certificates average size reduction on test baseline
					this.context.getMetaData().increment("ledger.blocks.bytes", Serialization.getInstance().toDson(committedBlock.getBlock(), Output.WIRE).length);
				}
	
				this.buildBranch = null;
				this.commitBranch = null;
				
				// Signal the commit
				// TODO Might need to catch exceptions on these from synchronous listeners
				for (PendingBlock committedBlock : committedBlocks)
				{
					BlockCommitEvent blockCommitEvent = new BlockCommitEvent(committedBlock.getBlock());
					BlockHandler.this.context.getEvents().post(blockCommitEvent);
				}

				for (PendingBlock committedBlock : committedBlocks)
				{
					BlockCommittedEvent blockCommittedEvent = new BlockCommittedEvent(committedBlock.getBlock());
					BlockHandler.this.context.getEvents().post(blockCommittedEvent);
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
								PendingAtom pendingAtom = BlockHandler.this.context.getLedger().getAtomHandler().get(atomHash);
								if (pendingAtom != null)
								{
									pendingAtom.lock();
									pendingBlock.putAtom(pendingAtom);
								}
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
							BlockHandler.this.context.getLedger().getLedgerStore().store(this.context.getLedger().getHead().getHeight(), pendingBlock.getBlock());
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
				BlockHandler.this.context.getLedger().getLedgerStore().store(BlockHandler.this.context.getLedger().getHead().getHeight(), pendingBlock.getHeader());
				upsert = true;
			}

			return upsert;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	/**
	 * Inserts blocks generated by the local validator.
	 * 
	 * Should be private but is visible for testing
	 * 
	 * @param pendingBlock
	 */
	@VisibleForTesting
	void insertBlock(final PendingBlock pendingBlock)
	{
		Objects.requireNonNull(pendingBlock, "Pending block is null");
		if (pendingBlock.getHeader() == null)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" does not have a header");
		
		if (pendingBlock.getBlock() == null)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" does not have a constructed block");
		
		if (pendingBlock.getHeader().getOwner().equals(this.context.getNode().getIdentity()) == false)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" is not generated by local validator "+this.context.getNode().getIdentity());

		this.lock.writeLock().lock();
		try
		{
			if (this.pendingBlocks.containsKey(pendingBlock.getHash()) == true)
				throw new IllegalStateException("Generated block "+pendingBlock.getHash()+" is already inserted");

			this.pendingBlocks.put(pendingBlock.getHash(), pendingBlock);
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
				
				if (pendingBranch.isEmpty() == false && pendingBranch.getLow().getHeight() <= head.getHeight())
					pendingBranch.trimTo(head);
				
				if (pendingBranch.isEmpty() == true)
					pendingBranchIterator.remove();
			}
			
			this.buildBranch = null;
			this.commitBranch = null;
			
			// Clear out pending of blocks that may not be in a branch and can't be committed due to this commit
			Iterator<Entry<Hash, PendingBlock>> pendingBlocksIterator = BlockHandler.this.pendingBlocks.entrySet().iterator();
			while(pendingBlocksIterator.hasNext() == true)
			{
				Entry<Hash, PendingBlock> pendingBlockEntry = pendingBlocksIterator.next();
				if (pendingBlockEntry.getValue().getHeight() <= head.getHeight())
				{
					for (PendingAtom pendingAtom : pendingBlockEntry.getValue().getAtoms())
						pendingAtom.unlock();
						
					pendingBlocksIterator.remove();
				}
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
					if (pendingBranch.isMergable(branch) == true)
					{
						pendingBranch.merge(branch);
						return;
					}
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

	PendingBranch getBuildBranch()
	{
		this.lock.readLock().lock();
		try
		{
			return this.buildBranch;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	PendingBranch getCommitBranch()
	{
		this.lock.readLock().lock();
		try
		{
			return this.commitBranch;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	private void processBranches() throws IOException, StateLockedException
	{
		this.lock.readLock().lock();
		try
		{
			PendingBranch buildBranch = null;
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
				// If not full constructed will commit the portion that is
				PendingBlock committableBlock = pendingBranch.commitable();
				if (committableBlock != null)
				{
					committable.put(committableBlock, pendingBranch);
					continue;
				}
				
				// Need fully constructed branches in order to select them!
				// NOTE disable this to promote a stall on liveness when critical gossip / connectivity issues
				if (pendingBranch.isConstructed() == false)
					continue;

				// TODO need a lower probability tiebreaker here
				// TODO what happens if the branch has many un-constructable blocks which satisfy the below condition?
				//      possible situation for a liveness break?  is this an attack surface that needs to be countered?
				if (buildBranch == null || 
					buildBranch.getHigh().getHeader().getAverageStep() < pendingBranch.getHigh().getHeader().getAverageStep() ||
					(buildBranch.getHigh().getHeader().getAverageStep() == pendingBranch.getHigh().getHeader().getAverageStep() && buildBranch.getHigh().getHeader().getStep() < pendingBranch.getHigh().getHeader().getStep()))
				{
					buildBranch = pendingBranch;
					blocksLog.debug(BlockHandler.this.context.getName()+": Preselected build branch "+buildBranch.getHigh().getHeader());
				}
			}
			
			this.buildBranch = buildBranch;

			// Process any committable branches.
			// Multiple committable branches is tricky.  It is allowed that validators can vote on any distinct branch at any time providing the vote clock is monotonic.
			// Therefore a validator can legally vote on branch A, then vote on branch B which is stronger.  
			// In the mean time, branch A became super-majority, but validator hasn't seen the votes that constitute it yet before voting on branch B.
			// If many validators are voting similarly with latency regarding vote processing, branch A *AND* branch B could become super-majority.
			// The solution is to require a minimum of 2 super-majority blocks per branch, with the latter acting as a confirmation chain of the first.
			// However this will add at least one additional block production of latency to block commits, but will most certainly be secure!
			// TODO can round of latency be optimised?
			if (committable.isEmpty() == false)
			{
				if (committable.size() == 1)
					blocksLog.debug(BlockHandler.this.context.getName()+": Discovered committable branch "+committable);
				else 
					blocksLog.debug(BlockHandler.this.context.getName()+": Discovered multiple committable branches "+committable);

				PendingBranch commitBranch = null;
				int	commitBranchSuperBlocks = 0;
				for (Entry<PendingBlock, PendingBranch> c : committable.entrySet())
				{
					int superBlocks = 0;
					for (PendingBlock pendingBlock : c.getValue().getBlocks())
					{
						long blockWeight = c.getValue().getWeight(pendingBlock.getHeight());
						long votePowerThresholdAtBlock = c.getValue().getVotePowerThreshold(pendingBlock.getHeight());
						
						if (blockWeight < votePowerThresholdAtBlock)
							continue;
						
						superBlocks++;
					}
					
					if (superBlocks > commitBranchSuperBlocks)
					{
						commitBranchSuperBlocks = superBlocks;
						commitBranch = c.getValue();
						blocksLog.debug(BlockHandler.this.context.getName()+": Preselected commit branch with "+commitBranchSuperBlocks+" supers "+commitBranch.getHigh().getHeader());
					}
					else if (superBlocks == commitBranchSuperBlocks)
						commitBranch = null;
				}
				
				if (commitBranch != null)
				{
					// Always select the best commit branch as the build branch
					this.buildBranch = commitBranch;

					if (commitBranchSuperBlocks >= 2)
					{
						this.commitBranch = commitBranch;
						blocksLog.debug(BlockHandler.this.context.getName()+": Selected commit branch with "+commitBranchSuperBlocks+" supers "+commitBranch.getHigh().getHeader());
					}
				}
			}
	
			if (this.buildBranch != null)
			{
				if (blocksLog.hasLevel(Logging.DEBUG) == true)
				{
					long branchWeight = this.buildBranch.getWeight(this.buildBranch.getHigh().getHeight());
					long branchTotalVotePower = this.buildBranch.getTotalVotePower(this.buildBranch.getHigh().getHeight());
					blocksLog.debug(BlockHandler.this.context.getName()+": Selected build branch "+this.buildBranch.getHigh().getHeader().getAverageStep()+":"+branchWeight+"/"+branchTotalVotePower+" "+this.buildBranch.getBlocks().stream().map(pb -> pb.getHash().toString()).collect(Collectors.joining(" -> ")));
				}
			}
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
					for (long height = Math.max(0, BlockHandler.this.context.getLedger().getHead().getHeight() - Node.OOS_TRIGGER_LIMIT) ; height <= BlockHandler.this.context.getLedger().getHead().getHeight() ; height++)
					{
						try
						{
							Collection<Hash> items = BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, BlockHeader.class);
							for (Hash item : items)
							{
								BlockHeader blockHeader = BlockHandler.this.context.getLedger().getLedgerStore().get(item, BlockHeader.class);
								if (blockHeader.getHeight() <= BlockHandler.this.context.getLedger().getHead().getHeight())
									continue;
								
								BlockHandler.this.push(blockHeader);
							}
						}
						catch (IOException | ValidationException | CryptoException ex)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": Failed to load block headers state for block handler at height "+height, ex);
						}
						
						try
						{
							Collection<Hash> items = BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(height, BlockVote.class);
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
							blocksLog.error(BlockHandler.this.context.getName()+": Failed to load block vote state for block handler at height "+height, ex);
						}
					}
				}
				else
				{
					blocksLog.info(BlockHandler.this.context.getName()+": Sync status changed to "+event.isSynced()+", flushing block handler");
					BlockHandler.this.buildBranch = null;
					BlockHandler.this.commitBranch = null;
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
