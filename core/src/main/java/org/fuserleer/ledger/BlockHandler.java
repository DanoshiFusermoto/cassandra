package org.fuserleer.ledger;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.EventListener;
import org.fuserleer.exceptions.DependencyNotFoundException;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.AtomErrorEvent;
import org.fuserleer.ledger.events.AtomPersistedEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.messages.BlockHeaderInventoryMessage;
import org.fuserleer.ledger.messages.BlockHeaderMessage;
import org.fuserleer.ledger.messages.BlockMessage;
import org.fuserleer.ledger.messages.BlockVoteInventoryMessage;
import org.fuserleer.ledger.messages.BlockVoteMessage;
import org.fuserleer.ledger.messages.GetBlockHeaderMessage;
import org.fuserleer.ledger.messages.GetBlockMessage;
import org.fuserleer.ledger.messages.GetBlockVoteMessage;
import org.fuserleer.ledger.messages.InventoryMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.node.Node;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.utils.MathUtils;

import com.google.common.eventbus.Subscribe;
import com.google.common.primitives.Longs;
import com.sleepycat.je.OperationStatus;

public class BlockHandler implements Service
{
	private static final Logger blocksLog = Logging.getLogger("blocks");

	public final static long 	BASELINE_DISTANCE_FACTOR = 4;
	public final static long 	BASELINE_DISTANCE_TARGET = BigInteger.valueOf(Long.MIN_VALUE).abs().subtract(BigInteger.valueOf((long) (Long.MIN_VALUE / (BASELINE_DISTANCE_FACTOR * Math.log(BASELINE_DISTANCE_FACTOR)))).abs()).longValue(); // TODO rubbish, but produces close enough needed output
	public final static long 	BLOCK_DISTANCE_FACTOR = 256;
	public final static long 	BLOCK_DISTANCE_TARGET = BigInteger.valueOf(Long.MIN_VALUE).abs().subtract(BigInteger.valueOf((long) (Long.MIN_VALUE / (BLOCK_DISTANCE_FACTOR * Math.log(BLOCK_DISTANCE_FACTOR)))).abs()).longValue(); // TODO rubbish, but produces close enough needed output
	
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
		@Override
		public void execute()
		{
			try 
			{
				while (this.isTerminated() == false)
				{
					try
					{
						Thread.sleep(1000);
						
						if (BlockHandler.this.context.getLedger().isSynced() == false)
							continue;
						
						// TODO do clean up of old pending blocks / committed / orphans etc
						
						// Find the best block candidate to build on top of
						PendingBlock buildCandidate = null;
						
						BlockHandler.this.lock.lock();
						try
						{
							updateBranches();
							BlockHandler.this.bestBranch = getBestBranch();
							
							if (BlockHandler.this.bestBranch != null)
								buildCandidate = BlockHandler.this.bestBranch.getLast();
							else
								buildCandidate = new PendingBlock(BlockHandler.this.context, BlockHandler.this.context.getLedger().get(BlockHandler.this.context.getLedger().getHead().getHash(), Block.class));
						}
						finally
						{
							BlockHandler.this.lock.unlock();
						}
						
						BlockHandler.this.lock.lock();
						try
						{
							// Vote on best branch?
							if (BlockHandler.this.voteClock.get() < buildCandidate.getBlockHeader().getHeight())
								vote(BlockHandler.this.bestBranch);
						}
						finally
						{
							BlockHandler.this.lock.unlock();
						}
						
						final Block generatedBlock;
						// No one has produced a new block yet that that the local node has voted on, 
						// if havent successfully built one for the current round attempt to
						if (BlockHandler.this.lastGenerated.get() == null || BlockHandler.this.lastGenerated.get().getHeight() <= buildCandidate.getBlockHeader().getHeight())
							generatedBlock = BlockHandler.this.build(buildCandidate, BlockHandler.this.bestBranch);
						else 
							generatedBlock = null;
						
						BlockHandler.this.lock.lock();
						try
						{
							if (generatedBlock != null && BlockHandler.this.pendingBlocks.containsKey(generatedBlock.getHash()) == false)
							{
								PendingBlock pendingBlock = new PendingBlock(BlockHandler.this.context, generatedBlock);
								BlockHandler.this.pendingBlocks.put(generatedBlock.getHash(), pendingBlock);
								BlockHandler.this.lastGenerated.set(generatedBlock.getHeader());
								blocksLog.info(BlockHandler.this.context.getName()+": Generated block "+generatedBlock.getHeader());
								broadcast(generatedBlock.getHeader());
							}
						
							if (BlockHandler.this.bestBranch != null)
							{
								PendingBlock committable = BlockHandler.this.bestBranch.commitable();
								if (committable != null)
									commit(committable, BlockHandler.this.bestBranch);
							}
						}
						finally
						{
							BlockHandler.this.lock.unlock();
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

	private final Context context;
	private final ReentrantLock lock = new ReentrantLock(true);

	private final VoteRegulator 				voteRegulator;
	private final Map<Hash, PendingBlock>		pendingBlocks;
	private final Set<PendingBranch>			pendingBranches;
	
	private final AtomicLong					voteClock;
	private final AtomicReference<BlockHeader> 	currentVote;
	private final AtomicReference<BlockHeader> 	lastGenerated;
	private PendingBranch 						bestBranch;

	private final BlockingQueue<Hash> blockInventory = new LinkedBlockingQueue<Hash>();
	private final Executable blockInventoryProcessor = new Executable()
	{
		@Override
		public void execute()
		{
			while(isTerminated() == false)
			{
				try
				{
					Hash hash = BlockHandler.this.blockInventory.poll(1, TimeUnit.SECONDS);
					if (hash == null)
						continue;
					
					List<Hash> blockInventory = new ArrayList<Hash>();
					blockInventory.add(hash);
					BlockHandler.this.blockInventory.drainTo(blockInventory, InventoryMessage.MAX_INVENTORY-1);
					
					BlockHeaderInventoryMessage blockHeaderInventoryMessage = new BlockHeaderInventoryMessage(blockInventory);
					for (ConnectedPeer connectedPeer : BlockHandler.this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED))
					{
						if (BlockHandler.this.context.getNode().isInSyncWith(connectedPeer.getNode(), Node.OOS_TRIGGER_LIMIT) == false)
							continue;
								
						for (Hash blockHash : blockHeaderInventoryMessage.getInventory())
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Broadcasting block header "+blockHash+" to "+connectedPeer);
						}
	
						try
						{
							BlockHandler.this.context.getNetwork().getMessaging().send(blockHeaderInventoryMessage, connectedPeer);
						}
						catch (IOException ex)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": Unable to send BlockHeaderInventoryMessage of "+blockHeaderInventoryMessage.getInventory().size()+" block headers to "+connectedPeer, ex);
						}
					}
				}
				catch (Exception ex)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": Processing of block header inventory failed", ex);
				}
			}
		}
	};
			
	private final BlockingQueue<Hash> blockVoteInventory = new LinkedBlockingQueue<Hash>();
	private final Executable blockVoteInventoryProcessor = new Executable()
	{
		@Override
		public void execute()
		{
			while(isTerminated() == false)
			{
				try
				{
					Hash hash = BlockHandler.this.blockVoteInventory.poll(1, TimeUnit.SECONDS);
					if (hash == null)
						continue;
					
					List<Hash> blockVoteInventory = new ArrayList<Hash>();
					blockVoteInventory.add(hash);
					BlockHandler.this.blockVoteInventory.drainTo(blockVoteInventory, InventoryMessage.MAX_INVENTORY-1);
					
					BlockVoteInventoryMessage blockVoteInventoryMessage = new BlockVoteInventoryMessage(blockVoteInventory);
					for (ConnectedPeer connectedPeer : BlockHandler.this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED))
					{
						if (BlockHandler.this.context.getNode().isInSyncWith(connectedPeer.getNode(), Node.OOS_TRIGGER_LIMIT) == false)
							continue;
								
						for (Hash blockVoteHash : blockVoteInventoryMessage.getInventory())
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Broadcasting block vote "+blockVoteHash+" to "+connectedPeer);
						}
		
						try
						{
							BlockHandler.this.context.getNetwork().getMessaging().send(blockVoteInventoryMessage, connectedPeer);
						}
						catch (IOException ex)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": Unable to send BlockVoteInventoryMessage of "+blockVoteInventoryMessage.getInventory().size()+" block votes to "+connectedPeer, ex);
						}
					}
				}
				catch (Exception ex)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": Processing of block vote inventory failed", ex);
				}
			}
		}
	};

	BlockHandler(Context context, VoteRegulator voteRegulator)
	{
		this.context = Objects.requireNonNull(context);
		this.pendingBlocks = Collections.synchronizedMap(new HashMap<Hash, PendingBlock>());
		this.pendingBranches = Collections.synchronizedSet(new HashSet<PendingBranch>());
		this.voteRegulator = Objects.requireNonNull(voteRegulator, "Vote regulator is null");
		this.voteClock = new AtomicLong(0);
		this.currentVote = new AtomicReference<BlockHeader>();
		this.lastGenerated = new AtomicReference<BlockHeader>();
		this.bestBranch = null;
	}

	@Override
	public void start() throws StartupException
	{
		this.voteClock.set(this.context.getLedger().getHead().getHeight());

		this.context.getNetwork().getMessaging().register(BlockHeaderMessage.class, this.getClass(), new MessageProcessor<BlockHeaderMessage>()
		{
			@Override
			public void process(final BlockHeaderMessage blockHeaderMessage, final ConnectedPeer peer)
			{
				BlockHandler.this.lock.lock();
				try
				{
					if (blockHeaderMessage.getBlockHeader().getHeight() <= BlockHandler.this.context.getLedger().getHead().getHeight())
					{
						blocksLog.warn(BlockHandler.this.context.getName()+": Block header is old "+blockHeaderMessage.getBlockHeader()+" from "+peer);
						BlockHandler.this.pendingBlocks.remove(blockHeaderMessage.getBlockHeader().getHash());
						return;
					}

					if (BlockHandler.this.context.getLedger().getLedgerStore().has(blockHeaderMessage.getBlockHeader().getHash()) == false)
					{
						if (blocksLog.hasLevel(Logging.DEBUG) == true)
							blocksLog.debug(BlockHandler.this.context.getName()+": Block header "+blockHeaderMessage.getBlockHeader().getHash()+" from "+peer);
							
						// Try to build the block from known atoms and certificates
						List<Atom> atoms = new ArrayList<Atom>();
						for (Hash atomHash : blockHeaderMessage.getBlockHeader().getInventory(InventoryType.ATOMS))
						{
							Atom atom = BlockHandler.this.context.getLedger().get(atomHash, Atom.class);
							if (atom == null)
								break;
							
							atoms.add(atom);
						}

						List<AtomCertificate> certificates = new ArrayList<AtomCertificate>();
						for (Hash certificateHash : blockHeaderMessage.getBlockHeader().getInventory(InventoryType.CERTIFICATES))
						{
							AtomCertificate certificate = BlockHandler.this.context.getLedger().get(certificateHash, AtomCertificate.class);
							if (certificate == null)
								break;
							
							certificates.add(certificate);
						}

						if (atoms.size() == blockHeaderMessage.getBlockHeader().getInventory(InventoryType.ATOMS).size() && 
							certificates.size() == blockHeaderMessage.getBlockHeader().getInventory(InventoryType.CERTIFICATES).size())
						{
							Block block = new Block(blockHeaderMessage.getBlockHeader(), atoms, certificates);

							// TODO needs block verification
							
							BlockHandler.this.context.getLedger().getLedgerStore().store(block);

							PendingBlock pendingBlock = BlockHandler.this.pendingBlocks.get(block.getHeader().getHash());
							if (pendingBlock == null)
							{
								pendingBlock = new PendingBlock(BlockHandler.this.context, block);
								BlockHandler.this.pendingBlocks.put(block.getHeader().getHash(), pendingBlock);
							}
							else
								pendingBlock.setBlock(block);

							BlockHandler.this.blockInventory.add(pendingBlock.getHash());
						}
						else
						{
							// Cant build it, need an explicit request
							try
							{
								BlockHandler.this.context.getNetwork().getMessaging().send(new GetBlockMessage(blockHeaderMessage.getBlockHeader().getHash()), peer);
							}
							catch (IOException ex)
							{
								blocksLog.error(BlockHandler.this.context.getName()+": Unable to send GetBlockMessage for "+blockHeaderMessage.getBlockHeader().getHash()+" to "+peer, ex);
							}
						}
					}
				}
				catch (Exception ex)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block.header "+peer, ex);
				}
				finally
				{
					BlockHandler.this.lock.unlock();
				}
			}
		});

		this.context.getNetwork().getMessaging().register(BlockMessage.class, this.getClass(), new MessageProcessor<BlockMessage>()
		{
			@Override
			public void process(final BlockMessage blockMessage, final ConnectedPeer peer)
			{
				BlockHandler.this.lock.lock();
				try
				{
					if (blockMessage.getBlock().getHeader().getHeight() <= BlockHandler.this.context.getLedger().getHead().getHeight())
					{
						blocksLog.warn(BlockHandler.this.context.getName()+": Block is old "+blockMessage.getBlock().getHeader()+" from "+peer);
						BlockHandler.this.pendingBlocks.remove(blockMessage.getBlock().getHeader().getHash());
						return;
					}

					if (BlockHandler.this.context.getLedger().getLedgerStore().has(blockMessage.getBlock().getHeader().getHash()) == false)
					{
						if (blocksLog.hasLevel(Logging.DEBUG) == true)
							blocksLog.debug(BlockHandler.this.context.getName()+": Block "+blockMessage.getBlock().getHeader().getHash()+" from "+peer);

						// TODO need a block validation / verification processor

						BlockHandler.this.context.getLedger().getLedgerStore().store(blockMessage.getBlock());
						
						PendingBlock pendingBlock = BlockHandler.this.pendingBlocks.get(blockMessage.getBlock().getHeader().getHash());
						if (pendingBlock == null)
						{
							pendingBlock = new PendingBlock(BlockHandler.this.context, blockMessage.getBlock());
							BlockHandler.this.pendingBlocks.put(blockMessage.getBlock().getHeader().getHash(), pendingBlock);
						}
						else
							pendingBlock.setBlock(blockMessage.getBlock());

						BlockHandler.this.blockInventory.add(pendingBlock.getHash());
					}
				}
				catch (Exception ex)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block "+peer, ex);
				}
				finally
				{
					BlockHandler.this.lock.unlock();
				}
			}
		});

		this.context.getNetwork().getMessaging().register(BlockVoteMessage.class, this.getClass(), new MessageProcessor<BlockVoteMessage>()
		{
			@Override
			public void process(final BlockVoteMessage blockVoteMessage, final ConnectedPeer peer)
			{
				BlockHandler.this.lock.lock();
				try
				{
					if (OperationStatus.KEYEXIST.equals(BlockHandler.this.context.getLedger().getLedgerStore().store(blockVoteMessage.getVote())) == false)
					{
						// Can extract the height from the hash at its prefixed
						long pendingBlockHeight = Longs.fromByteArray(blockVoteMessage.getVote().getObject().toByteArray());
						if (pendingBlockHeight > BlockHandler.this.context.getLedger().getHead().getHeight())
						{
							PendingBlock pendingBlock = BlockHandler.this.pendingBlocks.computeIfAbsent(blockVoteMessage.getVote().getObject(), (h) -> new PendingBlock(BlockHandler.this.context, blockVoteMessage.getVote().getObject()));
							// If already have a vote recorded for the identity, then vote was already seen possibly selected and broadcast
							if (pendingBlock.voted(blockVoteMessage.getVote().getOwner()) == false)
							{
								if (blocksLog.hasLevel(Logging.DEBUG) == true)
									blocksLog.debug(BlockHandler.this.context.getName()+": Block vote "+blockVoteMessage.getVote().getHash()+"/"+blockVoteMessage.getVote().getObject()+" for "+blockVoteMessage.getVote().getOwner()+" from "+peer);
		
								pendingBlock.vote(blockVoteMessage.getVote(), BlockHandler.this.voteRegulator.getVotePower(blockVoteMessage.getVote().getOwner()));
								BlockHandler.this.blockVoteInventory.add(blockVoteMessage.getVote().getHash());
							}
						}
					}
					else if (blocksLog.hasLevel(Logging.DEBUG) == true)
						blocksLog.debug(BlockHandler.this.context.getName()+": Received already seen block vote "+blockVoteMessage.getVote().getObject()+" for "+blockVoteMessage.getVote().getOwner()+" from "+peer);
				}
				catch (Exception ex)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block.vote "+peer, ex);
				}
				finally
				{
					BlockHandler.this.lock.unlock();
				}
			}
		});
		
		this.context.getNetwork().getMessaging().register(GetBlockVoteMessage.class, this.getClass(), new MessageProcessor<GetBlockVoteMessage>()
		{
			@Override
			public void process(final GetBlockVoteMessage getBlockVoteMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						BlockHandler.this.lock.lock();
						try
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Block vote request for "+getBlockVoteMessage.getInventory().size()+" votes from "+peer);
							
							if (getBlockVoteMessage.getInventory().size() == 0)
							{
								blocksLog.error(BlockHandler.this.context.getName()+": Received empty block votes request from " + peer);
								// TODO disconnect and ban
								return;
							}
							
							for (Hash blockVoteHash : getBlockVoteMessage.getInventory())
							{
								if (blocksLog.hasLevel(Logging.DEBUG) == true)
									blocksLog.debug(BlockHandler.this.context.getName()+": Request for block vote "+blockVoteHash+" for "+peer);

								BlockVote blockVote = BlockHandler.this.context.getLedger().getLedgerStore().get(blockVoteHash, BlockVote.class);
								if (blockVote == null)
								{
									if (blocksLog.hasLevel(Logging.DEBUG) == true)
										blocksLog.debug(BlockHandler.this.context.getName()+": Requested block vote "+blockVoteHash+" not found for "+peer);
									
									continue;
								}

								try
								{
									BlockHandler.this.context.getNetwork().getMessaging().send(new BlockVoteMessage(blockVote), peer);
								}
								catch (IOException ex)
								{
									blocksLog.error(BlockHandler.this.context.getName()+": Unable to send BlockVoteMessage for "+blockVoteHash+" to "+peer, ex);
								}
							}
						}
						catch (Exception ex)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block.vote.get " + peer, ex);
						}
						finally
						{
							BlockHandler.this.lock.unlock();
						}
					}
				});
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
						BlockHandler.this.lock.lock();
						try
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Block request for "+getBlockMessage.getBlock()+" votes from "+peer);
							
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
							BlockHandler.this.lock.unlock();
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(GetBlockHeaderMessage.class, this.getClass(), new MessageProcessor<GetBlockHeaderMessage>()
		{
			@Override
			public void process(final GetBlockHeaderMessage getBlockHeaderMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						BlockHandler.this.lock.lock();
						try
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Block header request for "+getBlockHeaderMessage.getInventory().size()+" headers from "+peer);
							
							if (getBlockHeaderMessage.getInventory().size() == 0)
							{
								blocksLog.error(BlockHandler.this.context.getName()+": Received empty block header request from " + peer);
								// TODO disconnect and ban
								return;
							}
							
							for (Hash blockHash : getBlockHeaderMessage.getInventory())
							{
								BlockHeader blockHeader = null;
								PendingBlock pendingBlock = BlockHandler.this.pendingBlocks.get(blockHash);
								if (pendingBlock != null)
									blockHeader = pendingBlock.getBlockHeader();
								else
									blockHeader = BlockHandler.this.context.getLedger().getLedgerStore().get(blockHash, BlockHeader.class);
								
								if (blockHeader == null)
								{
									if (blocksLog.hasLevel(Logging.DEBUG) == true)
										blocksLog.debug(BlockHandler.this.context.getName()+": Requested block header not found "+blockHash+" for "+peer);
									
									continue;
								}

								try
								{
									BlockHandler.this.context.getNetwork().getMessaging().send(new BlockHeaderMessage(blockHeader), peer);
								}
								catch (IOException ex)
								{
									blocksLog.error(BlockHandler.this.context.getName()+": Unable to send BlockHeaderMessage for "+blockHeader+" to "+peer, ex);
								}
							}
						}
						catch (Exception ex)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block.header.get " + peer, ex);
						}
						finally
						{
							BlockHandler.this.lock.unlock();
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(BlockVoteInventoryMessage.class, this.getClass(), new MessageProcessor<BlockVoteInventoryMessage>()
		{
			@Override
			public void process(final BlockVoteInventoryMessage blockVoteInventoryMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						BlockHandler.this.lock.lock();
						try
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Block votes inventory for "+blockVoteInventoryMessage.getInventory().size()+" votes from " + peer);

							if (blockVoteInventoryMessage.getInventory().size() == 0)
							{
								blocksLog.error(BlockHandler.this.context.getName()+": Received empty block votes inventory from " + peer);
								// TODO disconnect and ban
								return;
							}

							List<Hash> blockVoteInventoryRequired = new ArrayList<Hash>();
							for (Hash blockVoteHash : blockVoteInventoryMessage.getInventory())
							{
								if (BlockHandler.this.context.getLedger().getLedgerStore().has(blockVoteHash) == true)
									continue;
								
								if (blocksLog.hasLevel(Logging.DEBUG) == true)
									blocksLog.debug(BlockHandler.this.context.getName()+": Added request for block vote "+blockVoteHash+" for "+peer);
								
								blockVoteInventoryRequired.add(blockVoteHash);
							}
							
							if (blockVoteInventoryRequired.isEmpty() == true)
								return;
							
							BlockHandler.this.context.getNetwork().getMessaging().send(new GetBlockVoteMessage(blockVoteInventoryRequired), peer);
						}
						catch (Exception ex)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block.vote.inv "+peer, ex);
						}
						finally
						{
							BlockHandler.this.lock.unlock();
						}
					}
				});
			}
		});

		// FIXME this could interfere with above and vice versa as both work to request block headers.  
		//       Needs wrapping in a proper request process to mitigate duplicate fetches from different sources.
		//		 See AtomHandler requestAtoms
		this.context.getNetwork().getMessaging().register(BlockHeaderInventoryMessage.class, this.getClass(), new MessageProcessor<BlockHeaderInventoryMessage>()
		{
			@Override
			public void process(final BlockHeaderInventoryMessage blockHeaderInventoryMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						BlockHandler.this.lock.lock();
						try
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Block header inventory for "+blockHeaderInventoryMessage.getInventory().size()+" headers from " + peer);

							if (blockHeaderInventoryMessage.getInventory().size() == 0)
							{
								blocksLog.error(BlockHandler.this.context.getName()+": Received empty block headers inventory from " + peer);
								// TODO disconnect and ban
								return;
							}

							List<Hash> blockHeaderInventoryRequired = new ArrayList<Hash>();
							for (Hash blockHash : blockHeaderInventoryMessage.getInventory())
							{
								PendingBlock pendingBlock = BlockHandler.this.pendingBlocks.get(blockHash);
								if ((pendingBlock != null && pendingBlock.getBlockHeader() != null) || 
									BlockHandler.this.context.getLedger().getLedgerStore().has(blockHash) == true)
									continue;
								
								blockHeaderInventoryRequired.add(blockHash);
							}
							
							if (blockHeaderInventoryRequired.isEmpty() == true)
								return;
							
							BlockHandler.this.context.getNetwork().getMessaging().send(new GetBlockHeaderMessage(blockHeaderInventoryRequired), peer);
						}
						catch (Exception ex)
						{
							blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block.header.inv "+peer, ex);
						}
						finally
						{
							BlockHandler.this.lock.unlock();
						}
					}
				});
			}
		});

		Thread blockInventoryProcessorThread = new Thread(this.blockInventoryProcessor);
		blockInventoryProcessorThread.setDaemon(true);
		blockInventoryProcessorThread.setName(this.context.getName()+" Block Inventory Processor");
		blockInventoryProcessorThread.start();

		Thread blockVoteInventoryProcessorThread = new Thread(this.blockVoteInventoryProcessor);
		blockVoteInventoryProcessorThread.setDaemon(true);
		blockVoteInventoryProcessorThread.setName(this.context.getName()+" Block Vote Inventory Processor");
		blockVoteInventoryProcessorThread.start();

		this.context.getEvents().register(this.asyncAtomListener);

		Thread blockProcessorThread = new Thread(this.blockProcessor);
		blockProcessorThread.setDaemon(true);
		blockProcessorThread.setName(this.context.getName()+" Block Processor");
		blockProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException
	{
		this.blockInventoryProcessor.terminate(true);
		this.blockVoteInventoryProcessor.terminate(true);
		this.blockProcessor.terminate(true);
		this.context.getEvents().unregister(this.asyncAtomListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	public int size()
	{
		return this.pendingBlocks.size();
	}
	
	private List<BlockVote> vote(final PendingBranch branch) throws IOException, CryptoException, ValidationException
	{
		long votePower = BlockHandler.this.voteRegulator.getVotePower(BlockHandler.this.context.getNode().getIdentity());

		List<BlockVote> branchVotes = new ArrayList<BlockVote>();
		synchronized(BlockHandler.this.voteClock)
		{
			Iterator<PendingBlock> reverseIterator = branch.getBlocks().iterator();
			while(reverseIterator.hasNext())
			{
				PendingBlock pendingBlock = reverseIterator.next();
				if (BlockHandler.this.voteClock.get() < pendingBlock.getBlockHeader().getHeight())
				{
					BlockHandler.this.voteClock.set(pendingBlock.getBlockHeader().getHeight());
					BlockHandler.this.currentVote.set(pendingBlock.getBlockHeader());
					
					if (votePower > 0)
					{
						BlockVote blockHeaderVote = new BlockVote(pendingBlock.getBlockHeader().getHash(), BlockHandler.this.voteClock.get(), BlockHandler.this.context.getNode().getIdentity());
						blockHeaderVote.sign(BlockHandler.this.context.getNode().getKey());
						pendingBlock.vote(blockHeaderVote, votePower);
						this.context.getLedger().getLedgerStore().store(blockHeaderVote);
						broadcast(blockHeaderVote);
						branchVotes.add(blockHeaderVote);
						
						if (blocksLog.hasLevel(Logging.DEBUG) == true)
						{
							if (pendingBlock.getBlockHeader().getOwner().equals(BlockHandler.this.context.getNode().getIdentity()) == true)
								blocksLog.info(BlockHandler.this.context.getName()+": Voted on own block "+pendingBlock);
							else
								blocksLog.info(BlockHandler.this.context.getName()+": Voted on block "+pendingBlock);
						}
					}
				}
			}
		}
		
		return branchVotes;
	}
	
	private void broadcast(BlockVote blockVote) throws SerializationException, CryptoException
	{
		BlockVoteMessage blockVoteMessage = new BlockVoteMessage(blockVote);
		for (ConnectedPeer connectedPeer : BlockHandler.this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED))
		{
			if (BlockHandler.this.context.getNode().isInSyncWith(connectedPeer.getNode(), Node.OOS_TRIGGER_LIMIT) == false)
				continue;
			
//			if (connectedPeer.getDirection().equals(Direction.OUTBOUND) == false)
//				continue;
			
			try
			{
				if (blockVote.getOwner().equals(BlockHandler.this.context.getNode().getIdentity()) == true && blocksLog.hasLevel(Logging.DEBUG) == true)
					blocksLog.debug(BlockHandler.this.context.getName()+": Sending own vote "+blockVote.getHash()+" for "+blockVoteMessage.getVote().getObject()+" to " + connectedPeer);
				
				BlockHandler.this.context.getNetwork().getMessaging().send(blockVoteMessage, connectedPeer);
			}
			catch (IOException ex)
			{
				blocksLog.error(BlockHandler.this.context.getName()+": Unable to send BlockVoteMessage for "+blockVoteMessage.getVote()+" to "+connectedPeer, ex);
			}
		}
	}
	
	private void broadcast(BlockHeader blockHeader) throws SerializationException, CryptoException
	{
		BlockHeaderMessage blockCandidateMessage = new BlockHeaderMessage(blockHeader);
		for (ConnectedPeer connectedPeer : BlockHandler.this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED))
		{
			if (BlockHandler.this.context.getNode().isInSyncWith(connectedPeer.getNode(), Node.OOS_TRIGGER_LIMIT) == false)
				continue;
			
//			if (connectedPeer.getDirection().equals(Direction.OUTBOUND) == false)
//				continue;
			
			try
			{
				BlockHandler.this.context.getNetwork().getMessaging().send(blockCandidateMessage, connectedPeer);
			}
			catch (IOException ex)
			{
				blocksLog.error(BlockHandler.this.context.getName()+": Unable to send BlockCandidateMessage for "+blockHeader+" to "+connectedPeer, ex);
			}
		}
	}

	private Block build(final PendingBlock head, final PendingBranch branch) throws IOException
	{
		final Set<Hash> branchAtomExclusions = (branch == null || branch.isEmpty() == true) ? Collections.emptySet() : new HashSet<Hash>();
		final Set<Hash> branchCertificateExclusions = (branch == null || branch.isEmpty() == true) ? Collections.emptySet() : new HashSet<Hash>();
		if (branch != null && branch.isEmpty() == false)
		{
			for (PendingBlock block : branch.getBlocks())
			{
				branchAtomExclusions.addAll(block.getBlockHeader().getInventory(InventoryType.ATOMS));
				branchCertificateExclusions.addAll(block.getBlockHeader().getInventory(InventoryType.CERTIFICATES));
			}
			
			if (branch != null && branch.isEmpty() == false && head.equals(branch.getLast()) == false)
				throw new IllegalArgumentException("Head is not top of branch "+head);
		}
		
		final PendingBlock previous = head;
		final long initialTarget = (this.context.getNode().getIdentity().asHash().asLong()+previous.getHash().asLong()); 
		final List<Atom> seedAtoms = this.context.getLedger().getAtomPool().get(initialTarget-Long.MIN_VALUE, initialTarget, BlockHandler.BASELINE_DISTANCE_TARGET, 8, branchAtomExclusions);

		Block strongestBlock = null;
		for (int i=0 ; i < this.context.getConfiguration().get("ledger.accumulator.iterations", 1) ; i++)
		{
			this.context.getMetaData().increment("ledger.accumulator.iterations");

			StateAccumulator accumulator = new StateAccumulator(this.context, this.context.getLedger().getStateAccumulator());
			List<Hash> atomExclusions = new ArrayList<Hash>(branchAtomExclusions);
			List<Hash> certificateExclusions = new ArrayList<Hash>(branchCertificateExclusions);
			long nextTarget = initialTarget;
			
			final LinkedList<Atom> candidateAtoms = new LinkedList<Atom>();
			Collections.shuffle(seedAtoms);
			List<Atom> atoms = seedAtoms;
			boolean foundAtom = false;
			do
			{
				if (nextTarget != initialTarget)
				{
					// Should only select atoms from the pool that have 2/3 agreement
					atoms = this.context.getLedger().getAtomPool().get(nextTarget-Long.MIN_VALUE, nextTarget, BlockHandler.BASELINE_DISTANCE_TARGET, 8, atomExclusions);
					Collections.shuffle(atoms);
					if (atoms.isEmpty() == true)
						break;
				}

				foundAtom = false;
				for (Atom atom : atoms)
				{
					// Discover an atom in range of nextTarget
					if (withinRange(atom.getHash().asLong(), nextTarget, BlockHandler.BASELINE_DISTANCE_TARGET) == true)
					{
						try
						{
							// FIXME this is junk
							StateMachine stateMachine = new StateMachine(BlockHandler.this.context, head.getBlockHeader(), atom, accumulator);
							stateMachine.lock();
							atomExclusions.add(atom.getHash());
							candidateAtoms.add(atom);
							nextTarget = atom.getHash().asLong();
							foundAtom = true;
							
							// Try to build a block
							Collection<AtomCertificate> candidateCertificates = this.context.getLedger().getStateHandler().get(BlockHeader.MAX_ATOMS, certificateExclusions);
							Block discoveredBlock = new Block(previous.getBlockHeader().getHeight()+1, previous.getHash(), previous.getBlockHeader().getStepped(), previous.getBlockHeader().getNextIndex(), this.context.getNode().getIdentity(), candidateAtoms, candidateCertificates);
							if (discoveredBlock.getHeader().getStep() >= BlockHandler.BLOCK_DISTANCE_TARGET) // TODO difficulty stuff
							{
								if (strongestBlock == null || strongestBlock.getAtoms().size() < discoveredBlock.getAtoms().size())
									strongestBlock = discoveredBlock;
								
								certificateExclusions.addAll(candidateCertificates.stream().map(c -> c.getHash()).collect(Collectors.toSet()));
							}
							
							break;
						}
						catch (IOException ioex)
						{
							blocksLog.error(this.context.getName(), ioex);
							throw ioex;
						}
						catch (ValidationException vex)
						{
							// Skip the reporting of these here, they are reported on a discard event
							if ((vex instanceof DependencyNotFoundException) == false)
								blocksLog.error(this.context.getName(), vex);
										
							this.context.getEvents().post(new AtomErrorEvent(atom, vex));
						}
					}
				}
			}
			while(foundAtom == true && candidateAtoms.size() < BlockHeader.MAX_ATOMS);
		}
		
		if (strongestBlock != null)
			this.context.getLedger().getLedgerStore().store(strongestBlock);
		
		return strongestBlock;
	}
	
	private void commit(PendingBlock block, PendingBranch branch) throws IOException
	{
		BlockHandler.this.lock.lock();
		try
		{
			LinkedList<PendingBlock> committedBlocks = branch.commit(block.getBlockHeader());
			for (PendingBlock committedBlock : committedBlocks)
				blocksLog.info(BlockHandler.this.context.getName()+": Committed block "+committedBlock.getBlockHeader());

			// Clean or trim pending branches as a result of this commit
			Iterator<PendingBranch> pendingBranchIterator = BlockHandler.this.pendingBranches.iterator();
			while(pendingBranchIterator.hasNext() == true)
			{
				PendingBranch pendingBranch = pendingBranchIterator.next();
				if (pendingBranch.intersects(branch.getHead()) == false)
				{
					for (PendingBlock pendingBlock : pendingBranch.getBlocks())
						this.pendingBlocks.remove(pendingBlock.getHash());
					
					pendingBranchIterator.remove();
				}
				else
					pendingBranch.trimTo(branch.getHead()); 
			}
			
			// Clear out pending of blocks that may not be in a branch and can't be committed due to this commit
			long branchHeadHeight = branch.getHead().getHeight();
			Iterator<Entry<Hash, PendingBlock>> pendingBlocksIterator = BlockHandler.this.pendingBlocks.entrySet().iterator();
			while(pendingBlocksIterator.hasNext() == true)
			{
				Entry<Hash, PendingBlock> pendingBlockEntry = pendingBlocksIterator.next();
				if (Longs.fromByteArray(pendingBlockEntry.getValue().getHash().toByteArray()) <= branchHeadHeight)
					pendingBlocksIterator.remove();
			}

			// Signal the commit
			for (PendingBlock committedBlock : committedBlocks)
			{
				BlockCommittedEvent blockCommittedEvent = new BlockCommittedEvent(committedBlock.getBlock());
				BlockHandler.this.context.getEvents().post(blockCommittedEvent); // TODO Might need to catch exceptions on this from synchronous listeners
			}
		}
		finally
		{
			BlockHandler.this.lock.unlock();
		}
	}
	
	private void updateBranches()
	{
		this.lock.lock();
		try
		{
			// Not very efficient, but simple
			Iterator<PendingBlock> pendingBlockIterator = this.pendingBlocks.values().iterator();
			while (pendingBlockIterator.hasNext() == true)
			{
				PendingBlock pendingBlock  = pendingBlockIterator.next();
				if (pendingBlock.getBlock() == null)
					continue;
				
				try
				{
					PendingBranch newBranch = null;
					for (PendingBranch pendingBranch : this.pendingBranches)
					{
						if (pendingBranch.contains(pendingBlock) == true)
							continue;
						
						if (pendingBranch.getLast().getHash().equals(pendingBlock.getBlockHeader().getPrevious()) == true)
							pendingBranch.add(pendingBlock);
						else if (pendingBranch.intersects(pendingBlock.getBlockHeader()) == true)
						{
							newBranch = new PendingBranch(this.context);
							for (PendingBlock branchBlock : pendingBranch.getBlocks())
							{
								if (branchBlock.getHash().equals(pendingBlock.getBlockHeader().getPrevious()) == true)
									break;
								
								newBranch.add(branchBlock);
							}
							break;
						}
					}
					
					if (newBranch == null && this.context.getLedger().getHead().getHash().equals(pendingBlock.getBlockHeader().getPrevious()) == true)
						newBranch = new PendingBranch(this.context, pendingBlock);
					
					if (newBranch != null)
						this.pendingBranches.add(newBranch);
				}
				catch (ValidationException e)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": Branch validation block "+pendingBlock+" failed", e);
					pendingBlockIterator.remove();
				}
				catch (Exception e)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": Branch injection / maintenence for block "+pendingBlock+" failed", e);
					pendingBlockIterator.remove();
				}
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}

	private PendingBranch getBestBranch()
	{
		this.lock.lock();
		try
		{
			PendingBranch bestBranch = null;
			for (PendingBranch pendingBranch : this.pendingBranches)
			{
				// Short circuit on any branch that has a commit possible
				if (pendingBranch.commitable() != null)
				{
					bestBranch = pendingBranch;
					break;
				}
				
				if (pendingBranch.getFirst().getBlockHeader().getPrevious().equals(this.context.getLedger().getHead().getHash()) == false)
				{
					if (blocksLog.hasLevel(Logging.DEBUG) == true)
						blocksLog.debug(this.context.getName()+": Branch doesn't attach to ledger "+pendingBranch.getFirst());
					
					continue;
				}
	
				if (bestBranch == null || 
					bestBranch.getLast().getBlockHeader().getAverageStep() < pendingBranch.getLast().getBlockHeader().getAverageStep() ||
					(bestBranch.getLast().getBlockHeader().getAverageStep() == pendingBranch.getLast().getBlockHeader().getAverageStep() && bestBranch.getLast().getBlockHeader().getStep() < pendingBranch.getLast().getBlockHeader().getStep()))
					bestBranch = pendingBranch;
			}
	
			if (bestBranch != null)
			{
				if (blocksLog.hasLevel(Logging.DEBUG) == true)
					blocksLog.debug(BlockHandler.this.context.getName()+": Selected branch "+bestBranch.getLast().weight()+"/"+this.voteRegulator.getTotalVotePower(Collections.singleton(BlockHandler.this.context.getLedger().getShardGroup(BlockHandler.this.context.getNode().getIdentity())))+" "+bestBranch.getBlocks().stream().map(pb -> pb.getHash().toString()).collect(Collectors.joining(" -> ")));
			}
			
			return bestBranch;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	// ASYNC ATOM LISTENER //
	private EventListener asyncAtomListener = new EventListener()
	{
		@Subscribe
		public void on(AtomPersistedEvent atomPersistedEvent) 
		{
		}
		
		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent)
		{
			if (BlockHandler.this.voteClock.get() < blockCommittedEvent.getBlock().getHeader().getHeight())
			{
				BlockHandler.this.voteClock.set(blockCommittedEvent.getBlock().getHeader().getHeight());
				BlockHandler.this.currentVote.set(null);
			}
		}
	};
}
