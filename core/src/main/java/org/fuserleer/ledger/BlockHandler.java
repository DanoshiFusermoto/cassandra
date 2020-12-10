package org.fuserleer.ledger;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignatureBag;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.crypto.MerkleTree;
import org.fuserleer.events.EventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomNotFoundException;
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
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.time.Time;
import org.fuserleer.utils.MathUtils;
import org.fuserleer.utils.UInt128;
import org.fuserleer.utils.UInt256;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.Subscribe;
import com.google.common.primitives.Longs;
import com.sleepycat.je.OperationStatus;

public class BlockHandler implements Service
{
	private static final Logger blocksLog = Logging.getLogger("blocks");

	public final static long 	BASELINE_DISTANCE_FACTOR = 8;
	public final static long 	BASELINE_DISTANCE_TARGET = BigInteger.valueOf(Long.MIN_VALUE).abs().subtract(BigInteger.valueOf((long) (Long.MIN_VALUE / (BASELINE_DISTANCE_FACTOR * Math.log(BASELINE_DISTANCE_FACTOR)))).abs()).longValue(); // TODO rubbish, but produces close enough needed output
	public final static long 	BLOCK_DISTANCE_FACTOR = 256;
	public final static long 	BLOCK_DISTANCE_TARGET = BigInteger.valueOf(Long.MIN_VALUE).abs().subtract(BigInteger.valueOf((long) (Long.MIN_VALUE / (BLOCK_DISTANCE_FACTOR * Math.log(BLOCK_DISTANCE_FACTOR)))).abs()).longValue(); // TODO rubbish, but produces close enough needed output
	
	private final static Comparator<PendingBlock> PENDING_BRANCH_COMPARATOR = Collections.reverseOrder(new Comparator<PendingBlock>() 
	{
		@Override
		public int compare(PendingBlock arg0, PendingBlock arg1)
		{
			if (arg0.getBlockHeader().getHeight() < arg1.getBlockHeader().getHeight())
				return -1;

			if (arg0.getBlockHeader().getHeight() > arg1.getBlockHeader().getHeight())
				return 1;
			
			return 0;
		}
	});

	public static boolean withinRange(Hash hash, long point, long range)
	{
		long location = hash.asLong();
		long distance = MathUtils.ringDistance64(point, location);
		if (distance > range)
			return true;
		
		return false;
	}
	
	public static long getDistance(Hash hash, long point, long range)
	{
		long location = hash.asLong();
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
						
						// TODO do clean up of old pending blocks / committed / orphans etc
						
						// Find the best block candidate to build on top of
						PendingBlock buildCandidate = null;
						
						synchronized(BlockHandler.this.bestBranch)
						{
							BlockHandler.this.bestBranch.clear();
							BlockHandler.this.bestBranch.addAll(getBestBranch());
						
							if (BlockHandler.this.bestBranch.isEmpty() == false)
								buildCandidate = BlockHandler.this.bestBranch.getFirst();
							else
								buildCandidate = new PendingBlock(BlockHandler.this.context.getLedger().get(BlockHandler.this.context.getLedger().getHead().getHash(), Block.class));
						}
						
						synchronized(BlockHandler.this.voteClock)
						{
							// Check didn't vote on a new block while building one
//							if (lastGenerated.get() != null && lastGenerated.get().getHeight() > BlockHandler.this.voteClock.get())
							{
								// Vote on best branch?
								if (BlockHandler.this.voteClock.get() < buildCandidate.getBlockHeader().getHeight())
									vote(BlockHandler.this.bestBranch);
							}
						}
						
						// No one has produced a new block yet that that the local node has voted on, 
						// if havent successfully built one for the current round attempt to
						if (BlockHandler.this.lastGenerated.get() == null || BlockHandler.this.lastGenerated.get().getHeight() <= buildCandidate.getBlockHeader().getHeight())
						{
							final Block generatedBlock;
							synchronized(BlockHandler.this.bestBranch)
							{
								generatedBlock = BlockHandler.this.build(buildCandidate, BlockHandler.this.bestBranch);
							}
							
							if (generatedBlock != null)
							{
								BlockHandler.this.lastGenerated.set(generatedBlock.getHeader());
								blocksLog.info(BlockHandler.this.context.getName()+": Generated block "+generatedBlock.getHeader());
								BlockHandler.this.pending.computeIfAbsent(generatedBlock.getHash(), (h) -> new PendingBlock(generatedBlock));
								broadcast(generatedBlock.getHeader());
							}
						}
						
						List<PendingBlock> commitBranchSection = new ArrayList<PendingBlock>();
						synchronized(BlockHandler.this.bestBranch)
						{
							if (BlockHandler.this.bestBranch.isEmpty() == false)
							{
								boolean haveCommits = false;
								// See if there is a section of the best branch that can be committed (any block that has 2f+1 agreement)
								Iterator<PendingBlock> reverseIterator = BlockHandler.this.bestBranch.descendingIterator();
								while(reverseIterator.hasNext())
								{
									PendingBlock branchBlock = reverseIterator.next();
									commitBranchSection.add(branchBlock);	

									if (branchBlock.weight().compareTo(BlockHandler.this.voteRegulator.getVotePowerThreshold(branchBlock.getBlockHeader().getHeight())) >= 0)
									{
										haveCommits = true;
										blocksLog.info(BlockHandler.this.context.getName()+": Found commit at block with weight "+branchBlock.weight()+"/"+BlockHandler.this.voteRegulator.getTotalVotePower(branchBlock.getBlockHeader().getHeight())+" to commit list "+branchBlock);
										break;
									}
								}
								
								if (haveCommits == false)
									commitBranchSection.clear();
							}
						}
							
						if (commitBranchSection.isEmpty() == false)
							commit(commitBranchSection);
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
	
	private class PendingBlock implements Hashable
	{
		private Hash		hash;
		private	final long 	witnessed;
		private Block		block;

		private UInt256		voteWeight;
		private final Map<ECPublicKey, BlockVote> votes;

		public PendingBlock(Hash block)
		{
			this.hash = Objects.requireNonNull(block);
			this.witnessed = Time.getLedgerTimeMS();
			this.voteWeight = UInt256.ZERO;
			this.votes = Collections.synchronizedMap(new HashMap<ECPublicKey, BlockVote>());
		}

		public PendingBlock(Block block)
		{
			this.hash = Objects.requireNonNull(block).getHash();
			this.block = block;
			this.witnessed = Time.getLedgerTimeMS();
			this.voteWeight = UInt256.ZERO;
			this.votes = Collections.synchronizedMap(new HashMap<ECPublicKey, BlockVote>());
		}
		
		@Override
		public Hash getHash()
		{
			return this.hash;
		}
		
		public BlockHeader getBlockHeader()
		{
			return this.getBlock().getHeader();
		}
				
		public Block getBlock()
		{
			return this.block;
		}

		void setBlock(Block block)
		{
			this.block = Objects.requireNonNull(block);
		}

		@Override
		public int hashCode()
		{
			return this.hash.hashCode();
		}

		@Override
		public boolean equals(Object obj)
		{
			if (obj == null)
				return false;
			
			if (obj != this)
				return false;
			
			return true;
		}

		@Override
		public String toString()
		{
			return (this.block == null ? this.hash : this.block.getHeader().getHeight()+" "+this.block.getHeader().getHash()+" "+this.block.getHeader().getStep())+" @ "+this.witnessed;
		}
		
		public long getWitnessed()
		{
			return this.witnessed;
		}
		
		public boolean voted(ECPublicKey identity)
		{
			return this.votes.containsKey(Objects.requireNonNull(identity, "Vote identity is null"));
		}
		
		public ECSignatureBag certificate()
		{
			if (getBlockHeader().getCertificate() != null)
			{
				blocksLog.warn("Block header already has a certificate "+getBlockHeader());
				return getBlockHeader().getCertificate();
			}
			
			synchronized(this.votes)
			{
				if (this.votes.isEmpty() == true)
					blocksLog.warn("Block header has no votes "+getBlockHeader());
				
				ECSignatureBag certificate = new ECSignatureBag(this.votes.values().stream().collect(Collectors.toMap(v -> v.getOwner(), v -> v.getSignature())));
				getBlockHeader().setCertificate(certificate);
				return certificate;
			}
		}

		public UInt256 vote(BlockVote vote, UInt128 weight) throws ValidationException
		{
			Objects.requireNonNull(vote, "Vote is null");
			Objects.requireNonNull(weight, "Weight is null");
			
			if (vote.getObject().equals(getHash()) == false)
				throw new ValidationException("Vote from "+vote.getOwner()+" is not for "+getHash());
			
			if (vote.getOwner().verify(getHash(), vote.getSignature()) == false)
				throw new ValidationException("Signature from "+vote.getOwner()+" did not verify against "+getHash());

			synchronized(this.votes)
			{
				if (this.votes.containsKey(vote.getOwner()) == false)
				{
					this.votes.put(vote.getOwner(), vote);
					this.voteWeight = this.voteWeight.add(weight);
				}
				else
					blocksLog.warn(BlockHandler.this.context.getName()+": "+vote.getOwner()+" has already cast a vote for "+this.hash);
				
			}

			return this.voteWeight;
		}
		
		public UInt256 weight()
		{
			return this.voteWeight;
		}

		public Collection<BlockVote> votes()
		{
			synchronized(this.votes)
			{
				return this.votes.values().stream().collect(Collectors.toList());
			}
		}
	}

	private final VoteRegulator 				voteRegulator;
	private final Map<Hash, PendingBlock>		pending;
	private final AtomicLong					voteClock;
	private final AtomicReference<BlockHeader> 	currentVote;
	private final AtomicReference<BlockHeader> 	lastGenerated;
	private final LinkedList<PendingBlock> 		bestBranch;
	
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
						if (BlockHandler.this.context.getNode().isInSyncWith(connectedPeer.getNode()) == false)
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
						if (BlockHandler.this.context.getNode().isInSyncWith(connectedPeer.getNode()) == false)
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
		this.pending = Collections.synchronizedMap(new HashMap<Hash, PendingBlock>());
		this.voteRegulator = Objects.requireNonNull(voteRegulator, "Vote regulator is null");
		this.voteClock = new AtomicLong(0);
		this.currentVote = new AtomicReference<BlockHeader>();
		this.lastGenerated = new AtomicReference<BlockHeader>();
		this.bestBranch = new LinkedList<PendingBlock>();
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
				try
				{
					if (blockHeaderMessage.getBlockHeader().getHeight() <= BlockHandler.this.context.getLedger().getHead().getHeight())
					{
						blocksLog.warn(BlockHandler.this.context.getName()+": Block header is old "+blockHeaderMessage.getBlockHeader()+" from "+peer);
						BlockHandler.this.pending.remove(blockHeaderMessage.getBlockHeader().getHash());
						return;
					}

					synchronized(BlockHandler.this.pending)
					{
						if (BlockHandler.this.context.getLedger().getLedgerStore().has(blockHeaderMessage.getBlockHeader().getHash()) == false)
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Block header "+blockHeaderMessage.getBlockHeader().getHash()+" from "+peer);
							
							// Try to build the block from known atoms
							List<Atom> atoms = new ArrayList<Atom>();
							for (Hash atomHash : blockHeaderMessage.getBlockHeader().getInventory())
							{
								Atom atom = BlockHandler.this.context.getLedger().get(atomHash, Atom.class);
								if (atom == null)
									break;
								
								atoms.add(atom);
							}

							if (atoms.size() == blockHeaderMessage.getBlockHeader().getInventory().size())
							{
								Block block = new Block(blockHeaderMessage.getBlockHeader(), atoms);

								// TODO need a block validation / verification processor
								
								BlockHandler.this.context.getLedger().getLedgerStore().store(block);

								PendingBlock pendingBlock = BlockHandler.this.pending.get(block.getHeader().getHash());
								if (pendingBlock == null)
								{
									pendingBlock = new PendingBlock(block);
									BlockHandler.this.pending.put(block.getHeader().getHash(), pendingBlock);
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
				}
				catch (Exception ex)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block.header "+peer, ex);
				}
			}
		});

		this.context.getNetwork().getMessaging().register(BlockMessage.class, this.getClass(), new MessageProcessor<BlockMessage>()
		{
			@Override
			public void process(final BlockMessage blockMessage, final ConnectedPeer peer)
			{
				try
				{
					if (blockMessage.getBlock().getHeader().getHeight() <= BlockHandler.this.context.getLedger().getHead().getHeight())
					{
						blocksLog.warn(BlockHandler.this.context.getName()+": Block is old "+blockMessage.getBlock().getHeader()+" from "+peer);
						BlockHandler.this.pending.remove(blockMessage.getBlock().getHeader().getHash());
						return;
					}

					synchronized(BlockHandler.this.pending)
					{
						if (BlockHandler.this.context.getLedger().getLedgerStore().has(blockMessage.getBlock().getHeader().getHash()) == false)
						{
							if (blocksLog.hasLevel(Logging.DEBUG) == true)
								blocksLog.debug(BlockHandler.this.context.getName()+": Block "+blockMessage.getBlock().getHeader().getHash()+" from "+peer);

							// TODO need a block validation / verification processor
	
							BlockHandler.this.context.getLedger().getLedgerStore().store(blockMessage.getBlock());
							
							PendingBlock pendingBlock = BlockHandler.this.pending.get(blockMessage.getBlock().getHeader().getHash());
							if (pendingBlock == null)
							{
								pendingBlock = new PendingBlock(blockMessage.getBlock());
								BlockHandler.this.pending.put(blockMessage.getBlock().getHeader().getHash(), pendingBlock);
							}
							else
								pendingBlock.setBlock(blockMessage.getBlock());

							BlockHandler.this.blockInventory.add(pendingBlock.getHash());
						}
					}
				}
				catch (Exception ex)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block "+peer, ex);
				}
			}
		});

		this.context.getNetwork().getMessaging().register(BlockVoteMessage.class, this.getClass(), new MessageProcessor<BlockVoteMessage>()
		{
			@Override
			public void process(final BlockVoteMessage blockVoteMessage, final ConnectedPeer peer)
			{
				try
				{
					if (OperationStatus.KEYEXIST.equals(BlockHandler.this.context.getLedger().getLedgerStore().store(blockVoteMessage.getVote())) == false)
					{
						// Can extract the height from the hash at its prefixed
						long pendingBlockHeight = Longs.fromByteArray(blockVoteMessage.getVote().getObject().toByteArray());
						if (pendingBlockHeight > BlockHandler.this.context.getLedger().getHead().getHeight())
						{
							PendingBlock pendingBlock = BlockHandler.this.pending.computeIfAbsent(blockVoteMessage.getVote().getObject(), (h) -> new PendingBlock(blockVoteMessage.getVote().getObject()));
							// If already have a vote recorded for the identity, then vote was already seen possibly selected and broadcast
							if (pendingBlock.voted(blockVoteMessage.getVote().getOwner()) == false)
							{
								if (blocksLog.hasLevel(Logging.DEBUG) == true)
									blocksLog.debug(BlockHandler.this.context.getName()+": Block vote "+blockVoteMessage.getVote().getHash()+"/"+blockVoteMessage.getVote().getObject()+" for "+blockVoteMessage.getVote().getOwner()+" from "+peer);
		
								pendingBlock.vote(blockVoteMessage.getVote(), BlockHandler.this.voteRegulator.getVotePower(blockVoteMessage.getVote().getOwner(), Long.MAX_VALUE));
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
								PendingBlock pendingBlock = BlockHandler.this.pending.get(blockHash);
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
								PendingBlock pendingBlock = BlockHandler.this.pending.get(blockHash);
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
		return this.pending.size();
	}
	
	private List<BlockVote> vote(final LinkedList<PendingBlock> branch) throws IOException, CryptoException, ValidationException
	{
		UInt128 votePower = BlockHandler.this.voteRegulator.getVotePower(BlockHandler.this.context.getNode().getIdentity(), Long.MAX_VALUE);

		List<BlockVote> branchVotes = new ArrayList<BlockVote>();
		synchronized(BlockHandler.this.voteClock)
		{
			Iterator<PendingBlock> reverseIterator = branch.descendingIterator();
			while(reverseIterator.hasNext())
			{
				PendingBlock pendingBlock = reverseIterator.next();
				if (BlockHandler.this.voteClock.get() < pendingBlock.getBlockHeader().getHeight())
				{
					BlockHandler.this.voteClock.set(pendingBlock.getBlockHeader().getHeight());
					BlockHandler.this.currentVote.set(pendingBlock.getBlockHeader());
					
					if (votePower.compareTo(UInt128.ZERO) > 0)
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
			if (BlockHandler.this.context.getNode().isInSyncWith(connectedPeer.getNode()) == false)
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
			if (BlockHandler.this.context.getNode().isInSyncWith(connectedPeer.getNode()) == false)
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

	private Block build(final PendingBlock head, final LinkedList<PendingBlock> branch) throws IOException
	{
		// TODO Bloom based exclusion is fine for now, but maybe want a concrete list of hashes later
		final Set<Hash> branchExclusions = branch.isEmpty() == true ? Collections.emptySet() : new HashSet<Hash>(); 
		for (PendingBlock block : branch)
			branchExclusions.addAll(block.getBlockHeader().getInventory());
		
		if (branch.isEmpty() == false && head.equals(branch.getFirst()) == false)
			throw new IllegalArgumentException("Head is not top of branch "+head);
		
		final PendingBlock previous = head;
		final long initialTarget = (this.context.getNode().getIdentity().asHash().asLong()+previous.getHash().asLong()); 
		final List<Atom> seedAtoms = this.context.getLedger().getAtomPool().get(initialTarget-Long.MIN_VALUE, 8, branchExclusions);

		Block strongestBlock = null;
		for (int i=0 ; i < this.context.getConfiguration().get("ledger.accumulator.iterations", 1) ; i++)
		{
			this.context.getMetaData().increment("ledger.accumulator.iterations");

			List<Hash> exclusions = new ArrayList<Hash>(branchExclusions);
			MerkleTree merkle = new MerkleTree();
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
					atoms = this.context.getLedger().getAtomPool().get(nextTarget-Long.MIN_VALUE, 8, exclusions);
					Collections.shuffle(atoms);
					if (atoms.isEmpty() == true)
						break;
				}

				foundAtom = false;
				for (Atom atom : atoms)
				{
					// Discover an atom in range of nextTarget
					if (withinRange(atom.getHash(), nextTarget, BlockHandler.BASELINE_DISTANCE_TARGET) == true)
					{
						merkle.appendLeaf(atom.getHash());
						exclusions.add(atom.getHash());
						candidateAtoms.add(atom);
						nextTarget = atom.getHash().asLong();
						foundAtom = true;
						
						// Try to build a block
						Block discoveredBlock = new Block(previous.getBlockHeader().getHeight()+1, previous.getHash(), previous.getBlockHeader().getStepped(), previous.getBlockHeader().getNextIndex(), merkle.buildTree(), this.context.getNode().getIdentity(), candidateAtoms);
						if (discoveredBlock.getHeader().getStep() >= BlockHandler.BLOCK_DISTANCE_TARGET) // TODO difficulty stuff
						{
							if (strongestBlock == null || strongestBlock.getAtoms().size() < discoveredBlock.getAtoms().size())
								strongestBlock = discoveredBlock;
						}
						
						break;
					}
				}
			}
			while(foundAtom == true && candidateAtoms.size() < BlockHeader.MAX_ATOMS);
		}
		
		if (strongestBlock != null)
			this.context.getLedger().getLedgerStore().store(strongestBlock);
		
		return strongestBlock;
	}
	
	private void commit(List<PendingBlock> commitBranch)
	{
		Collections.sort(commitBranch, PENDING_BRANCH_COMPARATOR);
		Objects.requireNonNull(commitBranch, "Branch is null");
		if (commitBranch.isEmpty() == true)
			throw new IllegalArgumentException("Branch is empty");
	
		synchronized(BlockHandler.this.pending)
		{
			// Create the certificates
			for (PendingBlock commitBlock : commitBranch)
				commitBlock.certificate();

			// Clear out pending of blocks that can't be committed due to this commit
			long branchHeadHeight = commitBranch.get(0).getBlockHeader().getHeight();
			Iterator<Entry<Hash, PendingBlock>> pendingIterator = BlockHandler.this.pending.entrySet().iterator();
			while(pendingIterator.hasNext() == true)
			{
				Entry<Hash, PendingBlock> pendingBlockEntry = pendingIterator.next();
				if (Longs.fromByteArray(pendingBlockEntry.getValue().getHash().toByteArray()) <= branchHeadHeight)
					pendingIterator.remove();
			}
			
			// Need to do the commit in ascending order
			List<PendingBlock> reversedCommitBranch = new ArrayList<PendingBlock>(commitBranch);
			if (reversedCommitBranch.size() > 1)
				Collections.reverse(reversedCommitBranch);
			for (PendingBlock commitBlock : reversedCommitBranch)
			{
				Block blockToCommit = commitBlock.getBlock();
				BlockCommittedEvent blockCommittedEvent = new BlockCommittedEvent(commitBlock.getBlockHeader());
				BlockHandler.this.context.getEvents().post(blockCommittedEvent); // TODO Might need to catch exceptions on this from synchronous listeners
			}
		}
	}
	
	private List<PendingBlock> getBestBranch()
	{
		List<PendingBlock> bestBranch = new ArrayList<PendingBlock>();
		synchronized(BlockHandler.this.pending)
		{
			PendingBlock bestBranchEntry = null;
			Set<PendingBlock> branchSteps = new HashSet<PendingBlock>();
			Multimap<Long, PendingBlock> blockHeaderHeights = HashMultimap.create();
			for (PendingBlock pendingBlock : BlockHandler.this.pending.values())
				if (pendingBlock.getBlockHeader() != null)
					blockHeaderHeights.put(pendingBlock.getBlockHeader().getHeight(), pendingBlock);
			
			List<Long> sortedHeights = new ArrayList<Long>(blockHeaderHeights.keySet());
			Collections.sort(sortedHeights);
			for (long height : sortedHeights)
			{
				Set<PendingBlock> toRemove = new HashSet<PendingBlock>();
				Set<PendingBlock> nextBranchSteps = new HashSet<PendingBlock>();
				for (PendingBlock pendingBlock : blockHeaderHeights.get(height))
				{
					if (pendingBlock.getBlockHeader() == null)
						continue;

					// FIXME Quick fix to prevent pending blocks at the same height as the ledger being included in 
					//		 the best branch creation.  Likely they have been received just after AFTER the commit
					//		 AND managed the bypass the open checks.  Needs a lock on commit.
					//		 They will be pruned away on next commit.
					if (pendingBlock.getBlockHeader().getHeight() <= this.context.getLedger().getHead().getHeight())
						continue;
					
					if (pendingBlock.getBlockHeader().getPrevious().equals(this.context.getLedger().getHead().getHash()) == true)
					{
						nextBranchSteps.add(pendingBlock);
					}
					else
					{
						PendingBlock prevPendingBlock = this.pending.get(pendingBlock.getBlockHeader().getPrevious());
						if (prevPendingBlock == null || prevPendingBlock.getBlockHeader() == null || branchSteps.contains(prevPendingBlock) == false)
							continue;

						nextBranchSteps.add(pendingBlock);
						toRemove.add(prevPendingBlock);
					}
				}
				
				for (PendingBlock pendingBlock : toRemove)
					branchSteps.remove(pendingBlock);
				
				branchSteps.addAll(nextBranchSteps);
				
				// Short circuit on any branch that has a commit possible
				for (PendingBlock branchEntry : branchSteps)
				{
					if (branchEntry.weight().compareTo(this.voteRegulator.getVotePowerThreshold(branchEntry.getBlockHeader().getHeight())) >= 0)
					{
						if (bestBranchEntry != null)
							throw new IllegalStateException("Found multiple commits possible on distinct branches");
						
						bestBranchEntry = branchEntry;
					}
				}
				
				if (bestBranchEntry != null)
					break;
			}
			
			if (bestBranchEntry == null && branchSteps.isEmpty() == false)
			{
				for (PendingBlock branchEntry : branchSteps)
				{
					if (bestBranchEntry == null || 
						bestBranchEntry.getBlockHeader().getAverageStep() < branchEntry.getBlockHeader().getAverageStep() ||
						(bestBranchEntry.getBlockHeader().getAverageStep() == branchEntry.getBlockHeader().getAverageStep() && bestBranchEntry.getBlockHeader().getStep() < branchEntry.getBlockHeader().getStep()))
						bestBranchEntry = branchEntry;
				}
			}
			
			if (bestBranchEntry != null)
			{
				PendingBlock bestBranchHead = bestBranchEntry;
				do
				{
					bestBranch.add(bestBranchHead);

					// FIXME Quick fix to prevent pending blocks at the same height as the ledger being included in 
					//		 the best branch creation.  Likely they have been received just after AFTER the commit
					//		 AND managed the bypass the open checks.  Needs a lock on commit.
					//		 They will be pruned away on next commit.
					if (bestBranchHead.getBlockHeader().getPrevious().equals(this.context.getLedger().getHead().getHash()) == true)
						bestBranchHead = null;
					else
					{
						bestBranchHead = this.pending.get(bestBranchHead.getBlockHeader().getPrevious());

						if (bestBranchHead != null && bestBranchHead.getBlockHeader().getHeight() <= this.context.getLedger().getHead().getHeight())
							bestBranchHead = null;
					}
				}
				while(bestBranchHead != null);
				
				if (bestBranch.get(bestBranch.size()-1).getBlockHeader().getPrevious().equals(this.context.getLedger().getHead().getHash()) == false)
					if (blocksLog.hasLevel(Logging.DEBUG) == true)
						blocksLog.debug(this.context.getName()+": Branch doesn't attach to ledger "+bestBranch.get(0));
			}

			if (bestBranch.isEmpty() == false)
			{
				Collections.sort(bestBranch, PENDING_BRANCH_COMPARATOR);
				
				if (blocksLog.hasLevel(Logging.DEBUG) == true)
					blocksLog.debug(BlockHandler.this.context.getName()+": Selected branch "+bestBranch.get(0).weight()+"/"+this.voteRegulator.getTotalVotePower(bestBranch.get(0).getBlockHeader().getHeight())+" "+bestBranch.stream().map(pb -> pb.getHash().toString()).collect(Collectors.joining(" -> ")));
			}
		}
		
		return bestBranch;
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
			if (BlockHandler.this.voteClock.get() < blockCommittedEvent.getBlockHeader().getHeight())
			{
				BlockHandler.this.voteClock.set(blockCommittedEvent.getBlockHeader().getHeight());
				BlockHandler.this.currentVote.set(null);
			}
		}
	};
}
