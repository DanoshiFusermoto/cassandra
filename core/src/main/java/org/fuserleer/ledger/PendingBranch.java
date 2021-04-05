package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.Numbers;

import com.google.common.annotations.VisibleForTesting;

@VisibleForTesting
public class PendingBranch
{
	public enum Type
	{
		NONE, FORK
	}
	
	private static final Logger blocksLog = Logging.getLogger("blocks");

	private final Context context;
	private final long 	id;
	private final Type	type;
	private final LinkedList<PendingBlock> blocks;
	private final StateAccumulator accumulator;
	
	private BlockHeader root;

	private final ReentrantLock lock = new ReentrantLock();
	
	PendingBranch(final Context context, final Type type, final BlockHeader root, final StateAccumulator accumulator)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.type = Objects.requireNonNull(type, "Type is null");
		this.id = ThreadLocalRandom.current().nextLong();
		this.blocks = new LinkedList<PendingBlock>();
		this.root = Objects.requireNonNull(root, "Root is null");
		this.accumulator = Objects.requireNonNull(accumulator, "Accumulator is null");
	}

	PendingBranch(final Context context, final Type type, final BlockHeader root, final StateAccumulator accumulator, final PendingBlock block) throws ValidationException, IOException, StateLockedException
	{
		this(context, type, root, accumulator);

		add(block);
	}
	
	PendingBranch(final Context context, final Type type, final BlockHeader root, final StateAccumulator accumulator, final Collection<PendingBlock> blocks) throws StateLockedException, IOException
	{
		this(context, type, root, accumulator);
		
		for (PendingBlock block : blocks)
			add(block);
	}
	
	long getID()
	{
		return this.id;
	}
	
	Type getType()
	{
		return this.type;
	}

	boolean add(final PendingBlock block) throws StateLockedException, IOException
	{
		if (Objects.requireNonNull(block, "Block is null").getHeader() == null)
			throw new IllegalStateException("Block "+block.getHash()+" does not have a header");
		
		this.lock.lock();
		try
		{
			if (this.blocks.contains(block) == true)
				return false;
			
			if (block.getHeight() <= this.root.getHeight())
				blocksLog.warn(this.context.getName()+": Block is "+block.getHash()+" is before branch root "+this.root.getHash());
			
			boolean foundPrev = false;
			for (PendingBlock vertex : this.blocks)
			{
				if (vertex.getHash().equals(block.getHeader().getPrevious()) == true)
				{
					foundPrev = true;
					break;
				}
			}
			
			if (foundPrev == false && this.root.getHash().equals(block.getHeader().getPrevious()) == false)
				throw new IllegalStateException("Block "+block.getHash()+" does not attach to branch "+toString());
			
			lock(block);
			this.blocks.add(block);
			block.setInBranch();
			return true;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	boolean contains(final Hash block) throws StateLockedException
	{
		Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block hash is ZERO");
		
		this.lock.lock();
		try
		{
			for (PendingBlock vertex : this.blocks)
			{
				if (vertex.getHash().equals(block) == true)
					return true;
			}

			return false;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	boolean contains(final PendingBlock pendingBlock)
	{
		Objects.requireNonNull(pendingBlock, "Pending block is null");

		this.lock.lock();
		try
		{
			for (PendingBlock vertex : this.blocks)
				if (vertex.equals(pendingBlock) == true)
					return true;
			
			return false;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	boolean merge(final Collection<PendingBlock> blocks) throws StateLockedException, IOException
	{
		Objects.requireNonNull(blocks, "Blocks is null");

		List<PendingBlock> sortedBlocks = new ArrayList<PendingBlock>(blocks);
		List<PendingBlock> mergeBlocks = new ArrayList<PendingBlock>();
		this.lock.lock();
		try
		{
			Collections.sort(sortedBlocks, new Comparator<PendingBlock>() 
			{
				@Override
				public int compare(PendingBlock arg0, PendingBlock arg1)
				{
					if (arg0.getHeight() < arg1.getHeight())
						return -1;
					
					if (arg0.getHeight() > arg1.getHeight())
						return 1;

					return 0;
				}
			});
			
			for (PendingBlock sortedBlock : sortedBlocks)
			{
				if (sortedBlock.getHeader() == null)
					throw new IllegalStateException("Block "+sortedBlock.getHash()+" does not have a header");
				
				if (mergeBlocks.isEmpty() == false || (mergeBlocks.isEmpty() == true && this.blocks.getLast().getHash().equals(sortedBlock.getHeader().getPrevious()) == true))
					mergeBlocks.add(sortedBlock);
			}
			
			if (mergeBlocks.isEmpty() == false)
			{
				if (blocksLog.hasLevel(Logging.DEBUG) == true)
					blocksLog.debug(this.context.getName()+": Merging branch "+this.blocks+" with "+blocks);
				
				for (PendingBlock mergeBlock : mergeBlocks)
				{
					try
					{
						if (blocksLog.hasLevel(Logging.DEBUG) == true)
							blocksLog.debug(this.context.getName()+": Adding merge block "+mergeBlock);

						add(mergeBlock);
					}
					catch (Exception ex)
					{
						throw ex;
					}
				}
				
				return true;
			}
			
			for (PendingBlock sortedBlock : sortedBlocks)
			{
				if (sortedBlock.getHeight() <= this.root.getHeight())
					continue;
				
				if (this.contains(sortedBlock) == false)
					return false;
			}
			
			return true;
		}
		catch (Exception ex)
		{
			throw ex;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	boolean forks(final Collection<PendingBlock> blocks)
	{
		Objects.requireNonNull(blocks, "Blocks is null");

		this.lock.lock();
		try
		{
			List<PendingBlock> sortedBlocks = new ArrayList<PendingBlock>(blocks);
			Collections.sort(sortedBlocks, new Comparator<PendingBlock>() 
			{
				@Override
				public int compare(PendingBlock arg0, PendingBlock arg1)
				{
					if (arg0.getHeight() < arg1.getHeight())
						return 1;
					
					if (arg0.getHeight() > arg1.getHeight())
						return -1;

					return 0;
				}
			});

			for (PendingBlock sortedBlock : sortedBlocks)
			{
				if (sortedBlock.getHeader() == null)
					throw new IllegalStateException("Block "+sortedBlock.getHash()+" does not have a header");

				for (PendingBlock vertex : this.blocks)
				{
					if (vertex.getHash().equals(sortedBlock.getHeader().getPrevious()) == true)
					{
						if (vertex.equals(this.blocks.getLast()) == false)
							return true;
						else
							return false;
					}
				}
			}
			
			return false;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	boolean forks(final PendingBlock block)
	{
		if (Objects.requireNonNull(block, "Block is null").getHeader() == null)
			throw new IllegalStateException("Block "+block.getHash()+" does not have a header");

		this.lock.lock();
		try
		{
			for (PendingBlock vertex : this.blocks)
			{
				if (vertex.getHash().equals(block.getHeader().getPrevious()) == true)
				{
					if (vertex.equals(this.blocks.getLast()) == false)
						return true;
					else
						return false;
				}
			}
			
			return false;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	PendingBranch fork(final Collection<PendingBlock> blocks) throws StateLockedException, IOException
	{
		Objects.requireNonNull(blocks, "Blocks is null");

		this.lock.lock();
		try
		{
			List<PendingBlock> sortedBlocks = new ArrayList<PendingBlock>(blocks);
			Collections.sort(sortedBlocks, new Comparator<PendingBlock>() 
			{
				@Override
				public int compare(PendingBlock arg0, PendingBlock arg1)
				{
					if (arg0.getHeight() < arg1.getHeight())
						return 1;
					
					if (arg0.getHeight() > arg1.getHeight())
						return -1;

					return 0;
				}
			});

			PendingBlock forkBlock = null;
			for (PendingBlock sortedBlock : sortedBlocks)
			{
				if (sortedBlock.getHeader() == null)
					throw new IllegalStateException("Block "+sortedBlock.getHash()+" does not have a header");

				for (PendingBlock vertex : this.blocks)
				{
					if (vertex.getHash().equals(sortedBlock.getHeader().getPrevious()) == true)
					{
						if (vertex.equals(this.blocks.getLast()) == false)
						{
							forkBlock = vertex;
							break;
						}
					}
				}
				
				if (forkBlock != null)
					break;
			}
			
			if (forkBlock == null)
				return null;
			
			if (blocksLog.hasLevel(Logging.DEBUG) == true)
				blocksLog.debug(this.context.getName()+": Forking branch "+this.blocks+" from "+forkBlock);
			
			List<PendingBlock> forkBlocks = new LinkedList<PendingBlock>();
			for (PendingBlock vertex : this.blocks)
			{
				if (vertex.getHeight() <= forkBlock.getHeight())
				{
					forkBlocks.add(vertex);

					if (blocksLog.hasLevel(Logging.DEBUG) == true)
						blocksLog.debug(this.context.getName()+": Adding pre-fork block "+vertex);
				}
				else
					break;
			}

			Collections.reverse(sortedBlocks);
			for (PendingBlock sortedBlock : sortedBlocks)
			{
				if (sortedBlock.getHeight() > forkBlock.getHeight())
				{
					forkBlocks.add(sortedBlock);

					if (blocksLog.hasLevel(Logging.DEBUG) == true)
						blocksLog.debug(this.context.getName()+": Adding fork block "+sortedBlock);
				}
			}
			
			return new PendingBranch(this.context, Type.FORK, this.root, this.context.getLedger().getStateAccumulator().shadow(), forkBlocks);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	PendingBranch fork(final PendingBlock block) throws StateLockedException, IOException
	{
		if (Objects.requireNonNull(block, "Block is null").getHeader() == null)
			throw new IllegalStateException("Block "+block.getHash()+" does not have a header");

		this.lock.lock();
		try
		{
			PendingBlock forkBlock = null;
			for (PendingBlock vertex : this.blocks)
			{
				if (vertex.getHash().equals(block.getHeader().getPrevious()) == true)
				{
					if (vertex.equals(this.blocks.getLast()) == false)
					{
						forkBlock = vertex;
						break;
					}
				}
			}
			
			if (forkBlock == null)
				return null;
			
			if (blocksLog.hasLevel(Logging.DEBUG) == true)
				blocksLog.debug(this.context.getName()+": Forking branch "+this.blocks+" from "+forkBlock);
			
			List<PendingBlock> forkBlocks = new LinkedList<PendingBlock>();
			for (PendingBlock vertex : this.blocks)
			{
				if (vertex.getHeight() <= forkBlock.getHeight())
				{
					forkBlocks.add(vertex);
					
					if (blocksLog.hasLevel(Logging.DEBUG) == true)
						blocksLog.debug(this.context.getName()+": Adding pre-fork block "+vertex);
				}
				else
					break;
			}

			forkBlocks.add(block);	
			
			if (blocksLog.hasLevel(Logging.DEBUG) == true)
				blocksLog.debug(this.context.getName()+": Adding fork block "+block);
			
			return new PendingBranch(this.context, Type.FORK, this.root, this.context.getLedger().getStateAccumulator().shadow(), forkBlocks);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	boolean intersects(final PendingBlock block)
	{
		if (Objects.requireNonNull(block, "Block is null").getHeader() == null)
			throw new IllegalStateException("Block "+block.getHash()+" does not have a header");

		this.lock.lock();
		try
		{
			for (PendingBlock vertex : this.blocks)
				if (vertex.getHash().equals(block.getHeader().getPrevious()) == true)
					return true;
			
			return false;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	boolean intersects(final Collection<PendingBlock> blocks)
	{
		Objects.requireNonNull(blocks, "Blocks is null");

		this.lock.lock();
		try
		{
			List<PendingBlock> sortedBlocks = new ArrayList<PendingBlock>(blocks);
			Collections.sort(sortedBlocks, new Comparator<PendingBlock>() 
			{
				@Override
				public int compare(PendingBlock arg0, PendingBlock arg1)
				{
					if (arg0.getHeight() < arg1.getHeight())
						return 1;
					
					if (arg0.getHeight() > arg1.getHeight())
						return -1;

					return 0;
				}
			});

			for (PendingBlock vertex : this.blocks)
				for (PendingBlock sortedBlock : sortedBlocks)
					if (vertex.getHash().equals(sortedBlock.getHeader().getPrevious()) == true)
						return true;
			
			return false;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	/**
	 * Trims the branch to the block header (inclusive)
	 * 
	 * @param header
	 */
	void trimTo(final BlockHeader header)
	{
		Objects.requireNonNull(header, "Block is null");

		this.lock.lock();
		try
		{
			Iterator<PendingBlock> vertexIterator = this.blocks.iterator();
			while(vertexIterator.hasNext() == true)
			{
				PendingBlock vertex = vertexIterator.next();
				if (vertex.getHeader().getHeight() <= header.getHeight())
					vertexIterator.remove();
			}

			this.root = header;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	LinkedList<PendingBlock> commit(final PendingBlock block) throws IOException, StateLockedException
	{
		if (Objects.requireNonNull(block, "Block is null").getHeader() == null)
			throw new IllegalStateException("Block "+block.getHash()+" does not have a header");

		this.lock.lock();
		try
		{
			LinkedList<PendingBlock> committed = new LinkedList<PendingBlock>();
			Iterator<PendingBlock> vertexIterator = this.blocks.iterator();
			while(vertexIterator.hasNext() == true)
			{
				PendingBlock vertex = vertexIterator.next();
				if (vertex.getBlock() == null)
					break;
				
				vertex.certificate();
				this.context.getLedger().getLedgerStore().commit(vertex.getBlock());
				committed.add(vertex);
				vertexIterator.remove();
				
				if (vertex.getHash().equals(block.getHash()) == true)
					break;
			}
			
			if (committed.isEmpty() == false)
			{
				if (committed.getLast().equals(block) == false)
					blocksLog.warn(this.context.getName()+": Committed partial branch "+this.root+" -> "+committed.getLast().getHeader());

				this.root = committed.getLast().getHeader();
			}
			
			return committed;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	PendingBlock commitable() throws IOException
	{
		this.lock.lock();
		try
		{
			// See if there is a section of the best branch that can be committed (any block that has 2f+1 agreement)
			// TODO using pendingBlock.getHeader().getHeight() as the vote power timestamp possibly makes this weakly subjective and may cause issue in long branches
			Iterator<PendingBlock> vertexIterator = this.blocks.descendingIterator();
			while(vertexIterator.hasNext())
			{
				PendingBlock vertex = vertexIterator.next();
				if (vertex.getBlock() == null)
					continue;
				
				long weight = getWeight(vertex.getHeight());
				long total = getTotalVotePower(vertex.getHeight());
				long threshold = getVotePowerThreshold(vertex.getHeight());
				if (weight >= threshold)
				{
					blocksLog.info(this.context.getName()+": Found commit at block with weight "+weight+"/"+total+" to commit list "+vertex);
					return vertex;
				}
			}
			
			return null;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	private void lock(final PendingBlock block) throws IOException, StateLockedException
	{
		Objects.requireNonNull(block, "Pending block is null");
		
		this.lock.lock();
		try
		{
			final List<AtomCertificate> certificates = block.getCertificates();
			for (AtomCertificate certificate : certificates)
			{
				try
				{
					PendingAtom pendingAtom = PendingBranch.this.context.getLedger().getAtomHandler().get(certificate.getAtom());
					if (pendingAtom == null)
						throw new IllegalStateException("Pending atom "+certificate.getAtom()+" referenced in certificate not found for state unlock");
					
					this.accumulator.unlock(pendingAtom);
				}
				catch (Exception ex)
				{
					throw ex;
				}
			}

			final List<PendingAtom> pendingAtoms = block.getAtoms();
			for (PendingAtom pendingAtom : pendingAtoms)
			{
				try
				{
					this.accumulator.lock(pendingAtom);
				}
				catch (Exception ex)
				{
					throw ex;
				}
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}

	StateAccumulator getStateAccumulator()
	{
		return this.accumulator;
	}

	BlockHeader getRoot()
	{
		this.lock.lock();
		try
		{
			return this.root;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	LinkedList<PendingBlock> getBlocks()
	{
		this.lock.lock();
		try
		{
			return new LinkedList<PendingBlock>(this.blocks);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	boolean isEmpty()
	{
		this.lock.lock();
		try
		{
			return this.blocks.isEmpty();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public PendingBlock get(final Hash block)
	{
		Objects.requireNonNull(block, "Block hash is null");
		Hash.notZero(block, "Block hash is ZERO");
		
		this.lock.lock();
		try
		{
			for (PendingBlock vertex : this.blocks)
				if (vertex.getHash().equals(block) == true)
					return vertex;
			
			return null;
		}
		finally
		{
			this.lock.unlock();
		}
	}


	PendingBlock getLow()
	{
		this.lock.lock();
		try
		{
			return this.blocks.getFirst();
		}
		finally
		{
			this.lock.unlock();
		}
	}

	PendingBlock getHigh()
	{
		this.lock.lock();
		try
		{
			return this.blocks.getLast();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	@Override
	public String toString()
	{
		return getBlocks().stream().map(pb -> pb.getHash().toString()).collect(Collectors.joining(" -> "));
	}

	public int size()
	{
		this.lock.lock();
		try
		{
			return this.blocks.size();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	// Vote power and weights //
	long getVotePower(final long height, final ECPublicKey identity) throws IOException
	{
		Objects.requireNonNull(identity, "Identity is null");
		Numbers.isNegative(height, "Height is negative");
		
		long committedPower = this.context.getLedger().getValidatorHandler().getVotePower(Math.max(0, height - ValidatorHandler.VOTE_POWER_MATURITY), identity);
		long pendingPower = 0;
		this.lock.lock();
		try
		{
			for (PendingBlock vertex : this.blocks)
			{
				if (vertex.getHeight() <= height - ValidatorHandler.VOTE_POWER_MATURITY && vertex.getHeader().getOwner().equals(identity) == true)
					pendingPower++;
			}
		}
		finally
		{
			this.lock.unlock();
		}
		
		return committedPower+pendingPower;
	}

	long getWeight(final long height) throws IOException
	{
		Numbers.isNegative(height, "Height is negative");
		
		this.lock.lock();
		try
		{
			PendingBlock block = getBlockAtHeight(height);
			if (block == null)
				throw new IllegalStateException("Expected to find pending block at height "+height);
			
			long blockWeight = 0;
			for (BlockVote vote : block.votes())
			{
				long voteWeight = getVotePower(vote.getHeight(), vote.getOwner());
				if (voteWeight == 0)
					blocksLog.warn(this.context.getName()+": Block vote "+vote.getHash()+" has zero weight for block "+block.getBlock()+" from "+vote.getOwner());
				
				blockWeight += voteWeight;
			}
				
			return blockWeight;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	long getVotePowerThreshold(final long height) throws IOException
	{
		Numbers.isNegative(height, "Height is negative");
		return twoFPlusOne(getTotalVotePower(height));
	}

	long getTotalVotePower(final long height) throws IOException
	{
		Numbers.isNegative(height, "Height is negative");
		
		long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups()); 
		long committedPower = this.context.getLedger().getValidatorHandler().getTotalVotePower(Math.max(0, height - ValidatorHandler.VOTE_POWER_MATURITY), localShardGroup);
		long pendingPower = 0;
		this.lock.lock();
		try
		{
			for (PendingBlock vertex : this.blocks)
			{
				if (vertex.getHeight() <= height - ValidatorHandler.VOTE_POWER_MATURITY)
					pendingPower++;
			}
		}
		finally
		{
			this.lock.unlock();
		}
		
		return committedPower+pendingPower;
	}
	
	private long twoFPlusOne(final long power)
	{
		Numbers.isNegative(power, "Power is negative");

		long F = Math.max(1, power / 3);
		long T = F * 2;
		return Math.min(power, T + 1);
	}
	
	private PendingBlock getBlockAtHeight(final long height)
	{
		Numbers.isNegative(height, "Height is negative");
		
		this.lock.lock();
		try
		{
			PendingBlock block = null;
			for (PendingBlock vertex : this.blocks)
			{
				if (vertex.getHeight() == height)
				{
					block = vertex;
					break;
				}
			}
			return block;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public boolean isConstructed()
	{
		this.lock.lock();
		try
		{
			for (PendingBlock vertex : this.blocks)
			{
				if (vertex.getBlock() == null)
					return false;
			}
			return true;
		}
		finally
		{
			this.lock.unlock();
		}
	}
}
