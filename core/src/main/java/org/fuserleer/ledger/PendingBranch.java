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
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

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
	
	private BlockHeader head;

	private final ReentrantLock lock = new ReentrantLock();
	
	PendingBranch(Context context, Type type, BlockHeader head, StateAccumulator accumulator)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.type = Objects.requireNonNull(type, "Type is null");
		this.id = ThreadLocalRandom.current().nextLong();
		this.blocks = new LinkedList<PendingBlock>();
		this.head = Objects.requireNonNull(head, "Head is null");
		this.accumulator = Objects.requireNonNull(accumulator, "Accumulator is null");
	}

	PendingBranch(Context context, Type type, BlockHeader head, StateAccumulator accumulator, PendingBlock block) throws ValidationException, IOException, StateLockedException
	{
		this(context, type, head, accumulator);

		add(block);
	}
	
	PendingBranch(Context context, Type type, BlockHeader head, StateAccumulator accumulator, Collection<PendingBlock> blocks) throws StateLockedException, IOException
	{
		this(context, type, head, accumulator);
		
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

	boolean add(PendingBlock block) throws StateLockedException, IOException
	{
		if (Objects.requireNonNull(block, "Block is null").getHeader() == null)
			throw new IllegalStateException("Block "+block.getHash()+" does not have a header");
		
		this.lock.lock();
		try
		{
			if (this.blocks.contains(block) == true)
				return false;
			
			if (block.getHeight() <= this.head.getHeight())
				blocksLog.warn(this.context.getName()+": Block is "+block.getHash()+" is before branch head "+this.head.getHash());
			
			boolean foundPrev = false;
			for (PendingBlock vertex : this.blocks)
			{
				if (vertex.getHash().equals(block.getHeader().getPrevious()) == true)
				{
					foundPrev = true;
					break;
				}
			}
			
			if (foundPrev == false && this.head.getHash().equals(block.getHeader().getPrevious()) == false)
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
	
	boolean contains(Hash block) throws StateLockedException
	{
		Objects.requireNonNull(block, "Block is null");
		
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

	boolean merge(Collection<PendingBlock> blocks) throws StateLockedException, IOException
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
				if (sortedBlock.getHeight() <= this.head.getHeight())
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

	boolean forks(Collection<PendingBlock> blocks)
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

	boolean forks(PendingBlock block)
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

	PendingBranch fork(Collection<PendingBlock> blocks) throws StateLockedException, IOException
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
			
			return new PendingBranch(this.context, Type.FORK, this.head, this.context.getLedger().getStateAccumulator().shadow(), forkBlocks);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	PendingBranch fork(PendingBlock block) throws StateLockedException, IOException
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
			
			return new PendingBranch(this.context, Type.FORK, this.head, this.context.getLedger().getStateAccumulator().shadow(), forkBlocks);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	boolean intersects(PendingBlock block)
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
	
	boolean intersects(Collection<PendingBlock> blocks)
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
	 * @throws IOException 
	 * @throws StateLockedException 
	 */
	void trimTo(PendingBlock block) throws IOException, StateLockedException
	{
		if (Objects.requireNonNull(block, "Block is null").getHeader() == null)
			throw new IllegalStateException("Block "+block.getHash()+" does not have a header");

		this.lock.lock();
		try
		{
			Iterator<PendingBlock> vertexIterator = this.blocks.iterator();
			while(vertexIterator.hasNext() == true)
			{
				PendingBlock vertex = vertexIterator.next();
				if (vertex.getHeader().getHeight() <= block.getHeight())
					vertexIterator.remove();
			}

			this.head = block.getHeader();
		}
		finally
		{
			this.lock.unlock();
		}
	}

	LinkedList<PendingBlock> commit(PendingBlock block) throws IOException, StateLockedException
	{
		if (Objects.requireNonNull(block, "Block is null").getHeader() == null)
			throw new IllegalStateException("Block "+block.getHash()+" does not have a header");

		this.lock.lock();
		try
		{
			// TODO the blocks will be committed separately when sharded
			LinkedList<PendingBlock> committed = new LinkedList<PendingBlock>();
			Iterator<PendingBlock> vertexIterator = this.blocks.iterator();
			while(vertexIterator.hasNext() == true)
			{
				PendingBlock vertex = vertexIterator.next();
				vertex.certificate();
				this.context.getLedger().getLedgerStore().commit(vertex.getBlock());
				committed.add(vertex);
				vertexIterator.remove();
				
				if (vertex.getHash().equals(block.getHash()) == true)
					break;
			}
			
			this.head = block.getHeader();
			return committed;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	PendingBlock commitable()
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
				long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups(vertex.getHeight()));
				if (vertex.weight() >= this.context.getLedger().getVoteRegulator().getVotePowerThreshold(vertex.getHeight() - VoteRegulator.VOTE_POWER_MATURITY, Collections.singleton(localShardGroup)))
				{
					blocksLog.info(this.context.getName()+": Found commit at block with weight "+vertex.weight()+"/"+this.context.getLedger().getVoteRegulator().getTotalVotePower(vertex.getHeight() - VoteRegulator.VOTE_POWER_MATURITY, localShardGroup)+" to commit list "+vertex);
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

	private void lock(PendingBlock block) throws IOException, StateLockedException
	{
		this.lock.lock();
		try
		{
			List<AtomCertificate> certificates = block.getCertificates();
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

			List<PendingAtom> pendingAtoms = block.getAtoms();
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

	BlockHeader getHead()
	{
		this.lock.lock();
		try
		{
			return this.head;
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
	
	boolean contains(PendingBlock pendingBlock)
	{
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

	PendingBlock getFirst()
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

	PendingBlock getLast()
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
}
