package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Objects;

import org.fuserleer.Context;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

class PendingBranch
{
	private static final Logger blocksLog = Logging.getLogger("blocks");

	private final Context context;
	private final LinkedList<PendingBlock> blocks;
	private final StateAccumulator accumulator;
	
	private BlockHeader head;
	
	PendingBranch(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.blocks = new LinkedList<PendingBlock>();
		this.head = context.getLedger().getHead();
		this.accumulator = new StateAccumulator(context);
	}

	PendingBranch(Context context, PendingBlock block) throws ValidationException, IOException
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.blocks = new LinkedList<PendingBlock>();
		this.head = context.getLedger().getHead();
		this.accumulator = new StateAccumulator(context);

		validate(block);
		this.blocks.add(block);
	}
	
	PendingBranch(Context context, Collection<PendingBlock> blocks) throws ValidationException, IOException
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.blocks = new LinkedList<PendingBlock>();
		this.head = context.getLedger().getHead();
		this.accumulator = new StateAccumulator(context);
		
		for (PendingBlock block : blocks)
		{
			validate(block);
			this.blocks.add(block);
		}
	}
	
	void add(PendingBlock block) throws ValidationException, IOException
	{
		validate(block);
		this.blocks.add(block);
	}
	
	boolean intersects(BlockHeader header)
	{
		for (PendingBlock pendingBlock : this.blocks)
			if (pendingBlock.getBlockHeader().getPrevious().equals(header.getHash()) == true)
				return true;
		
		return false;
	}
	
	void alignTo(BlockHeader header)
	{
		Iterator<PendingBlock> pendingBlockIterator = this.blocks.descendingIterator();
		while(pendingBlockIterator.hasNext() == true)
		{
			PendingBlock pendingBlock = pendingBlockIterator.next();
			if (pendingBlock.getBlockHeader().getPrevious().equals(header.getHash()) == true)
				break;
		}
		
		this.accumulator.alignTo(header);
	}

	LinkedList<PendingBlock> commit(BlockHeader header) throws IOException
	{
		// TODO the blocks will be committed separately when sharded
		LinkedList<PendingBlock> committed = new LinkedList<PendingBlock>();
		Iterator<PendingBlock> pendingBlockIterator = this.blocks.iterator();
		while(pendingBlockIterator.hasNext() == true)
		{
			PendingBlock pendingBlock = pendingBlockIterator.next();
			pendingBlock.certificate();
			this.context.getLedger().getLedgerStore().commit(pendingBlock.getBlock());
			committed.add(pendingBlock);
			
			if (pendingBlock.getHash().equals(header.getHash()) == true)
				break;
		}
			
		this.accumulator.commit(header);
		
		pendingBlockIterator = this.blocks.descendingIterator();
		while(pendingBlockIterator.hasNext() == true)
		{
			PendingBlock pendingBlock = pendingBlockIterator.next();
			if (pendingBlock.getBlockHeader().getHeight() <= header.getHeight())
				pendingBlockIterator.remove();
		}
		
		this.head = header;
		return committed;
	}
	
	PendingBlock commitable()
	{
		// See if there is a section of the best branch that can be committed (any block that has 2f+1 agreement)
		Iterator<PendingBlock> pendingBlockIterator = this.blocks.descendingIterator();
		while(pendingBlockIterator.hasNext())
		{
			PendingBlock pendingBlock = pendingBlockIterator.next();
			if (pendingBlock.weight().compareTo(this.context.getLedger().getVoteRegulator().getVotePowerThreshold(pendingBlock.getBlockHeader().getHeight())) >= 0)
			{
				blocksLog.info(this.context.getName()+": Found commit at block with weight "+pendingBlock.weight()+"/"+this.context.getLedger().getVoteRegulator().getTotalVotePower(pendingBlock.getBlockHeader().getHeight())+" to commit list "+pendingBlock);
				return pendingBlock;
			}
		}
		
		return null;
	}

	private void validate(PendingBlock block) throws ValidationException, IOException
	{
		for (Atom atom : block.getBlock().getAtoms())
		{
			StateMachine stateMachine = new StateMachine(this.context, block.getBlockHeader(), atom, this.accumulator);
			stateMachine.execute();
		}
	}

	private Context getContext()
	{
		return this.context;
	}

	StateAccumulator getStateAccumulator()
	{
		return this.accumulator;
	}

	BlockHeader getHead()
	{
		return this.head;
	}

	LinkedList<PendingBlock> getBlocks()
	{
		return new LinkedList<PendingBlock>(this.blocks);
	}

	boolean isEmpty()
	{
		return this.blocks.isEmpty();
	}
	
	boolean contains(PendingBlock pendingBlock)
	{
		for (PendingBlock block : this.blocks)
			if (block.equals(pendingBlock) == true)
				return true;
		
		return false;
	}

	PendingBlock getFirst()
	{
		return this.blocks.getFirst();
	}

	PendingBlock getLast()
	{
		return this.blocks.getLast();
	}
}
