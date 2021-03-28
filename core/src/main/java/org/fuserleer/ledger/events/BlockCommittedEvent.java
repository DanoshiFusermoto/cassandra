package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.ledger.Block;
import org.fuserleer.ledger.BlockHeader;

public final class BlockCommittedEvent extends BlockEvent
{
	private final BlockHeader branch;
	
	public BlockCommittedEvent(final Block block, final BlockHeader branch)
	{
		super(Objects.requireNonNull(block, "Block is null"));
		
		Objects.requireNonNull(branch, "Branch header is null");
		if (branch.getHeight() < block.getHeader().getHeight())
			new IllegalArgumentException("Block height "+block.getHeader().getHeight()+" is less than branch header height "+branch.getHeight());
		
		this.branch = branch;
	}
	
	public BlockHeader getBranchHeader()
	{
		return this.branch;
	}
}
