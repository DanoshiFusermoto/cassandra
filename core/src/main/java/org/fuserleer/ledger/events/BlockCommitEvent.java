package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.ledger.Block;

public class BlockCommitEvent extends BlockEvent
{
	public BlockCommitEvent(final Block block)
	{
		super(Objects.requireNonNull(block, "Block is null"));
	}
}
