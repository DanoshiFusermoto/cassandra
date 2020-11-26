package org.fuserleer.ledger.events;

import org.fuserleer.ledger.BlockHeader;

public final class BlockCommittedEvent extends BlockEvent
{
	public BlockCommittedEvent(BlockHeader block)
	{
		super(block);
	}
}
