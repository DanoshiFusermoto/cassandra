package org.fuserleer.ledger.events;

import org.fuserleer.ledger.Block;

public final class BlockCommittedEvent extends BlockEvent
{
	public BlockCommittedEvent(Block block)
	{
		super(block);
	}
}
