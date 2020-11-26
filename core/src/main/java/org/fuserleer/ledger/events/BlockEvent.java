package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.events.Event;
import org.fuserleer.ledger.BlockHeader;

abstract class BlockEvent implements Event 
{
	private final BlockHeader blockHeader;
	
	BlockEvent(BlockHeader blockHeader)
	{
		this.blockHeader = Objects.requireNonNull(blockHeader);
	}
	
	public final BlockHeader getBlockHeader()
	{
		return this.blockHeader;
	}
}
