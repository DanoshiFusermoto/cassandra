package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.events.Event;
import org.fuserleer.ledger.Block;

abstract class BlockEvent implements Event 
{
	private final Block block;
	
	BlockEvent(Block block)
	{
		this.block = Objects.requireNonNull(block);
	}
	
	public final Block getBlock()
	{
		return this.block;
	}
}
