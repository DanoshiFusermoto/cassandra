package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.ledger.Block;

public final class SyncBlockEvent extends BlockEvent
{
	public SyncBlockEvent(final Block block)
	{
		super(Objects.requireNonNull(block, "Block is null"));
	}
}