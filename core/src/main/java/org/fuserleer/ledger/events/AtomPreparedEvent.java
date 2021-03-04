package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.PendingAtom;

public final class AtomPreparedEvent extends AtomEvent 
{
	private final BlockHeader block;
	
	public AtomPreparedEvent(final BlockHeader block, final PendingAtom pendingAtom)
	{
		super(pendingAtom);
		
		this.block = Objects.requireNonNull(block);
		if (this.block.getInventory(InventoryType.ATOMS).contains(pendingAtom.getHash()) == false)
			throw new IllegalArgumentException("Block "+block+" doesnt reference atom "+pendingAtom);
	}
	
	public BlockHeader getBlockHeader()
	{
		return this.block;
	}
}