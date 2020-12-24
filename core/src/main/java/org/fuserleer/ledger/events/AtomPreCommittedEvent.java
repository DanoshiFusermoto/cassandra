package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.atoms.Atom;

public final class AtomPreCommittedEvent extends AtomEvent 
{
	private final BlockHeader block;
	
	public AtomPreCommittedEvent(BlockHeader block, Atom atom)
	{
		super(atom);
		
		this.block = Objects.requireNonNull(block);
		if (this.block.getInventory(InventoryType.ATOMS).contains(atom.getHash()) == false)
			throw new IllegalArgumentException("Block "+block+" doesnt reference atom "+atom);
	}
	
	public BlockHeader getBlockHeader()
	{
		return this.block;
	}
}