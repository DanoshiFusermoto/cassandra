package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.ledger.atoms.Atom;

public final class AtomCommittedEvent extends AtomEvent 
{
	private final BlockHeader block;
	
	public AtomCommittedEvent(BlockHeader block, Atom atom)
	{
		super(atom);
		
		this.block = Objects.requireNonNull(block);
		if (this.block.getInventory(Atom.class).contains(atom.getHash()) == false)
			throw new IllegalArgumentException("Block "+block+" doesnt reference atom "+atom);
	}
	
	public BlockHeader getBlockHeader()
	{
		return this.block;
	}
}