package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.ledger.atoms.Atom;

public final class AtomCommittedEvent extends AtomEvent 
{
	private final BlockHeader blockHeader;
	
	public AtomCommittedEvent(BlockHeader blockHeader, Atom atom)
	{
		super(atom);
		
		this.blockHeader = Objects.requireNonNull(blockHeader);
	}
	
	public BlockHeader getBlockHeader()
	{
		return this.blockHeader;
	}
}