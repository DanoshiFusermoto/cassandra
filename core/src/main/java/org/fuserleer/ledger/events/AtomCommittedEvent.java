package org.fuserleer.ledger.events;

import org.fuserleer.ledger.atoms.Atom;

public final class AtomCommittedEvent extends AtomEvent 
{
	public AtomCommittedEvent(Atom atom)
	{
		super(atom);
	}
}