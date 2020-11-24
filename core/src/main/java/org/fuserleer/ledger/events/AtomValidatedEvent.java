package org.fuserleer.ledger.events;

import org.fuserleer.ledger.atoms.Atom;

public final class AtomValidatedEvent extends AtomEvent 
{
	public AtomValidatedEvent(Atom atom)
	{
		super(atom);
	}
}