package org.fuserleer.ledger.events;

import org.fuserleer.ledger.atoms.Atom;

public final class AtomPersistedEvent extends AtomEvent 
{
	public AtomPersistedEvent(Atom atom)
	{
		super(atom);
	}
}