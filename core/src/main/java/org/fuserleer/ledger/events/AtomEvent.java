package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.events.Event;
import org.fuserleer.ledger.atoms.Atom;

abstract class AtomEvent implements Event 
{
	private final Atom atom;
	
	AtomEvent(Atom atom)
	{
		this.atom = Objects.requireNonNull(atom);
	}
	
	public final Atom getAtom()
	{
		return this.atom;
	}
}
