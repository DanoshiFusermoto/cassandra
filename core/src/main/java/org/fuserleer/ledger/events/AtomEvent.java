package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.events.Event;
import org.fuserleer.ledger.PendingAtom;
import org.fuserleer.ledger.atoms.Atom;

abstract class AtomEvent implements Event 
{
	private final PendingAtom pendingAtom;
	
	AtomEvent(final PendingAtom pendingAtom)
	{
		this.pendingAtom = Objects.requireNonNull(pendingAtom);
	}
	
	public final PendingAtom getPendingAtom()
	{
		return this.pendingAtom;
	}

	public final Atom getAtom()
	{
		return this.pendingAtom.getAtom();
	}
}
