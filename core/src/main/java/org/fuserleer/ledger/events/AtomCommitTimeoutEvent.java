package org.fuserleer.ledger.events;

import org.fuserleer.ledger.atoms.Atom;

public final class AtomCommitTimeoutEvent extends AtomEvent
{
	public AtomCommitTimeoutEvent(Atom atom)
	{
		super(atom);
	}
}
