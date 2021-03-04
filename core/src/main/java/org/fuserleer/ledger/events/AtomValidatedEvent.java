package org.fuserleer.ledger.events;

import org.fuserleer.ledger.PendingAtom;

public final class AtomValidatedEvent extends AtomEvent 
{
	public AtomValidatedEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);
	}
}