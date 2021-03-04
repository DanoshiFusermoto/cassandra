package org.fuserleer.ledger.events;

import org.fuserleer.ledger.PendingAtom;

public final class AtomPersistedEvent extends AtomEvent 
{
	public AtomPersistedEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);
	}
}