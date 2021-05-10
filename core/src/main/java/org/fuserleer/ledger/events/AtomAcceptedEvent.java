package org.fuserleer.ledger.events;

import org.fuserleer.ledger.PendingAtom;

public class AtomAcceptedEvent extends AtomEvent 
{
	public AtomAcceptedEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);
	}
}
