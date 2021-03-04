package org.fuserleer.ledger.events;

import org.fuserleer.ledger.PendingAtom;

public final class AtomRejectedEvent extends AtomEvent
{
	public AtomRejectedEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);
		
		if (pendingAtom.getCertificate() == null)
			throw new IllegalArgumentException("Atom certificate not present for "+pendingAtom.getHash());
	}
}
