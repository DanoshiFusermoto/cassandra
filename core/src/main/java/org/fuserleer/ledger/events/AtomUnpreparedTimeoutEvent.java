package org.fuserleer.ledger.events;

import org.fuserleer.ledger.CommitStatus;
import org.fuserleer.ledger.PendingAtom;

public class AtomUnpreparedTimeoutEvent extends AtomEvent
{
	public AtomUnpreparedTimeoutEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);
		
		if (pendingAtom.getStatus().greaterThan(CommitStatus.NONE) == true)
			throw new IllegalStateException("Can not timeout unprepared pending atom "+pendingAtom.getHash()+" with status "+pendingAtom.getStatus());
	}
}
