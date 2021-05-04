package org.fuserleer.ledger.events;

import org.fuserleer.ledger.CommitStatus;
import org.fuserleer.ledger.PendingAtom;

public final class AtomAcceptedTimeoutEvent extends AtomEvent
{
	public AtomAcceptedTimeoutEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);
		
		if (pendingAtom.getStatus().greaterThan(CommitStatus.PREPARED) == true)
			throw new IllegalStateException("Can not timeout pending atom "+pendingAtom.getHash()+" with status "+pendingAtom.getStatus());
	}
}
