package org.fuserleer.ledger.events;

import org.fuserleer.ledger.PendingAtom;

public final class AtomCommitTimeoutEvent extends AtomEvent
{
	public AtomCommitTimeoutEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);
		
		if (pendingAtom.getBlock() == null && pendingAtom.getCertificates().isEmpty() == false)
			throw new IllegalStateException("Can not timeout "+pendingAtom.getHash()+" which has state certificates before being included in a block");
	}
}
