package org.fuserleer.ledger.events;

import org.fuserleer.ledger.CommitStatus;
import org.fuserleer.ledger.PendingAtom;

public final class AtomDiscardedEvent extends AtomEvent
{
	private final String message;
	
	public AtomDiscardedEvent(final PendingAtom pendingAtom)
	{
		this(pendingAtom, null);
	}
	
	public AtomDiscardedEvent(final PendingAtom pendingAtom, final String message)
	{
		super(pendingAtom);
		
		if (pendingAtom.getStatus().greaterThan(CommitStatus.PREPARED) == true)
			throw new IllegalStateException("Can not discard pending atom "+pendingAtom.getHash()+" with status "+pendingAtom.getStatus());
		
		this.message = message;
	}
	
	public String getMessage()
	{
		return this.message;
	}
}
