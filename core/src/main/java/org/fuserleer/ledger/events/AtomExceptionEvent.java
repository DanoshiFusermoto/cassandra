package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.ledger.PendingAtom;

public final class AtomExceptionEvent extends AtomEvent
{
	private final Exception exception;
	
	public AtomExceptionEvent(final PendingAtom pendingAtom, final Exception exception)
	{
		super(pendingAtom);
		
		this.exception = Objects.requireNonNull(exception);
	}
	
	public Exception getException()
	{
		return this.exception;
	}
}

