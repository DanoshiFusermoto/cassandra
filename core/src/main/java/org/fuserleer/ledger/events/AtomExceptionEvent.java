package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.ledger.atoms.Atom;

public final class AtomExceptionEvent extends AtomEvent
{
	private final Exception exception;
	
	public AtomExceptionEvent(Atom atom, Exception exception)
	{
		super(atom);
		
		this.exception = Objects.requireNonNull(exception);
	}
	
	public Exception getException()
	{
		return this.exception;
	}
}

