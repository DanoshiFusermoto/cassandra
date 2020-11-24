package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.atoms.Atom;

public final class AtomErrorEvent extends AtomEvent
{
	private final ValidationException error;
	
	public AtomErrorEvent(Atom atom, ValidationException error)
	{
		super(atom);
		
		this.error = Objects.requireNonNull(error);
	}
	
	public ValidationException getError()
	{
		return this.error;
	}
}
