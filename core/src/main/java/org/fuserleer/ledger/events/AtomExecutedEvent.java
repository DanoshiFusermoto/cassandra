package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.PendingAtom;

public class AtomExecutedEvent extends AtomEvent 
{
	private final ValidationException thrown;
	
	public AtomExecutedEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);
		this.thrown = null;
	}
	
	public AtomExecutedEvent(final PendingAtom pendingAtom, final ValidationException thrown)
	{
		super(pendingAtom);
		this.thrown = Objects.requireNonNull(thrown, "ValidationException is null");
	}
	
	public ValidationException thrown()
	{
		return this.thrown;
	}
}