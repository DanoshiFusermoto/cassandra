package org.fuserleer.ledger.events;

import org.fuserleer.ledger.atoms.Atom;

public class AtomDiscardedEvent extends AtomEvent
{
	private final String message;
	
	public AtomDiscardedEvent(Atom atom)
	{
		this(atom, null);
	}
	
	public AtomDiscardedEvent(Atom atom, String message)
	{
		super(atom);
		
		this.message = message;
	}
	
	public String getMessage()
	{
		return this.message;
	}
}
