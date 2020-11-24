package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.events.Event;

public final class AtomTimeoutEvent implements Event
{
	private final Hash atom;
	
	public AtomTimeoutEvent(Hash atom)
	{
		this.atom = Objects.requireNonNull(atom);
	}
	
	public Hash getAtom()
	{
		return this.atom;
	}
}
