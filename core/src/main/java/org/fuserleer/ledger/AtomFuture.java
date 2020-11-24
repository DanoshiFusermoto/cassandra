package org.fuserleer.ledger;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.fuserleer.ledger.atoms.Atom;

final class AtomFuture extends CompletableFuture<BlockHeader>
{
	private final Atom atom;
	
	public AtomFuture(final Atom atom)
	{
		this.atom = Objects.requireNonNull(atom);
	}

	public Atom getAtom()
	{
		return this.atom;
	}
}