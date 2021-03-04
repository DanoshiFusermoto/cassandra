package org.fuserleer.ledger;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;

public final class AtomFuture extends CompletableFuture<AtomCertificate>
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