package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;

public final class AtomCommittedEvent extends AtomEvent 
{
	// TODO dont believe a block reference is needed on the atom commit.  It will
	//		be referenced in the state accumulator and in order to pre-commit we
	//		must have a block with a certificate anyway.  CHECK
//	private final BlockHeader block;
	private final AtomCertificate certificate;
	
	public AtomCommittedEvent(Atom atom, AtomCertificate certificate)
	{
		super(atom);
		
		this.certificate = Objects.requireNonNull(certificate);
		if (this.certificate.getAtom().equals(atom.getHash()) == false)
			throw new IllegalArgumentException("Certificate "+certificate.getHash()+" doesnt reference atom "+atom.getHash());
	}
	
	public AtomCertificate getCertificate()
	{
		return this.certificate;
	}
}