package org.fuserleer.ledger.events;

import org.fuserleer.ledger.atoms.AtomCertificate;

public class AtomCertificateEvent extends CertificateEvent<AtomCertificate>
{
	public AtomCertificateEvent(final AtomCertificate certificate)
	{
		super(certificate);
	}
}
