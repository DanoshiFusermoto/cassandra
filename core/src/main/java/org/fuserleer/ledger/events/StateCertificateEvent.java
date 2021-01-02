package org.fuserleer.ledger.events;

import org.fuserleer.ledger.atoms.ParticleCertificate;

public final class ParticleCertificateEvent extends CertificateEvent<ParticleCertificate>
{
	public ParticleCertificateEvent(ParticleCertificate certificate)
	{
		super(certificate);
	}
}
