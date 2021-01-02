package org.fuserleer.ledger.events;

import org.fuserleer.ledger.StateCertificate;

public final class StateCertificateEvent extends CertificateEvent<StateCertificate>
{
	public StateCertificateEvent(StateCertificate certificate)
	{
		super(certificate);
	}
}
