package org.fuserleer.ledger.events;

import java.util.Objects;

import org.fuserleer.crypto.Certificate;
import org.fuserleer.events.Event;

abstract class CertificateEvent<T extends Certificate> implements Event 
{
	private final T certificate;
	
	CertificateEvent(T certificate)
	{
		this.certificate = Objects.requireNonNull(certificate);
	}
	
	public final T getCertificate()
	{
		return this.certificate;
	}
}
