package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.atoms.ParticleCertificate;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.particle.certificate")
public final class ParticleCertificateMessage extends Message
{
	@JsonProperty("certificate")
	@DsonOutput(Output.ALL)
	private ParticleCertificate certificate;

	ParticleCertificateMessage()
	{
		super();
	}

	public ParticleCertificateMessage(ParticleCertificate certificate)
	{
		super();

		Objects.requireNonNull(certificate, "Certificate is null");
		this.certificate = certificate;
	}

	public ParticleCertificate getCertificate()
	{
		return this.certificate;
	}
}
