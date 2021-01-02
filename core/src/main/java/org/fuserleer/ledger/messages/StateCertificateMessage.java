package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.StateCertificate;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.state.certificate")
public final class StateCertificateMessage extends Message
{
	@JsonProperty("certificate")
	@DsonOutput(Output.ALL)
	private StateCertificate certificate;

	StateCertificateMessage()
	{
		super();
	}

	public StateCertificateMessage(StateCertificate certificate)
	{
		super();

		Objects.requireNonNull(certificate, "Certificate is null");
		this.certificate = certificate;
	}

	public StateCertificate getCertificate()
	{
		return this.certificate;
	}
}
