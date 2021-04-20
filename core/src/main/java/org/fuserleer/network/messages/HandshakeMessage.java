package org.fuserleer.network.messages;

import java.util.Objects;

import org.fuserleer.crypto.BLSSignature;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.node.Node;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("message.handshake")
public class HandshakeMessage extends Message
{
	@JsonProperty("node")
	@DsonOutput(Output.ALL)
	private Node node;

	@JsonProperty("ephemeral_key")
	@DsonOutput(Output.ALL)
	private ECPublicKey ephemeralKey;

	@JsonProperty("binding")
	@DsonOutput(Output.ALL)
	private BLSSignature binding;

	protected HandshakeMessage()
	{
		super();
	}
	
	public HandshakeMessage(final Node node, final ECPublicKey ephemeralKey, final BLSSignature binding)
	{
		super();

		this.node = new Node(Objects.requireNonNull(node, "Node is null"));
		this.ephemeralKey = Objects.requireNonNull(ephemeralKey, "Ephemeral key is null");
		this.binding = Objects.requireNonNull(binding, "Key binding signature is null");
	}

	public Node getNode()
	{
		return this.node;
	}
	
	public ECPublicKey getEphemeralKey()
	{
		return this.ephemeralKey;
	}

	public BLSSignature getBinding()
	{
		return this.binding;
	}
}
