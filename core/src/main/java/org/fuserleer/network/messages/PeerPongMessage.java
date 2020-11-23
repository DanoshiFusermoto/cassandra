package org.fuserleer.network.messages;

import org.fuserleer.node.Node;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("network.message.pong")
public final class PeerPongMessage extends NodeMessage
{
	@JsonProperty("nonce")
	@DsonOutput(Output.ALL)
	private long nonce;

	PeerPongMessage()
	{
		super();

		this.nonce = 0l;
	}

	public PeerPongMessage(Node node, long nonce)
	{
		super(node);

		this.nonce = nonce;
	}

	public long getNonce() 
	{ 
		return this.nonce; 
	}
}
