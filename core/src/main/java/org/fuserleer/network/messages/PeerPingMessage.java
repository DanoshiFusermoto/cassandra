package org.fuserleer.network.messages;

import org.fuserleer.node.Node;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("network.message.ping")
public final class PeerPingMessage extends NodeMessage
{
	@JsonProperty("nonce")
	@DsonOutput(Output.ALL)
	private long nonce;
	
	PeerPingMessage()
	{
		super();
	}

	public PeerPingMessage(Node node, long nonce)
	{
		super(node);
		
		this.nonce = nonce;
	}

	public long getNonce() 
	{ 
		return this.nonce; 
	}
}
