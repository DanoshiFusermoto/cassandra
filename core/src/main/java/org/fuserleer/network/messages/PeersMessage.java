package org.fuserleer.network.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.fuserleer.network.messaging.Message;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("network.message.peers")
public final class PeersMessage extends Message
{
	@JsonProperty("peers")
	@DsonOutput(Output.ALL)
	private List<Peer> peers;

	@SuppressWarnings("unused")
	private PeersMessage()
	{
		super();
	}
	
	public PeersMessage(final Collection<Peer> peers)
	{
		Objects.requireNonNull(peers, "Peers is null");
		if (peers.isEmpty() == true)
			throw new IllegalArgumentException("Peer is empty");
		
		this.peers = new ArrayList<Peer>(peers);
	}

	public List<Peer> getPeers() 
	{ 
		return this.peers; 
	}
}
