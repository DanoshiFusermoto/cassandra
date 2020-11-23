package org.fuserleer.network.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
	private List<Peer> peers = new ArrayList<>();

	public PeersMessage ()
	{
		super();
	}

	public List<Peer> getPeers() 
	{ 
		return this.peers; 
	}

	public void setPeers(Collection<Peer> peers)
	{
		this.peers.clear();
		this.peers.addAll(peers);
	}
}
