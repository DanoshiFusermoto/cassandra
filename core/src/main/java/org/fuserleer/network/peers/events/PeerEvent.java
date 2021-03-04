package org.fuserleer.network.peers.events;

import java.util.Objects;

import org.fuserleer.events.Event;
import org.fuserleer.network.peers.ConnectedPeer;

public abstract class PeerEvent implements Event 
{
	private final ConnectedPeer peer;
	
	public PeerEvent(final ConnectedPeer peer) 
	{
		super();
		
		this.peer = Objects.requireNonNull(peer, "Connected peer is null");
	}

	public ConnectedPeer getPeer()
	{ 
		return this.peer; 
	}
	
	@Override
	public String toString()
	{
		return super.toString()+" "+this.peer.toString();
	}
}
