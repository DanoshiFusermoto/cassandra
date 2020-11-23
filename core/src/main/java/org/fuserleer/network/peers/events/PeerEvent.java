package org.fuserleer.network.peers.events;

import org.fuserleer.events.Event;
import org.fuserleer.network.peers.ConnectedPeer;

public abstract class PeerEvent implements Event 
{
	private final ConnectedPeer peer;
	
	public PeerEvent(ConnectedPeer peer) 
	{
		super();
		
		this.peer = peer;
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
