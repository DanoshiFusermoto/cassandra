package org.fuserleer.network.peers.events;

import org.fuserleer.network.peers.ConnectedPeer;

public final class PeerDisconnectedEvent extends PeerEvent
{
	public PeerDisconnectedEvent(ConnectedPeer peer)
	{
		super(peer);
	}
}
