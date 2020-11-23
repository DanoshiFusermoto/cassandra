package org.fuserleer.network.peers.events;

import org.fuserleer.network.peers.ConnectedPeer;

public final class PeerConnectedEvent extends PeerEvent
{
	public PeerConnectedEvent(ConnectedPeer peer)
	{
		super(peer);
	}
}
