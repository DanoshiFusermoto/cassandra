package org.fuserleer.network.peers.events;

import org.fuserleer.network.peers.ConnectedPeer;

public final class PeerConnectingEvent extends PeerEvent
{
	public PeerConnectingEvent(ConnectedPeer peer)
	{
		super(peer);
	}
}
