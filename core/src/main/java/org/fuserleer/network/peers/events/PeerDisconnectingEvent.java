package org.fuserleer.network.peers.events;

import org.fuserleer.network.peers.ConnectedPeer;

public final class PeerDisconnectingEvent extends PeerEvent
{
	public PeerDisconnectingEvent(ConnectedPeer peer)
	{
		super(peer);
	}
}
