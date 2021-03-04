package org.fuserleer.network.peers.events;

import org.fuserleer.network.peers.ConnectedPeer;

public final class PeerBannedEvent extends PeerEvent
{
	public PeerBannedEvent(final ConnectedPeer peer)
	{
		super(peer);
	}
}
