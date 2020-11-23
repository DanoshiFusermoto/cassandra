package org.fuserleer.network.peers.events;

import org.fuserleer.network.peers.ConnectedPeer;

public final class PeerAvailableEvent extends PeerEvent
{
	public PeerAvailableEvent(ConnectedPeer peer)
	{
		super(peer);
	}
}