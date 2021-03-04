package org.fuserleer.network.peers.events;

import org.fuserleer.network.peers.ConnectedPeer;

public final class PeerAvailableEvent extends PeerEvent
{
	public PeerAvailableEvent(final ConnectedPeer peer)
	{
		super(peer);
	}
}