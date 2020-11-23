package org.fuserleer.network.peers.filters;

import org.fuserleer.network.peers.Peer;

public interface PeerFilter
{
	public boolean filter(Peer peer);
}
