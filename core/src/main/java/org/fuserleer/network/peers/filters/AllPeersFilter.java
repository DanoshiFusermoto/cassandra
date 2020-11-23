package org.fuserleer.network.peers.filters;

import org.fuserleer.network.peers.Peer;

public class AllPeersFilter implements PeerFilter
{
	@Override
	public boolean filter(Peer peer)
	{
		return false;
	}
}
