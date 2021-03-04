package org.fuserleer.network.peers.filters;

import org.fuserleer.network.peers.Peer;

public final class AllPeersFilter implements PeerFilter<Peer>
{
	@Override
	public boolean filter(final Peer peer)
	{
		return true;
	}
}
