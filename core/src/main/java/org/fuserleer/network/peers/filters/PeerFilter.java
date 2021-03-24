package org.fuserleer.network.peers.filters;

import java.io.IOException;

import org.fuserleer.network.peers.Peer;

@FunctionalInterface
public interface PeerFilter<T extends Peer>
{
	public boolean filter(final T peer) throws IOException;
}
