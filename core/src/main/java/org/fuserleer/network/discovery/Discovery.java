package org.fuserleer.network.discovery;

import java.io.IOException;
import java.util.Collection;

import org.fuserleer.network.peers.Peer;
import org.fuserleer.network.peers.filters.PeerFilter;

public interface Discovery
{
	public Collection<Peer> discover(PeerFilter filter) throws IOException;
}
