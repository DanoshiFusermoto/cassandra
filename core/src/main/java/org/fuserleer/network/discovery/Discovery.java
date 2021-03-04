package org.fuserleer.network.discovery;

import java.io.IOException;
import java.util.Collection;

import org.fuserleer.network.peers.Peer;

public interface Discovery
{
	public Collection<Peer> discover(DiscoveryFilter filter, int limit) throws IOException;
}
