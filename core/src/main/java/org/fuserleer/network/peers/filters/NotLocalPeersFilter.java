package org.fuserleer.network.peers.filters;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Objects;

import org.fuserleer.network.peers.Peer;
import org.fuserleer.node.Node;

public class NotLocalPeersFilter implements PeerFilter<Peer>
{
	private Node node;
	
	public NotLocalPeersFilter(final Node node)
	{
		Objects.requireNonNull(node, "Node is null");
		this.node = node;
	}
	
	
	@Override
	public boolean filter(final Peer peer) throws IOException
	{
		if (peer.getNode().getIdentity().equals(this.node.getIdentity()) == true)
			return false;

		InetAddress inetAddress = InetAddress.getByName(peer.getURI().getHost());
		if (inetAddress.isAnyLocalAddress() == true || inetAddress.isLinkLocalAddress() == true || inetAddress.isLoopbackAddress() == true)
			return false;
		
		return true;
	}

}
