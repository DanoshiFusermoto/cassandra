package org.fuserleer.network.messaging;

import org.fuserleer.network.peers.ConnectedPeer;

public interface MessageProcessor<T extends Message>
{
	public void process (T message, ConnectedPeer peer);
}
