package org.fuserleer.network;

import org.fuserleer.common.Primitive;

public interface GossipReceiver
{
	public void receive(Primitive object) throws Throwable;
}
