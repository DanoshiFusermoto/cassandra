package org.fuserleer.network.peers;

import java.util.concurrent.TimeUnit;

import org.fuserleer.executors.ScheduledExecutable;

public abstract class PeerTask extends ScheduledExecutable
{
	private final ConnectedPeer peer;

	public PeerTask(ConnectedPeer peer, long delay, TimeUnit unit)
	{
		super(delay, 0, unit);

		this.peer = peer;
	}

	public ConnectedPeer getPeer()
	{
		return this.peer;
	}
}
