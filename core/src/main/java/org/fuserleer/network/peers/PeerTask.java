package org.fuserleer.network.peers;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.fuserleer.executors.ScheduledExecutable;

public abstract class PeerTask extends ScheduledExecutable
{
	private final ConnectedPeer peer;

	public PeerTask(final ConnectedPeer peer, final long delay, final TimeUnit unit)
	{
		super(delay, 0, unit);

		this.peer = Objects.requireNonNull(peer, "PeerTask peer is null");
	}

	public ConnectedPeer getPeer()
	{
		return this.peer;
	}
}
