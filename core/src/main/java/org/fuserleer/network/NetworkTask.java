package org.fuserleer.network;

import java.util.concurrent.TimeUnit;

import org.fuserleer.executors.ScheduledExecutable;

public abstract class NetworkTask extends ScheduledExecutable
{
	public NetworkTask(final long delay, final TimeUnit unit)
	{
		super(delay, 0, unit);
	}
}
