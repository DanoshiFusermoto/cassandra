package org.fuserleer.network;

import java.util.concurrent.TimeUnit;

import org.fuserleer.executors.ScheduledExecutable;

public abstract class NetworkTask extends ScheduledExecutable
{
	public NetworkTask(long delay, TimeUnit unit)
	{
		super(delay, 0, unit);
	}
}
