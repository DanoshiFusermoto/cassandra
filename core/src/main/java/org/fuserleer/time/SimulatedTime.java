package org.fuserleer.time;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.fuserleer.Configuration;
import org.fuserleer.Context;
import org.fuserleer.Universe;

public class SimulatedTime implements TimeProvider
{
	private long initialTime;
	private long millisPerCommit;
	private long highestCommit = 0;
	
	public SimulatedTime(final Configuration configuration)
	{
		this.initialTime = Objects.requireNonNull(configuration, "Configuration is null").get("time.simulated.inital", Universe.getDefault().getTimestamp());
		this.millisPerCommit = Objects.requireNonNull(configuration, "Configuration is null").get("time.simulated.increment", 10);
	}

	@Override
	public long getSystemTime()
	{
		return System.currentTimeMillis();
	}

	@Override
	public int getLedgerTimeSeconds()
	{
		this.highestCommit = Math.max(Context.get().getLedger().getHead().getHeight(), this.highestCommit);
		return (int) TimeUnit.MILLISECONDS.toSeconds(this.initialTime + (this.highestCommit * millisPerCommit));
	}

	@Override
	public long getLedgerTimeMS()
	{
		this.highestCommit = Math.max(Context.get().getLedger().getHead().getHeight(), this.highestCommit);
		return this.initialTime + (this.highestCommit * millisPerCommit);
	}

	@Override
	public boolean isSynchronized()
	{
		return false;
	}
}
