package org.fuserleer.time;

import java.util.concurrent.TimeUnit;

import org.fuserleer.Configuration;

public class WallClockTime implements TimeProvider
{
	public WallClockTime(Configuration configuration)
	{
		
	}
	
	@Override
	public long getSystemTime()
	{
		return System.currentTimeMillis();
	}

	@Override
	public int getLedgerTimeSeconds()
	{
		return (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
	}

	@Override
	public long getLedgerTimeMS()
	{
		return System.currentTimeMillis();
	}

	@Override
	public boolean isSynchronized()
	{
		return false;
	}
}
