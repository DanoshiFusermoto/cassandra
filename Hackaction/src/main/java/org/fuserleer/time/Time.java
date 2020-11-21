package org.fuserleer.time;

import java.io.IOException;

public final class Time
{
	public static final int MAXIMUM_DRIFT = 30;
	
	private static Time instance = null;
	
	public static Time getDefault()
	{
		if (instance == null)
			throw new RuntimeException("Time instance not set");
		
		return instance;
	}

	public static Time createAsDefault(TimeProvider provider) throws IOException
	{
		if (instance != null)
			throw new RuntimeException("Default time provider already set");
		
		instance = new Time(provider);
		
		return instance;
	}
	
	public static Time clearDefault()
	{
		Time time = getDefault();
		instance = null;
		return time;
	}

	public static long getLedgerTimeMS() 
	{
		return getDefault().getProvider().getLedgerTimeMS();
	}
	
	public static long getLedgerTimeSeconds() 
	{
		return getDefault().getProvider().getLedgerTimeSeconds();
	}

	public static long getSystemTime() 
	{
		return getDefault().getProvider().getSystemTime();
	}

	private final TimeProvider provider;
	
	private Time(TimeProvider provider)
	{
		this.provider = provider;
	}
	
	private TimeProvider getProvider()
	{
		return this.provider;
	}
}
