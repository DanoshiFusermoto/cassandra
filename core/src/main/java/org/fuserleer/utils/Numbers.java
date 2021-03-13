package org.fuserleer.utils;

public class Numbers
{
	public static void inRange(long value, long low, long high, String message)
	{
		long rl = low, rh = high;
		if (low > high)
		{
			rl = high;
			rh = low;
		}
			
		
		if (value < rl || value > rh)
		{
			if (message == null)
				throw new IllegalArgumentException();
			else
				throw new IllegalArgumentException(message);
		}
	}

	public static void notNegative(long value, String message)
	{
		if (value < 0)
		{
			if (message == null)
				throw new IllegalArgumentException();
			else
				throw new IllegalArgumentException(message);
		}
	}
	
	public static void notPositive(long value, String message)
	{
		if (value > 0)
		{
			if (message == null)
				throw new IllegalArgumentException();
			else
				throw new IllegalArgumentException(message);
		}
	}

	public static void notZero(long value, String message)
	{
		if (value == 0)
		{
			if (message == null)
				throw new IllegalArgumentException();
			else
				throw new IllegalArgumentException(message);
		}
	}

	public static void lessThan(long value, long bound, String message)
	{
		if (value < bound)
		{
			if (message == null)
				throw new IllegalArgumentException();
			else
				throw new IllegalArgumentException(message);
		}
	}

	public static void greaterThan(long value, long bound, String message)
	{
		if (value > bound)
		{
			if (message == null)
				throw new IllegalArgumentException();
			else
				throw new IllegalArgumentException(message);
		}
	}
}
