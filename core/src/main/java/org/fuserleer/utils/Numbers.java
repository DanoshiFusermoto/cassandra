package org.fuserleer.utils;

public class Numbers
{
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

	public static void greaterThan(long value, long bound, String message)
	{
		if (value <= bound)
		{
			if (message == null)
				throw new IllegalArgumentException();
			else
				throw new IllegalArgumentException(message);
		}
	}

}
