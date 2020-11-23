package org.fuserleer.network;

@SuppressWarnings("serial")
public class BanException extends Exception 
{
	public BanException() 
	{
		super (); 
	}

	public BanException(String message, Throwable throwable) 
	{ 
		super (message, throwable); 
	}

	public BanException(String message) 
	{ 
		super (message); 
	}

	public BanException(Throwable throwable) 
	{ 
		super (throwable); 
	}
}
