package org.fuserleer.database;

import java.io.IOException;

@SuppressWarnings("serial")
public class DatabaseException extends IOException
{
	public DatabaseException() 
	{ 
		super (); 
	}

	public DatabaseException(String message, Throwable throwable) 
	{ 
		super(message, throwable); 
	}

	public DatabaseException(String message) 
	{ 
		super(message); 
	}

	public DatabaseException(Throwable throwable) 
	{ 
		super (throwable); 
	}
}
