package org.fuserleer.exceptions;

public class StartupException extends Exception 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -3096794654963649858L;
	
	private final Class<?> clazz;
	
	public StartupException(String message, Throwable throwable)
	{
		super (message, throwable);
		
		this.clazz = null;
	}

	public StartupException(String message)
	{
		super(message);
		
		this.clazz = null;
	}

	public StartupException (Throwable arg0)
	{
		super (arg0);
		
		this.clazz = null;
	}

	public StartupException(Class<?> clazz)
	{
		super ();
		
		this.clazz = clazz;
	}

	public StartupException(String message, Throwable throwable, Class<?> clazz)
	{
		super (message, throwable);
		
		this.clazz = clazz;
	}

	public StartupException(String message, Class<?> clazz)
	{
		super (message);
		
		this.clazz = clazz;
	}

	public StartupException(Throwable throwable, Class<?> clazz)
	{
		super (throwable);
		
		this.clazz = clazz;
	}
	
	public Class<?> getClazz() 
	{ 
		return this.clazz; 
	}
}