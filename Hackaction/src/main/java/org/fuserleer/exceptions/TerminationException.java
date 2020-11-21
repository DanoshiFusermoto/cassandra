package org.fuserleer.exceptions;

public class TerminationException extends Exception 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7203560549694305527L;
	
	private final Class<?> clazz;
	
	public TerminationException(String message, Throwable throwable)
	{
		super (message, throwable);
		
		this.clazz = null;
	}

	public TerminationException(String message)
	{
		super(message);
		
		this.clazz = null;
	}

	public TerminationException(Throwable arg0)
	{
		super (arg0);
		
		this.clazz = null;
	}

	public TerminationException(Class<?> clazz)
	{
		super ();
		
		this.clazz = clazz;
	}

	public TerminationException(String message, Throwable throwable, Class<?> clazz)
	{
		super (message, throwable);
		
		this.clazz = clazz;
	}

	public TerminationException(String message, Class<?> clazz)
	{
		super (message);
		
		this.clazz = clazz;
	}

	public TerminationException(Throwable throwable, Class<?> clazz)
	{
		super (throwable);
		
		this.clazz = clazz;
	}
	
	public Class<?> getClazz() 
	{ 
		return this.clazz; 
	}
}