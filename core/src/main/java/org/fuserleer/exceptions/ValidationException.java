package org.fuserleer.exceptions;

@SuppressWarnings("serial")
public class ValidationException extends Exception
{
	public ValidationException(final Throwable cause)
	{
		super(cause);
	}

	public ValidationException(final String message, final Throwable cause)
	{
		super(message, cause);
	}

	public ValidationException(final String message)
	{
		super(message);
	}
}
