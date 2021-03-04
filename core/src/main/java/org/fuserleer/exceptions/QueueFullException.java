package org.fuserleer.exceptions;

import java.io.IOException;

@SuppressWarnings("serial")
public class QueueFullException extends IOException
{
	public QueueFullException(final String message)
	{
		super(message);
	}

	public QueueFullException(final Throwable throwable)
	{
		super(throwable);
	}

	public QueueFullException(final String message, final Throwable throwable)
	{
		super(message, throwable);
	}
}
