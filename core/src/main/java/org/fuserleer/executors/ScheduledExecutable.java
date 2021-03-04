package org.fuserleer.executors;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public abstract class ScheduledExecutable extends Executable
{
	private static final Logger log = Logging.getLogger("executor");

	private final long initialDelay;
	private final long recurrentDelay;
	private final TimeUnit unit;

	public ScheduledExecutable(final long recurrentDelay, final TimeUnit unit)
	{
		this(0, recurrentDelay, unit);
	}
	
	public ScheduledExecutable(final long initialDelay, final long recurrentDelay, final TimeUnit unit)
	{
		super();
		
		if (initialDelay < 0)
			throw new IllegalArgumentException("Initial delay is negative");
		
		if (recurrentDelay < 0)
			throw new IllegalArgumentException("Recurrent delay is negative");

		this.initialDelay = initialDelay;
		this.recurrentDelay = recurrentDelay;
		this.unit = Objects.requireNonNull(unit, "Time unit for scheduled executable is null");
	}

	public long getInitialDelay() 
	{ 
		return this.initialDelay; 
	}

	public long getRecurrentDelay() 
	{ 
		return this.recurrentDelay; 
	}
	
	public TimeUnit getTimeUnit() 
	{ 
		return this.unit; 
	}
}

