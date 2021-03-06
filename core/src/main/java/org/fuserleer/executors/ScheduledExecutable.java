package org.fuserleer.executors;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.Numbers;

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
		
		Numbers.isNegative(initialDelay, "Initial delay is negative");
		Numbers.isNegative(recurrentDelay, "Recurrent delay is negative");

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

