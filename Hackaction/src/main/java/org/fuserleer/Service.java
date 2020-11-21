package org.fuserleer;

import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;

public interface Service
{
	public void start() throws StartupException;
	public void stop() throws TerminationException;
	default public String getName()
	{
		return this.getClass().getSimpleName();
	}
}
