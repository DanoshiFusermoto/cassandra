package org.fuserleer.database;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public abstract class DatabaseStore
{
	protected static final Logger log = Logging.getLogger ();

	private final DatabaseEnvironment environment;
	
	public DatabaseStore(DatabaseEnvironment environment)
	{
		super();
		
		this.environment = Objects.requireNonNull(environment);

		if (environment.isRegistered(this) == false)
			environment.register(this);
	}

	public void close() throws IOException
	{
		flush();

		if (this.environment.isRegistered(this) == true)
			this.environment.deregister(this);
	}
	
	protected DatabaseEnvironment getEnvironment()
	{
		return this.environment;
	}

	public abstract void flush() throws IOException;
	
	public abstract void clean() throws IOException;
}
