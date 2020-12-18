package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public final class StatePool implements Service
{
	private static final Logger cerbyLog = Logging.getLogger("cerby");

	private final Context context;
	
	StatePool(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
	}
	
	@Override
	public void start() throws StartupException
	{
	}

	@Override
	public void stop() throws TerminationException
	{
	}

}
