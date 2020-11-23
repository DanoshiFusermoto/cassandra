package org.fuserleer.network;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public final class Network implements Service
{
	private static final Logger networkLog = Logging.getLogger("network");

    private final Context		context;

    public Network(Context context)
    {
    	super();
    	
    	this.context = Objects.requireNonNull(context);
    }

	@Override
	public void start() throws StartupException
	{
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() throws TerminationException
	{
		// TODO Auto-generated method stub
		
	}
	
	public void clean() throws IOException
	{
	}
}
