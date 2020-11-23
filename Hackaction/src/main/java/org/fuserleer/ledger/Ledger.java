package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public final class Ledger implements Service
{
	private static final Logger ledgerLog = Logging.getLogger("ledger");

	private final Context context;
	
	private transient BlockHeader head;

	public Ledger(Context context)
	{
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
	
	public BlockHeader getHead()
	{
		return this.head;
	}

}
