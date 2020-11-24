package org.fuserleer.console;

import java.io.PrintStream;

import org.apache.commons.cli.Options;
import org.fuserleer.Context;

public class Echo extends Function
{
	Echo()
	{
		super("echo", new Options());
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream)
	{
		String echo = String.join(" ", arguments);
		printStream.println(echo);
	}

}
