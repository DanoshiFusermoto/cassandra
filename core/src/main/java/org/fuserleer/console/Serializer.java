package org.fuserleer.console;

import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.serialization.Serialization;

public class Serializer extends Function
{
	private final static Options options = new Options().addOption("stats", false, "Outputs serializerstatistics");


	public Serializer()
	{
		super("serializer", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);

		if (commandLine.hasOption("stats") == true)
		{
			for (Entry<Class<?>, Entry<Long, Long>> type: Serialization.getInstance().statistics())
				printStream.println(type.getKey()+": "+type.getValue().getKey()+" -> "+type.getValue().getValue()+" / "+(type.getValue().getValue() / type.getValue().getKey()));
		}
	}
}