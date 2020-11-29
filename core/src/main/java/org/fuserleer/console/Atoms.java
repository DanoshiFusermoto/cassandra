package org.fuserleer.console;

import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.fuserleer.API;
import org.fuserleer.Context;
import org.fuserleer.crypto.Hash;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.UniqueParticle;
import org.fuserleer.serialization.Serialization;
import org.json.JSONObject;
import org.fuserleer.serialization.DsonOutput.Output;

public class Atoms extends Function
{
	private final static Options options = new Options().addOption("submit", true, "Submit atom/atoms")
														.addOption("get", true, "Get an atom by hash");

	public Atoms()
	{
		super("atom", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);

		if (commandLine.hasOption("get") == true)
		{
			Atom atom = context.getLedger().get(new Hash(commandLine.getOptionValue("get")), Atom.class);
			if (atom == null)
				printStream.println("Atom "+commandLine.getOptionValue("get")+" not found");
			else
			{
				JSONObject atomJSONObject = Serialization.getInstance().toJsonObject(atom, Output.PERSIST);
				printStream.println(atomJSONObject.toString(4));
			}			
		}
		else if (commandLine.hasOption("submit") == true)
		{
			for (int i = 0 ; i < Integer.parseInt(commandLine.getOptionValue("submit", "1")) ; i++)
			{
				Hash randomValue = Hash.random();
				UniqueParticle particle = new UniqueParticle(randomValue, context.getNode().getIdentity());
				Atom atom = new Atom(particle);
				context.getLedger().submit(atom);
	
				JSONObject atomJSONObject = Serialization.getInstance().toJsonObject(atom, Output.PERSIST);
				printStream.println("Submitted "+atom.getHash());
				printStream.println(atomJSONObject.toString(4));
			}
		}
	}
}