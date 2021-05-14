package org.fuserleer.console;

import java.io.PrintStream;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.Hash;
import org.fuserleer.ledger.PendingAtom;
import org.fuserleer.ledger.SearchResult;
import org.fuserleer.ledger.StateAddress;
import org.fuserleer.ledger.StateSearchQuery;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.atoms.UniqueParticle;
import org.fuserleer.serialization.Serialization;
import org.json.JSONObject;
import org.fuserleer.serialization.DsonOutput.Output;

public class Atoms extends Function
{
	private final static Options options = new Options().addOption("submit", true, "Submit atom/atoms")
														.addOption("pending", false, "Returns the hashes of all atoms pending")
														.addOption("pool", false, "Returns the hashes of all atoms in the pool")
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
			AtomCertificate certificate = null;
			if (atom == null)
				printStream.println("Atom "+commandLine.getOptionValue("get")+" not found");
			else
			{
				Future<SearchResult> certificateFuture = context.getLedger().get(new StateSearchQuery(new StateAddress(Atom.class, atom.getHash()), AtomCertificate.class));
				SearchResult searchResult = certificateFuture.get();
				if (searchResult != null) 
					certificate = searchResult.getPrimitive();

				JSONObject atomJSONObject = Serialization.getInstance().toJsonObject(atom, Output.PERSIST);
				if (certificate != null)
					atomJSONObject.put("certificate", Serialization.getInstance().toJsonObject(certificate, Output.PERSIST));
				
				printStream.println(atomJSONObject.toString(4));
			}			
		}
		else if (commandLine.hasOption("pending") == true)
		{
			for (PendingAtom pendingAtom : context.getLedger().getAtomHandler().getAll())
				printStream.println(pendingAtom.getHash());
		}
		else if (commandLine.hasOption("pool") == true)
		{
			context.getLedger().getAtomPool().getAll().forEach(pa -> printStream.println(pa.getHash()));		
		}
		else if (commandLine.hasOption("submit") == true)
		{
			for (int i = 0 ; i < Integer.parseInt(commandLine.getOptionValue("submit", "1")) ; i++)
			{
				Hash randomValue = Hash.random();
				ECKeyPair key = new ECKeyPair();
				UniqueParticle particle = new UniqueParticle(randomValue, key.getIdentity());
				particle.sign(key);

				Atom atom = new Atom(particle);
				context.getLedger().submit(atom);

				JSONObject atomJSONObject = Serialization.getInstance().toJsonObject(atom, Output.PERSIST);
				printStream.println("Submitted "+atom.getHash());
				printStream.println(atomJSONObject.toString(4));
			}
		}
	}
}