package org.fuserleer.console;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Date;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.crypto.BLSPublicKey;

public class Validators extends Function
{
	private final static Options options = new Options().addOption("remote", false, "Return remote ledger information").addOption("known", false, "All known validator identities")
																													   .addOption("powers", false, "Validators and their powers");
	public Validators()
	{
		super("validators", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);

		if (commandLine.hasOption("known") == true)
		{
			Collection<BLSPublicKey> identities = context.getLedger().getValidatorHandler().getIdentities();
			identities.forEach(id -> printStream.println(id.asHash()));
			printStream.println(identities.size()+" validator identities");
		}
		else if (commandLine.hasOption("powers") == true)
		{
			Collection<Entry<BLSPublicKey, Long>> powers = context.getLedger().getValidatorHandler().getVotePowers();
			powers.forEach(pw -> printStream.println(pw.getKey().asHash()+" = "+pw.getValue()));
			printStream.println(powers.size()+" validators with power");
		}
	}
}