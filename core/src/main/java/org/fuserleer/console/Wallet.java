package org.fuserleer.console;

import java.io.File;
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.apps.SimpleWallet;
import org.fuserleer.crypto.ECKeyPair;

public class Wallet extends Function
{
	private final static Options options = new Options().addOption(Option.builder("init").desc("Initialized a wallet").optionalArg(true).numberOfArgs(1).build())
														.addOption("close", false, "Closes the open wallet")
														.addOption("address", false, "Returns the wallet address");
	
	private static final Map<Context, SimpleWallet> wallets = Collections.synchronizedMap(new HashMap<Context, SimpleWallet>());
	public static SimpleWallet get(Context context)
	{
		return wallets.get(context);
	}
	
	private static SimpleWallet put(Context context, SimpleWallet wallet)
	{
		return wallets.put(context, wallet);
	}

	private static boolean remove(Context context, SimpleWallet wallet)
	{
		return wallets.remove(context, wallet);
	}

	public Wallet()
	{
		super("wallet", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);
		
		SimpleWallet wallet = Wallet.get(context);

		if (commandLine.hasOption("init") == true)
		{
			if (wallet != null)
				throw new IllegalStateException("Wallet "+wallet.getIdentity()+" is already open");
			
			String filename = commandLine.getOptionValue("init", "wallet.key");
			ECKeyPair walletKeyPair = ECKeyPair.fromFile(new File(filename), true);
			wallet = new SimpleWallet(context, walletKeyPair);
			Wallet.put(context, wallet);
		}
		else if (wallet == null)
			throw new IllegalStateException("No wallet is open");
		
		if (commandLine.hasOption("close") == true)
		{
			Wallet.remove(context, wallet);
			wallet.close();
		}
		else if (commandLine.hasOption("address") == true)
			printStream.println(wallet.getIdentity());
	}
}