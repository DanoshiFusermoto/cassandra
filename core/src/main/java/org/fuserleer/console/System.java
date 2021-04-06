package org.fuserleer.console;

import java.io.PrintStream;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.crypto.CryptoUtils;

public final class System extends Function
{
	private final static Options options = new Options();

	public System()
	{
		super("system", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
//		CommandLine commandLine = Function.parser.parse(options, arguments);
		printStream.println("Identity: "+context.getNode().getIdentity());
		printStream.println("CPU: "+Runtime.getRuntime().availableProcessors());
		printStream.println("Memory: "+Runtime.getRuntime().freeMemory()+"/"+Runtime.getRuntime().maxMemory()+"/"+Runtime.getRuntime().totalMemory());
		printStream.println("Hashes (S/D/T): "+CryptoUtils.singles.get()+"/"+CryptoUtils.doubles.get()+"/"+CryptoUtils.hashed.get());
		printStream.println("Signatures (ECC) (S/V): "+CryptoUtils.ECCSigned.get()+"/"+CryptoUtils.ECCVerified.get());
		printStream.println("Signatures (BLS) (S/V): "+CryptoUtils.BLSSigned.get()+"/"+CryptoUtils.BLSVerified.get());
	}
}