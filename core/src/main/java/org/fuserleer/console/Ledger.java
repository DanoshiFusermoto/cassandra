package org.fuserleer.console;

import java.io.PrintStream;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.time.Time;

public class Ledger extends Function
{
	private final static Options options = new Options().addOption("remote", false, "Return remote ledger information");

	public Ledger()
	{
		super("ledger", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);

		{
			printStream.println("Current head: "+context.getLedger().getHead());
			printStream.println("Ledger timestamp: "+Time.getLedgerTimeSeconds()+" / "+new Date(Time.getLedgerTimeMS())); // TODO only accurate for simulated time
			printStream.println("Commit gets: "+context.getMetaData().get("ddb.commit.gets", 0l));
			printStream.println("Accumulation (I/A/T): "+context.getMetaData().get("ledger.accumulator.iterations", 0l)+"/"+(context.getMetaData().get("ledger.accumulator.duration", 0l) / Math.max(1, context.getMetaData().get("ledger.accumulator.iterations", 0l)))+"/"+context.getMetaData().get("ledger.accumulator.duration", 0l));
			printStream.println("Block throughput: "+context.getMetaData().get("ledger.throughput.blocks", 0l));
			printStream.println("Atom throughput: "+context.getMetaData().get("ledger.throughput.atoms", 0l));
			printStream.println("Particle throughput: "+context.getMetaData().get("ledger.throughput.particles", 0l));
			printStream.println("Commit latency: "+context.getMetaData().get("ledger.throughput.latency", 0l));
		}
	}
}