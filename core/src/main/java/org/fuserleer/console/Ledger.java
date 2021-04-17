package org.fuserleer.console;

import java.io.PrintStream;
import java.util.Collection;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.crypto.Hash;
import org.fuserleer.ledger.Block;
import org.fuserleer.time.Time;

public class Ledger extends Function
{
	private final static Options options = new Options().addOption("remote", false, "Return remote ledger information").addOption("pending", false, "Return pending ledger information")
																													   .addOption("states", false, "Return hash list of all pending states")
																													   .addOption("block", true, "Return block at specified height")
																													   .addOption("branches", false, "Return pending branches");

	public Ledger()
	{
		super("ledger", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);

		if (commandLine.hasOption("pending") == true)
		{
			if (commandLine.hasOption("branches") == true)
			{
				context.getLedger().getBlockHandler().getPendingBranches().forEach(pb -> printStream.println(pb.toString()));
			}
			else if (commandLine.hasOption("states") == true)
			{
				Collection<Hash> pendingStates = context.getLedger().getStatePool().pending();
				pendingStates.forEach(ps -> printStream.println(ps.toString()));
				printStream.println(pendingStates.size()+" pending states");
			}
		}
		else if (commandLine.hasOption("block") == true)
		{
			Block block = context.getLedger().get(Long.parseLong(commandLine.getOptionValue("block")));
			printStream.println("Block: "+block.getHeader().toString());
		}
		else
		{
			printStream.println("Synced: "+context.getLedger().isSynced());
			printStream.println("Identity: "+context.getNode().getIdentity());
			printStream.println("Current head: "+context.getLedger().getHead());
			printStream.println("Ledger timestamp: "+Time.getLedgerTimeSeconds()+" / "+new Date(Time.getLedgerTimeMS())); // TODO only accurate for simulated time
			printStream.println("Atoms (P/L/T): "+context.getLedger().getAtomHandler().numPending()+"/"+context.getMetaData().get("ledger.processed.atoms.local", 0l)+"/"+context.getMetaData().get("ledger.processed.atoms.total", 0l));
			printStream.println("Certificates (A/R/T): "+context.getMetaData().get("ledger.commits.certificates.accept", 0l)+"/"+context.getMetaData().get("ledger.commits.certificates.reject", 0l)+"/"+context.getMetaData().get("ledger.commits.certificates", 0l));
			printStream.println("Accumulation (I/A/T): "+context.getMetaData().get("ledger.accumulator.iterations", 0l)+"/"+(context.getMetaData().get("ledger.accumulator.duration", 0l) / Math.max(1, context.getMetaData().get("ledger.accumulator.iterations", 0l)))+"/"+context.getMetaData().get("ledger.accumulator.duration", 0l));
			printStream.println("Block size avg: "+(context.getMetaData().get("ledger.blocks.bytes", 0l)/(context.getLedger().getHead().getHeight()+1)));
			printStream.println("Block throughput: "+context.getMetaData().get("ledger.throughput.blocks", 0l));
			printStream.println("Atom throughput: "+context.getMetaData().get("ledger.throughput.atoms.local", 0l)+"/"+context.getMetaData().get("ledger.throughput.atoms.total", 0l));
			printStream.println("Commit latency: "+context.getMetaData().get("ledger.throughput.latency", 0l));
			printStream.println("Atom pool (S/A/R/C/Q): "+context.getLedger().getAtomPool().size()+" / "+context.getMetaData().get("ledger.pool.atoms.added", 0l)+" / "+context.getMetaData().get("ledger.pool.atoms.removed", 0l)+" / "+context.getMetaData().get("ledger.pool.atoms.agreed", 0l)+" / "+context.getMetaData().get("ledger.pool.atom.certificates", 0l));
			printStream.println("State pool (S/A/R/V/C): "+context.getLedger().getStatePool().size()+" / "+context.getMetaData().get("ledger.pool.state.added", 0l)+" / "+context.getMetaData().get("ledger.pool.state.removed", 0l)+" / "+context.getMetaData().get("ledger.pool.state.votes", 0l)+" / "+context.getMetaData().get("ledger.pool.state.certificates", 0l));
			printStream.println("Block pool: "+context.getLedger().getBlockHandler().size()+" / "+context.getMetaData().get("ledger.pool.blocks.added", 0l)+" / "+context.getMetaData().get("ledger.pool.blocks.removed", 0l));
			printStream.println("Shard (G/A): "+context.getLedger().numShardGroups()+"/"+context.getMetaData().get("ledger.throughput.shards.touched", 0l));
			printStream.println("Gossip Req (A/AV/SV/SC/BH/BV): "+context.getMetaData().get("gossip.requests.atom", 0l)+"/"+context.getMetaData().get("gossip.requests.atomvote", 0l)+"/"+
															  	  context.getMetaData().get("gossip.requests.statevote", 0l)+"/"+context.getMetaData().get("gossip.requests.statecertificate", 0l)+"/"+
															  	  context.getMetaData().get("gossip.requests.blockheader", 0l)+"/"+context.getMetaData().get("gossip.requests.blockvote", 0l)+"/"+context.getMetaData().get("gossip.requests.total", 0l));
		}
	}
}