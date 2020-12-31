package org.fuserleer.console;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.tools.Spamathon;
import org.fuserleer.tools.Spamathon.Spammer;

public class Spam extends Function
{
	private static final Logger ledgerLog = Logging.getLogger("ledger");
	private static final Logger spammerLog = Logging.getLogger("spammer");

	private final static Options options = new Options().addRequiredOption("i", "iterations", true, "Quantity of spam iterations").
														 addRequiredOption("r", "rate", true, "Rate at which to produce events").
														 addOption("u", "uniques", true, "Unique elements per atom").
														 addOption("n", "nodes", true, "Connected nodes count to instruct to spam").
														 addOption("profile", false, "Profile the spam and output stats");

	public Spam()
	{
		super("spam", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);
		
		if (Spamathon.getInstance().isSpamming() == true)
			throw new IllegalStateException("Already an instance of spammer running");
		
		printStream.println("Starting spam of "+commandLine.getOptionValue("iterations")+" iterations at rate of "+commandLine.getOptionValue("rate")+" ... ");
		
		if (commandLine.hasOption("nodes") == true)
		{
			List<ConnectedPeer> connected = context.getNetwork().get(PeerState.CONNECTED);
			Collections.shuffle(connected);
			
			for (int p = 0 ; p < Integer.parseInt(commandLine.getOptionValue("nodes")) ; p++)
			{
				printStream.println((p+1)+"/"+Integer.parseInt(commandLine.getOptionValue("nodes"))+" sending spam request to "+connected.get(p));
				
				connected.get(p).send(new Spamathon.InitiateSpamMessage(Integer.parseInt(commandLine.getOptionValue("iterations")), 
													  					Integer.parseInt(commandLine.getOptionValue("rate")),
													  					Integer.parseInt(commandLine.getOptionValue("uniques", "1"))));
			}
		}
		
		Spammer spammer = Spamathon.getInstance().spam(Integer.parseInt(commandLine.getOptionValue("iterations")), 
													   Integer.parseInt(commandLine.getOptionValue("rate")),
									  				   Integer.parseInt(commandLine.getOptionValue("uniques", "1")));
		if (commandLine.hasOption("profile") == true)
		{
			long start = System.currentTimeMillis();
			long totalAtomsCommittedCount = 0;
			long lastCommittedBlockHeight = context.getLedger().getHead().getHeight();
			while(spammer.getNumProcessed() != spammer.getNumIterations())
			{
				Thread.sleep(100);
				
				if (context.getLedger().getHead().getHeight() > lastCommittedBlockHeight)
				{
					long duration = System.currentTimeMillis()-start;
					long committedAtoms = context.getLedger().getHead().getInventory(InventoryType.ATOMS).size();
					totalAtomsCommittedCount += committedAtoms;
					printStream.println("Atoms: "+totalAtomsCommittedCount+" TPS: "+(totalAtomsCommittedCount / TimeUnit.MILLISECONDS.toSeconds(duration)));
					lastCommittedBlockHeight = context.getLedger().getHead().getHeight();
				}
			}
			
			printStream.println("Completed local spam, waiting for network...");
			
			while(Spamathon.getInstance().isSpamming() == true)
			{
				Thread.sleep(1000);
			
				if (context.getLedger().getHead().getHeight() > lastCommittedBlockHeight)
				{
					long duration = System.currentTimeMillis()-start;
					long committedAtoms = context.getLedger().getHead().getInventory(InventoryType.ATOMS).size();
					totalAtomsCommittedCount += committedAtoms;
					printStream.println("Atoms: "+totalAtomsCommittedCount+" TPS: "+(totalAtomsCommittedCount / TimeUnit.MILLISECONDS.toSeconds(duration)));
					lastCommittedBlockHeight = context.getLedger().getHead().getHeight();
				}
			}

			long duration = System.currentTimeMillis()-start;
			printStream.println("Spam took "+duration+" to process "+totalAtomsCommittedCount+" atoms @ average rate of "+(totalAtomsCommittedCount / TimeUnit.MILLISECONDS.toSeconds(duration))+" TPS");
		}
	}
}

