package org.fuserleer.console;

import java.io.PrintStream;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.apps.SimpleWallet;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.MessageParticle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.time.Time;

public class Messages extends Function
{
	private final static Options options = new Options().addOption(Option.builder("send").desc("Sends a message to an address").numberOfArgs(1).build())
													    .addOption("sent", false, "Lists sent messages")
													    .addOption("inbox", false, "Lists received messages");
	
	public Messages()
	{
		super("messages", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);
		
		SimpleWallet wallet = Wallet.get(context);
		if (wallet == null)
			throw new IllegalStateException("No wallet is open");
		
		if (commandLine.hasOption("send") == true)
		{
			ECPublicKey receiver = ECPublicKey.from(commandLine.getOptionValue("send"));
			String message = commandLine.getArgList().stream().collect(Collectors.joining(" "));
			
			MessageParticle messageParticle = new MessageParticle(message, wallet.getIdentity(), receiver, Time.getSystemTime());
			wallet.sign(messageParticle);
			
			Atom atom = new Atom(messageParticle);
			wallet.submit(atom);
			printStream.println(Serialization.getInstance().toJson(atom, Output.API));
		}
		else if (commandLine.hasOption("sent") == true)
		{
			Collection<MessageParticle> messages = wallet.get(MessageParticle.class, Spin.UP);
			for (MessageParticle message : messages)
			{
				if (message.getSender().equals(wallet.getIdentity()) == false)
					continue;
				
				printStream.println(message.getHash()+" "+Date.from(Instant.ofEpochMilli(message.getCreatedAt()))+" "+message.getRecipient()+" "+message.getMessage());
			}
		}
		else if (commandLine.hasOption("inbox") == true)
		{
			Collection<MessageParticle> messages = wallet.get(MessageParticle.class, Spin.UP);
			for (MessageParticle message : messages)
			{
				if (message.getRecipient().equals(wallet.getIdentity()) == false)
					continue;
				
				printStream.println(message.getHash()+" "+Date.from(Instant.ofEpochMilli(message.getCreatedAt()))+" "+message.getSender()+" "+message.getMessage());
			}
		}
	}
}