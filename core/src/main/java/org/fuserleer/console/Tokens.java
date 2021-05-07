package org.fuserleer.console;

import java.io.PrintStream;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.Universe;
import org.fuserleer.apps.SimpleWallet;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.atoms.TokenSpecification;
import org.fuserleer.ledger.atoms.TransferParticle;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.UInt256;

public class Tokens extends Function
{
	private final static Options options = new Options().addOption(Option.builder("balance").desc("Returns balance for a token").optionalArg(true).numberOfArgs(1).build())
														.addOption(Option.builder("send").desc("Sends a quantity of tokens to an address").numberOfArgs(3).build())
														.addOption(Option.builder("mint").desc("Mints a quantity of tokens").numberOfArgs(2).build())
														.addOption("owned", false, "List owned tokens")
														.addOption(Option.builder("create").desc("Creates a new token definition").numberOfArgs(1).build())
														.addOption(Option.builder("debits").desc("List debits for specified token").optionalArg(true).numberOfArgs(1).build())
														.addOption(Option.builder("credits").desc("List credits for specified token").optionalArg(true).numberOfArgs(1).build());
	
	public Tokens()
	{
		super("tokens", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);
		
		SimpleWallet wallet = Wallet.get(context);
		if (wallet == null)
			throw new IllegalStateException("No wallet is open");
		
		if (commandLine.hasOption("balance") == true)
		{
			TokenSpecification flexToken = null;
			for (Atom atom : Universe.getDefault().getGenesis().getAtoms())
				for(Particle particle : atom.getParticles())
					if (particle instanceof TokenSpecification && ((TokenSpecification)particle).getISO().equalsIgnoreCase("FLEX") == true)
						flexToken = (TokenSpecification) particle;
			
			if (flexToken == null)
				throw new IllegalStateException("Token FLEX not found");

			printStream.println(wallet.getBalance(flexToken)+" "+flexToken.getISO());
		}
		else if (commandLine.hasOption("send") == true)
		{
			String[] options = commandLine.getOptionValues("send");
			ECPublicKey receiver = ECPublicKey.from(options[0]);
			UInt256 amount = UInt256.from(options[1]);
			
			TokenSpecification flexToken = null;
			for (Atom atom : Universe.getDefault().getGenesis().getAtoms())
				for(Particle particle : atom.getParticles())
					if (particle instanceof TokenSpecification && ((TokenSpecification)particle).getISO().equalsIgnoreCase("FLEX") == true)
						flexToken = (TokenSpecification) particle;
			
			if (flexToken == null)
				throw new IllegalStateException("Token FLEX not found");
			
			Atom atom = wallet.spend(flexToken, amount, receiver);
			wallet.submit(atom);
			printStream.println(Serialization.getInstance().toJson(atom, Output.API));
		}
		else if (commandLine.hasOption("owned") == true)
		{
			Collection<TokenSpecification> owned = wallet.get(TokenSpecification.class, Spin.UP);
			for (TokenSpecification token : owned)
				printStream.println(token.getISO()+" "+token.getHash()+" '"+token.getDescription()+"' "+token.getSpin());
		}
		else if (commandLine.hasOption("create") == true)
		{
			String ISO = commandLine.getOptionValue("create");
			String description = commandLine.getArgList().stream().collect(Collectors.joining(" "));
			
			TokenSpecification token = new TokenSpecification(ISO, description, wallet.getIdentity());
			wallet.sign(token);
			
			Atom atom = new Atom(token);
			wallet.submit(atom);
			printStream.println(Serialization.getInstance().toJson(atom, Output.API));
		}
		else if (commandLine.hasOption("debits") == true)
		{
			String ISO = commandLine.getOptionValue("debits", "FLEX");
			TokenSpecification token = null;
			for (Atom atom : Universe.getDefault().getGenesis().getAtoms())
				for(Particle particle : atom.getParticles())
					if (particle instanceof TokenSpecification && ((TokenSpecification)particle).getISO().equalsIgnoreCase("FLEX") == true)
						token = (TokenSpecification) particle;
			
			if (token == null)
				throw new IllegalStateException("Token "+ISO+" not found");
			
			Collection<TransferParticle> transfers = wallet.get(TransferParticle.class, Spin.DOWN);
			for (TransferParticle transfer : transfers)
			{
				if (transfer.getToken().equals(token.getHash()) == false)
					continue;
				
				printStream.println(transfer.getHash()+" "+transfer.getQuantity()+" "+token.getISO()+" "+transfer.getSpin());
			}
		}
		else if (commandLine.hasOption("credits") == true)
		{
			String ISO = commandLine.getOptionValue("credits", "FLEX");
			TokenSpecification token = null;
			for (Atom atom : Universe.getDefault().getGenesis().getAtoms())
				for(Particle particle : atom.getParticles())
					if (particle instanceof TokenSpecification && ((TokenSpecification)particle).getISO().equalsIgnoreCase("FLEX") == true)
						token = (TokenSpecification) particle;
			
			if (token == null)
				throw new IllegalStateException("Token "+ISO+" not found");
			
			Collection<TransferParticle> transfers = wallet.get(TransferParticle.class, Spin.UP);
			for (TransferParticle transfer : transfers)
			{
				if (transfer.getToken().equals(token.getHash()) == false)
					continue;
				
				printStream.println(transfer.getHash()+" "+transfer.getQuantity()+" "+token.getISO()+" "+transfer.getSpin());
			}
		}
	}
}