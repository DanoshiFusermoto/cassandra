package org.fuserleer.console;

import java.io.PrintStream;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.Universe;
import org.fuserleer.apps.SimpleWallet;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Identity;
import org.fuserleer.ledger.SearchResult;
import org.fuserleer.ledger.StateAddress;
import org.fuserleer.ledger.StateSearchQuery;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.atoms.TokenParticle.Action;
import org.fuserleer.ledger.atoms.TokenSpecification;
import org.fuserleer.ledger.atoms.TokenParticle;
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
			String ISO = commandLine.getOptionValue("balance", "FLEX");

			TokenSpecification token = null;
			for (Atom atom : Universe.getDefault().getGenesis().getAtoms())
				for(Particle particle : atom.getParticles())
					if (particle instanceof TokenSpecification && ((TokenSpecification)particle).getISO().equalsIgnoreCase(ISO) == true)
						token = (TokenSpecification) particle;
			
			if (token == null)
			{
				Future<SearchResult> tokenSearchFuture = context.getLedger().get(new StateSearchQuery(new StateAddress(TokenSpecification.class, Hash.from(ISO.toLowerCase())), TokenSpecification.class));
				SearchResult tokenSearchResult = tokenSearchFuture.get(10, TimeUnit.SECONDS);
				if (tokenSearchResult == null || tokenSearchResult.getPrimitive() == null)
				{
					printStream.println("Token "+ISO+" not found");
					return;
				}
				
				token = tokenSearchResult.getPrimitive(); 
			}
			
			printStream.println(wallet.getBalance(token.getHash())+" "+token.getISO());
		}
		else if (commandLine.hasOption("send") == true)
		{
			String[] options = commandLine.getOptionValues("send");
			Identity receiver = Identity.from(options[0]);
			UInt256 amount = UInt256.from(options[1]);
			String ISO = options[2];

			TokenSpecification token = null;
			for (Atom atom : Universe.getDefault().getGenesis().getAtoms())
				for(Particle particle : atom.getParticles())
					if (particle instanceof TokenSpecification && ((TokenSpecification)particle).getISO().equalsIgnoreCase(ISO) == true)
						token = (TokenSpecification) particle;
			
			if (token == null)
			{
				Future<SearchResult> tokenSearchFuture = context.getLedger().get(new StateSearchQuery(new StateAddress(TokenSpecification.class, Hash.from(ISO.toLowerCase())), TokenSpecification.class));
				SearchResult tokenSearchResult = tokenSearchFuture.get(10, TimeUnit.SECONDS);
				if (tokenSearchResult == null || tokenSearchResult.getPrimitive() == null)
				{
					printStream.println("Token "+ISO+" not found");
					return;
				}
				
				token = tokenSearchResult.getPrimitive(); 
			}
			
			Atom atom = wallet.spend(token, amount, receiver.getIdentity());
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
		else if (commandLine.hasOption("mint") == true)
		{
			String[] options = commandLine.getOptionValues("mint");
			UInt256 amount = UInt256.from(options[0]);
			String ISO = options[1];
			
			Future<SearchResult> tokenSearchFuture = context.getLedger().get(new StateSearchQuery(new StateAddress(TokenSpecification.class, Hash.from(ISO.toLowerCase())), TokenSpecification.class));
			SearchResult tokenSearchResult = tokenSearchFuture.get(10, TimeUnit.SECONDS);
			if (tokenSearchResult == null || tokenSearchResult.getPrimitive() == null)
			{
				printStream.println("Token "+ISO+" not found");
				return;
			}

			if (tokenSearchResult != null && ((TokenSpecification)tokenSearchResult.getPrimitive()).getOwner().equals(wallet.getIdentity()) == false)
			{
				printStream.println("Can not mint token "+ISO+" as not owned by "+wallet.getIdentity());
				return;
			}

			TokenParticle mint = new TokenParticle(amount, ((TokenSpecification)tokenSearchResult.getPrimitive()).getHash(), Action.MINT, Spin.UP, wallet.getIdentity());
			wallet.sign(mint);
			TokenParticle mintTransfer = new TokenParticle(amount, ((TokenSpecification)tokenSearchResult.getPrimitive()).getHash(), Action.TRANSFER, Spin.UP, wallet.getIdentity());
			wallet.sign(mintTransfer);
			
			Atom atom = new Atom(mint, mintTransfer);
			wallet.submit(atom);
			printStream.println(Serialization.getInstance().toJson(atom, Output.API));
		}
		else if (commandLine.hasOption("debits") == true)
		{
			String ISO = commandLine.getOptionValue("debits", "FLEX");
			
			TokenSpecification token = null;
			for (Atom atom : Universe.getDefault().getGenesis().getAtoms())
				for(Particle particle : atom.getParticles())
					if (particle instanceof TokenSpecification && ((TokenSpecification)particle).getISO().equalsIgnoreCase(ISO) == true)
						token = (TokenSpecification) particle;
			
			if (token == null)
			{
				Future<SearchResult> tokenSearchFuture = context.getLedger().get(new StateSearchQuery(new StateAddress(TokenSpecification.class, Hash.from(ISO.toLowerCase())), TokenSpecification.class));
				SearchResult tokenSearchResult = tokenSearchFuture.get(10, TimeUnit.SECONDS);
				if (tokenSearchResult == null || tokenSearchResult.getPrimitive() == null)
				{
					printStream.println("Token "+ISO+" not found");
					return;
				}
				
				token = tokenSearchResult.getPrimitive(); 
			}
			
			Collection<TokenParticle> transfers = wallet.get(TokenParticle.class, Spin.DOWN);
			for (TokenParticle transfer : transfers)
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
					if (particle instanceof TokenSpecification && ((TokenSpecification)particle).getISO().equalsIgnoreCase(ISO) == true)
						token = (TokenSpecification) particle;
			
			if (token == null)
			{
				Future<SearchResult> tokenSearchFuture = context.getLedger().get(new StateSearchQuery(new StateAddress(TokenSpecification.class, Hash.from(ISO.toLowerCase())), TokenSpecification.class));
				SearchResult tokenSearchResult = tokenSearchFuture.get(10, TimeUnit.SECONDS);
				if (tokenSearchResult == null || tokenSearchResult.getPrimitive() == null)
				{
					printStream.println("Token "+ISO+" not found");
					return;
				}
				
				token = tokenSearchResult.getPrimitive(); 
			}
			
			Collection<TokenParticle> transfers = wallet.get(TokenParticle.class, Spin.UP);
			for (TokenParticle transfer : transfers)
			{
				if (transfer.getToken().equals(token.getHash()) == false)
					continue;
				
				printStream.println(transfer.getHash()+" "+transfer.getQuantity()+" "+token.getISO()+" "+transfer.getAction()+" "+transfer.getSpin());
			}
		}
	}
}