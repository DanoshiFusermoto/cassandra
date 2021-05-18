package org.fuserleer.console;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.fuserleer.Context;
import org.fuserleer.apps.SimpleWallet;
import org.fuserleer.crypto.ComputeKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Identity;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AutomataParticle;
import org.fuserleer.ledger.atoms.ExecuteAutomataParticle;
import org.fuserleer.ledger.atoms.PolyglotParticle;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.utils.Bytes;
import org.fuserleer.utils.UInt256;
import org.fuserleer.serialization.DsonOutput.Output;

public final class Automata extends Function
{
	private final static Options options = new Options().addOption(Option.builder("deploy").desc("Deploys an automata to the ledger").hasArgs().build())
														.addOption(Option.builder("call").desc("Calls an automata method").hasArgs().build());
	
	public Automata()
	{
		super("automata", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);
		
		SimpleWallet wallet = Wallet.get(context);
		if (wallet == null)
			throw new IllegalStateException("No wallet is open");
		
		if (commandLine.hasOption("deploy") == true)
		{
			String[] options = commandLine.getOptionValues("deploy");
			AutomataParticle automata;
			Class<?> automataClass = Serialization.getInstance().getClassForId(options[0].toLowerCase());
			if (automataClass == null)
				automataClass = PolyglotParticle.class;
			
			if (automataClass.equals(PolyglotParticle.class) == true)
			{
				// TODO
				throw new UnsupportedOperationException("Polyglot automata not yet supported for deployment");
			}
			else
			{
				// Attempt to find an appropriate constructor
				Constructor<?>[] constructors = automataClass.getConstructors();
				Constructor<?>	bestConstructor = null;
				for (int c = 0 ; c < constructors.length ; c++)
				{
					Constructor<?> constructor = constructors[c];
					
					// Would be options.length-1 but automata need owners which is inferred to be the wallet identity
					if (constructor.getParameterCount() != options.length)
						continue;
					
					if (bestConstructor == null)
						bestConstructor = constructor;
					else
						bestConstructor = null;
				}
				
				if (bestConstructor == null)
					throw new NoSuchElementException("Could not find an appropriate automata constructor for "+automataClass);
				
				//
				int optionIndex = 0;
				List<Object> parameters = new ArrayList<Object>();
				Class<?>[] parameterTypes = bestConstructor.getParameterTypes();
				for (int t = 0 ; t < parameterTypes.length ; t++)
				{
					if (parameterTypes[t].equals(Identity.class) == true)
						parameters.add(wallet.getIdentity());
					else
					{
						if (parameterTypes[t].equals(Hash.class) == true)
							parameters.add(new Hash(options[optionIndex]));
						else if (parameterTypes[t].equals(UInt256.class) == true)
							parameters.add(UInt256.from(options[optionIndex]));
						else if (parameterTypes[t].equals(String.class) == true)
							parameters.add(options[optionIndex]);
						else 
							throw new IllegalArgumentException("Argument type "+parameterTypes[t]+" not supported for deployment of automata "+automataClass);
						
						optionIndex++;
					}
				}
				
				automata = (AutomataParticle) bestConstructor.newInstance(parameters.toArray());
			}
			
			wallet.sign(automata);
			Atom atom = new Atom(automata);
			wallet.submit(atom);
			printStream.println(Serialization.getInstance().toJson(atom, Output.API));
		}
		else if(commandLine.hasOption("call") == true)
		{
			String[] options = commandLine.getOptionValues("call");
			Identity automataIdentity = ComputeKey.from(Bytes.fromHexString(options[0])).getIdentity();

			ExecuteAutomataParticle executeAutomataParticle = new ExecuteAutomataParticle(automataIdentity, options[1].toLowerCase(), options.length > 2 ? Arrays.copyOfRange(options, 2, options.length) : null);
			Atom atom = new Atom(executeAutomataParticle);
			wallet.submit(atom);
			printStream.println(Serialization.getInstance().toJson(atom, Output.API));
		}
	}
}