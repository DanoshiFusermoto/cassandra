package org.fuserleer;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.fuserleer.Configuration;
import org.fuserleer.Context;
import org.fuserleer.console.Console;
import org.fuserleer.API;
import org.fuserleer.time.Time;
import org.fuserleer.time.TimeProvider;
import org.fuserleer.utils.Bytes;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public class Hackation
{
	private static final Logger log = Logging.getLogger();

	public static void main(String[] args)
	{
		try
		{
			Configuration.createAsDefault("/commandline_options.json", args);

			// Setup bouncy castle
			// This is used when loading the node key below, so set it up now.
			Security.addProvider(new BouncyCastleProvider());
			try {
				Field isRestricted = Class.forName("javax.crypto.JceSecurity").getDeclaredField("isRestricted");

				log.info("Encryption restrictions are set, need to override...");

				if (Modifier.isFinal(isRestricted.getModifiers()) == true) 
				{
					Field modifiers = Field.class.getDeclaredField("modifiers");
					modifiers.setAccessible(true);
					modifiers.setInt(isRestricted, isRestricted.getModifiers() & ~Modifier.FINAL);
				}
				
				isRestricted.setAccessible(true);
				isRestricted.setBoolean(null, false);
				isRestricted.setAccessible(false);
				log.info("...override success!");
			} catch (NoSuchFieldException nsfex) {
				log.error("No such field - isRestricted");
			}
			
			System.setProperty("user.dir", Configuration.getDefault().getCommandLine().getOptionValue("home", System.getProperty("user.dir")));
			System.setProperty("console", Boolean.toString(Configuration.getDefault().getCommandLine().hasOption("console")));
			System.setProperty("godix", Boolean.toString(Configuration.getDefault().getCommandLine().hasOption("godix")));
			
			new Hackation();
			
			if (Boolean.getBoolean("console") == true)
			{
				if (Boolean.getBoolean("godix") == true)
					new Console(System.in, System.out, new org.fuserleer.console.Ledger(), new org.fuserleer.console.Network(), new org.fuserleer.console.Contexts(),
													   new org.fuserleer.console.System(),
												   	   new org.fuserleer.console.Atoms(), new org.fuserleer.console.Spam(), new org.fuserleer.console.Twitter());
				else
					new Console(System.in, System.out, new org.fuserleer.console.Ledger(), new org.fuserleer.console.Network(), new org.fuserleer.console.Atoms(), new org.fuserleer.console.System());
			}

		}
		catch (Throwable t)
		{
			log.fatal("Unable to start", t);
			java.lang.System.exit(-1);
		}
	}
	
	private Hackation() throws Exception
	{
		// Universe //
		Universe.createAsDefault(Bytes.fromBase64String(Configuration.getDefault().get("universe")));

		// TIME //
		Constructor<?> timeConstructor = Class.forName(Configuration.getDefault().get("time.provider")).getConstructor(Configuration.class);
		TimeProvider timeProvider = (TimeProvider) timeConstructor.newInstance(Configuration.getDefault());
		Time.createAsDefault(timeProvider);

		// CONTEXTS //
		List<Context> contexts = new ArrayList<Context>();
		if (Configuration.getDefault().has("contexts.definitions") == true)
		{
			StringTokenizer tokenizer = new StringTokenizer(Configuration.getDefault().get("contexts.definitions"), ",");
			while(tokenizer.hasMoreTokens() == true)
			{
				String name = tokenizer.nextToken();
				contexts.add(Context.createAndStart(name.trim().toLowerCase(), Configuration.getDefault()));
			}
		}
		else if (Configuration.getDefault().getCommandLine().hasOption("contexts") == false)
			contexts.add(Context.createAndStart());
		else
			contexts.addAll(Context.createAndStart(Integer.parseInt(Configuration.getDefault().getCommandLine().getOptionValue("contexts", "1")), "node", Configuration.getDefault()));
		
		// API //
		API.create(contexts.get(0)).start();
	}
}
