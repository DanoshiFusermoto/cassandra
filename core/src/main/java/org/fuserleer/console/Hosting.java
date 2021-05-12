package org.fuserleer.console;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.fuserleer.Context;
import org.fuserleer.apps.SimpleWallet;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.HostedFileParticle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;

public class Hosting extends Function
{
	private final static Options options = new Options().addOption(Option.builder("upload").desc("Uploads a file/directory to be hosted").numberOfArgs(2).build())
														.addOption("list", false, "Lists all files currently uploaded and accessible");
	
	public Hosting()
	{
		super("hosting", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);
		
		SimpleWallet wallet = Wallet.get(context);
		if (wallet == null)
			throw new IllegalStateException("No wallet is open");
		
		if (commandLine.hasOption("upload") == true)
		{
			String[] options = commandLine.getOptionValues("upload");
			String rootURL = options[0];
			File source = new File(options[1]);
			
			if (source.exists() == false)
				throw new FileNotFoundException(source.toString());
			
			Collection<File> files;
			if (source.isDirectory() == true)
				files = FileUtils.listFiles(source, null, true);
			else
				files = Collections.singletonList(source);
				
			for (File file : files)
			{
				byte[] data = FileUtils.readFileToByteArray(file);

				String URL = rootURL;
				if (source.isDirectory() == false)
					URL += "/"+source.getName();
				else
					URL += file.getCanonicalPath().replace(source.getCanonicalPath(), "").replace('\\', '/');
				
				HostedFileParticle hostedFileParticle = new HostedFileParticle("text/html", URL, data, wallet.getIdentity());
				wallet.sign(hostedFileParticle);
				Atom atom = new Atom(hostedFileParticle);
				wallet.submit(atom);
				printStream.println(Serialization.getInstance().toJson(atom, Output.API));
			}
		}
		else if(commandLine.hasOption("list") == true)
		{
			List<HostedFileParticle> hostedFiles = wallet.get(HostedFileParticle.class, Spin.UP).stream().collect(Collectors.toList());
			hostedFiles.sort(new Comparator<HostedFileParticle>() 
			{
				@Override
				public int compare(HostedFileParticle arg0, HostedFileParticle arg1)
				{
					return arg0.getPath().compareToIgnoreCase(arg1.getPath());
				}
			});
			
			for (HostedFileParticle hostedFile : hostedFiles)
				printStream.println(hostedFile.getHash()+" "+hostedFile.getPath()+"   "+hostedFile.getContentType()+"   "+hostedFile.size()+" bytes");
		}
	}
}