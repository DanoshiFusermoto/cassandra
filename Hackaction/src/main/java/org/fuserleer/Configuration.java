package org.fuserleer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;

public class Configuration extends PersistedConfiguration
{
	private static Configuration instance = null;
	
	static Configuration getDefault()
	{
		if (instance == null)
			throw new RuntimeException("Configuration not set");
		
		return instance;
	}

	static Configuration createAsDefault(String commandLineConfig, String[] commandLineArguments) throws IOException
	{
		if (instance != null)
			throw new RuntimeException("Default configuration already set");
		
		instance = new Configuration(commandLineConfig, commandLineArguments);
		
		return instance;
	}
	
	static Configuration clearDefault()
	{
		Configuration configuration = getDefault();
		instance = null;
		return configuration;
	}

	private CommandLine	commandLine = null;

	public Configuration(Configuration configuration)
	{
		super(new Properties(configuration.properties));
		
		this.commandLine = configuration.commandLine;
	}

	Configuration(String commandLineConfig, String[] commandLineArguments) throws IOException
	{
		try
		{
			JSONObject commandLineConfigJSON = new JSONObject();
			InputStream commandLineConfigStream = Configuration.class.getResourceAsStream(commandLineConfig);
			if (commandLineConfigStream != null)
				commandLineConfigJSON = new JSONObject(IOUtils.toString(commandLineConfigStream, StandardCharsets.UTF_8.name()));
	
			CommandLineParser parser = new DefaultParser ();
			Options gnuOptions = new Options();
			for (String clKey : commandLineConfigJSON.keySet())
			{
				JSONObject clOption = commandLineConfigJSON.getJSONObject(clKey);
				if (clOption.has("short") == true)
					gnuOptions.addOption(clOption.getString("short"), clKey, clOption.getBoolean("has_arg"),  clOption.optString("desc", ""));
				else
					gnuOptions.addOption(clKey, clOption.getBoolean("has_arg"),  clOption.optString("desc", ""));
			}
			this.commandLine = parser.parse(gnuOptions, commandLineArguments);
	
			load(this.commandLine.getOptionValue("config", "default.config"));
		}
		catch (JSONException | ParseException ex)
		{
			throw new IOException(ex);
		}
	}
	
	public CommandLine getCommandLine() 
	{ 
		return commandLine; 
	}

	@Override
	public String get(String key)
	{
		for (Option commandLineOption : this.commandLine.getOptions())
		{
			if (key.equals(commandLineOption.getOpt()) || key.equals(commandLineOption.getLongOpt()))
			{
				if (commandLineOption.hasArg())
					return commandLineOption.getValue();
				else
					return "1";
			}
		}

		return super.get(key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(String key, T defaultValue)
	{
		String value = null;

		for (Option commandLineOption : this.commandLine.getOptions())
		{
			if (key.equals(commandLineOption.getOpt()) || key.equals(commandLineOption.getLongOpt()))
			{
				if (commandLineOption.hasArg())
					value = commandLineOption.getValue();
				else
					value = "1";
			}
		}

		if (value == null)
			return super.get(key, defaultValue);
		else if (defaultValue instanceof Byte)
			return (T) Byte.valueOf(value);
		else if (defaultValue instanceof Short)
			return (T) Short.valueOf(value);
		else if (defaultValue instanceof Integer)
			return (T) Integer.valueOf(value);
		else if (defaultValue instanceof Long)
			return (T) Long.valueOf(value);
		else if (defaultValue instanceof Float)
			return (T) Float.valueOf(value);
		else if (defaultValue instanceof Double)
			return (T) Double.valueOf(value);
		else if (defaultValue instanceof Boolean)
			return (T) Boolean.valueOf(value);
		else if (defaultValue instanceof String)
			return (T) value;

		return null;
	}
}
