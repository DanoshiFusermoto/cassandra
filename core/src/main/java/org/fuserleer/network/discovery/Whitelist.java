package org.fuserleer.network.discovery;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.Numbers;

public class Whitelist
{
	private static final Logger networkLog = Logging.getLogger("network");

	private final Set<String> parameters = new HashSet<String>();
	
	public Whitelist(final String parameters)
	{
		Objects.requireNonNull(parameters, "Whitelist parameters is null");
		if (parameters.isEmpty() == true)
			return;
		
		String[] split = parameters.split(",");
		
		for (String parameter : split)
		{
			if (parameter.trim().length() == 0)
				continue;
			
			this.parameters.add(parameter.trim());
		}
	}
	
	private int[] convert(final String host)
	{
		Objects.requireNonNull(host, "Whitelist host is null");
		Numbers.isZero(host.length(), "Whitelist host has length zero");

		String[] segments;
		int[] output;

		// IPV4 //
		if (host.contains("."))
		{
			output = new int[4];
			segments = host.split("\\.");
		}
		// IPV6 //
		else if (host.contains(":"))
		{
			output = new int[8];
			segments = host.split(":");
		}
		else
			return new int[] {0, 0, 0, 0};
		
		Arrays.fill(output, Integer.MAX_VALUE);
		for (int s = 0 ; s < segments.length ; s++)
		{
			if (segments[s].equalsIgnoreCase("*"))
				break;

			output[s] = Integer.valueOf(segments[s]);
		}
		
		return output;
	}
	
	private boolean isRange(final String parameter)
	{
		Objects.requireNonNull(parameter, "Whitelist range parameter is null");
		Numbers.isZero(parameter.length(), "Whitelist range parameter has length zero");

		if (parameter.contains("-"))
			return true;
		
		return false;
	}

	private boolean isInRange(final String parameter, final String address)
	{
		Objects.requireNonNull(address, "Whitelist range address is null");
		Numbers.isZero(address.length(), "Whitelist range address has length zero");

		Objects.requireNonNull(parameter, "Whitelist range parameter is null");
		Numbers.isZero(parameter.length(), "Whitelist range parameter has length zero");

		String[] hosts = parameter.split("-");
		
		if (hosts.length != 2)
			throw new IllegalStateException("Range is invalid");
		
		int[] target = convert(address);
		int[] low = convert(hosts[0]);
		int[] high = convert(hosts[1]);
		
		if (low.length != high.length || target.length != low.length)
			return false;
		
		for (int s = 0 ; s < low.length ; s++)
		{
			if (low[s] < high[s])
			{
				int[] swap = low;
				low = high;
				high = swap;
				break;
			}
			
			if (target[s] < low[s] || target[s] > high[s])
				return false;
		}
		
		return true;
	}

	private boolean isMask(final String parameter)
	{
		Objects.requireNonNull(parameter, "Whitelist mask parameter is null");
		Numbers.isZero(parameter.length(), "Whitelist range parameter has length zero");

		if (parameter.contains("*") || parameter.contains("::"))
			return true;
		
		return false;
	}
	
	private boolean isMasked(final String parameter, final String address) throws UnknownHostException, URISyntaxException
	{
		Objects.requireNonNull(address, "Whitelist mask address is null");
		Numbers.isZero(address.length(), "Whitelist mask address has length zero");

		Objects.requireNonNull(parameter, "Whitelist mask parameter is null");
		Numbers.isZero(parameter.length(), "Whitelist range parameter has length zero");

		int[] target = convert(address);
		int[] mask = convert(parameter);
		
		if (target.length != mask.length)
			return false;
		
		for (int s = 0 ; s < mask.length ; s++)
		{
			if (mask[s] == Integer.MAX_VALUE)
				return true;
			else if (target[s] != mask[s])
				return false;
		}
		
		return false;
	}

	public boolean accept(final URI host)
	{
		Objects.requireNonNull(host, "Accept host URI is null");
		
		if (this.parameters.isEmpty())
			return true;
		
		try
		{
			for (String parameter : this.parameters)
			{
				if (parameter.equalsIgnoreCase(host.getHost()))
					return true;
				else if (isRange(parameter) && isInRange(parameter, InetAddress.getByName(host.getHost()).getHostAddress()))
					return true;
				else if (isMask(parameter) && isMasked(parameter, InetAddress.getByName(host.getHost()).getHostAddress()))
					return true;
			}
		}
		catch (UnknownHostException | URISyntaxException ex)
		{
			networkLog.error(ex);
		}
		
		return false;
	}
}
