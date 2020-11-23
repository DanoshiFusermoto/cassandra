package org.fuserleer.common;

import java.net.URI;

import org.fuserleer.Universe;

public class Agent
{
	public static final int 	PROTOCOL_VERSION 		= 100;
	public static final int 	AGENT_VERSION 			= 100;
	public static final int 	REFUSE_AGENT_VERSION 	= 99;
	public static final String 	URI_PREFIX 				= "FLEX://";
	public static final String 	AGENT 					= URI_PREFIX+AGENT_VERSION;
	
	public static URI getURI(String host)
	{
		if (!host.toUpperCase().startsWith(Agent.URI_PREFIX))
			host = Agent.URI_PREFIX+host;

		URI uri = URI.create(host);

		if ((uri.getPath() != null && uri.getPath().length() > 0) || (uri.getQuery() != null && uri.getQuery().length() > 0))
			throw new IllegalArgumentException(uri+": Paths or queries are not permitted in peer URIs");

		if (uri.getPort() == -1)
		{
			host += ":"+Universe.getDefault().getPort();
			uri = URI.create(host);
		}

		return uri;
	}

	public static URI getURI(String host, int port)
	{
		return URI.create(Agent.URI_PREFIX+host+":"+(port == -1?+Universe.getDefault().getPort():port));
	}
}
