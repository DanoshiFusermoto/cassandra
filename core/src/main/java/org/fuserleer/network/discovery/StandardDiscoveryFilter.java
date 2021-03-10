package org.fuserleer.network.discovery;

import java.util.Objects;

import org.fuserleer.Context;
import org.fuserleer.common.Agent;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.peers.Peer;

public class StandardDiscoveryFilter implements DiscoveryFilter
{
	private static final Logger log = Logging.getLogger();

	private Context context;
	
	// TODO remove context and use dependency injection instead
	public StandardDiscoveryFilter(Context context)
	{
		this.context = Objects.requireNonNull(context);
	}
	
	protected Context getContext()
	{
		return this.context;
	}

	public boolean filter(Peer peer)
	{
		try
		{
			if (this.context.getNetwork().isWhitelisted(peer.getURI()) == false)
		        return false;

			if (peer.getNode() == null)
				return false;

			if (peer.getNode().getIdentity().equals(this.context.getNode().getIdentity()))
				return false;

			if (peer.getNode().getProtocolVersion() != 0 && peer.getNode().getProtocolVersion() < Agent.PROTOCOL_VERSION)
	    		return false;

	    	if (peer.getNode().getAgentVersion() != 0 && peer.getNode().getAgentVersion() <= Agent.REFUSE_AGENT_VERSION)
	    		return false;

	    	if (peer.isBanned())
    			return false;

			return true;
		}
		catch (Exception ex)
		{
			log.error("Could not process filter on PeerFilter for Peer:"+peer.toString(), ex);
			return false;
		}
	}
}
