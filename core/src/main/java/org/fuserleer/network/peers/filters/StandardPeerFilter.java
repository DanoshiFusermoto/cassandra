package org.fuserleer.network.peers.filters;

import java.util.Objects;

import org.fuserleer.Context;
import org.fuserleer.common.Agent;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.peers.Peer;

public class StandardPeerFilter implements PeerFilter
{
	private static final Logger log = Logging.getLogger();

	private Context context;
	
	// TODO remove context and use dependency injection instead
	public StandardPeerFilter(Context context)
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
			// TODO test this is no longer needed
//			InetAddress address = InetAddress.getByName(peer.getURI().getHost());
//			if (Interfaces.getInstance().isSelf(address))
//				return true;

			if (this.context.getNetwork().isWhitelisted(peer.getURI()) == false)
		        return true;

			if (peer.getNode() == null)
				return true;

			if (peer.getNode().getIdentity().equals(this.context.getNode().getIdentity()))
				return true;

			if (peer.getNode().getProtocolVersion() != 0 && peer.getNode().getProtocolVersion() < Agent.PROTOCOL_VERSION)
	    		return true;

	    	if (peer.getNode().getAgentVersion() != 0 && peer.getNode().getAgentVersion() <= Agent.REFUSE_AGENT_VERSION)
	    		return true;

	    	if (peer.isBanned())
    			return true;

			return false;
		}
		catch (Exception ex)
		{
			log.error("Could not process filter on PeerFilter for Peer:"+peer.toString(), ex);
			return true;
		}
	}
}
