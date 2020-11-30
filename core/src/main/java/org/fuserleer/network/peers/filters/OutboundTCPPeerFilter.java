package org.fuserleer.network.peers.filters;

import java.util.concurrent.TimeUnit;

import org.fuserleer.Context;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.time.Time;

public class OutboundTCPPeerFilter extends StandardPeerFilter
{
	public OutboundTCPPeerFilter(Context context)
	{
		super(context);
	}

	@Override
	public boolean filter(Peer peer)
	{
		if (peer.getAttemptedAt() > 0 && peer.getDisconnectedAt() > 0 && 
			Time.getSystemTime() - peer.getDisconnectedAt() < TimeUnit.SECONDS.toMillis(getContext().getConfiguration().get("network.peer.inactivity", 30)))
			return true;
		
		if (peer.getAttemptAt() > 0 && Time.getSystemTime() < peer.getAttemptAt())
			return true;

		return super.filter(peer);
	}
}
