package org.fuserleer.network.peers.filters;

import java.util.concurrent.TimeUnit;

import org.fuserleer.Context;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.time.Time;

public class TCPPeerFilter extends StandardPeerFilter
{
	public TCPPeerFilter(Context context)
	{
		super(context);
	}

	@Override
	public boolean filter(Peer peer)
	{
		if (peer.getDisconnectedAt() > 0 && 
			Time.getSystemTime() - peer.getDisconnectedAt() < TimeUnit.SECONDS.toMillis(getContext().getConfiguration().get("network.peer.inactivity", 30)))
			return true;
		
		if (peer.getAttemptedAt() > 0 && peer.getAttemptedAt() > peer.getDisconnectedAt() && 
			Time.getSystemTime() - peer.getAttemptedAt() < TimeUnit.SECONDS.toMillis(getContext().getConfiguration().get("network.peer.reattempt", 600)))
			return true;

		return super.filter(peer);
	}
}
