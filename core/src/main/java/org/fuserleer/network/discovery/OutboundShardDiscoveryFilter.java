package org.fuserleer.network.discovery;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.fuserleer.Context;
import org.fuserleer.ledger.ShardMapper;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.time.Time;

public final class OutboundShardDiscoveryFilter extends StandardDiscoveryFilter
{
	private final Set<Long>	shardGroups;
	
	public OutboundShardDiscoveryFilter(final Context context, final Set<Long> shardGroups)
	{
		super(context);
		
		if (Objects.requireNonNull(shardGroups, "Shard groups is null").isEmpty() == true)
			throw new IllegalArgumentException("Shard groups is empty");
		
		this.shardGroups = new HashSet<Long>(shardGroups);
	}

	@Override
	public boolean filter(final Peer peer)
	{
		Objects.requireNonNull(peer, "Peer to filter is null");
		
		for (long shardGroup : this.shardGroups)
		{
			if (ShardMapper.toShardGroup(peer.getNode().getIdentity(), getContext().getLedger().numShardGroups()) != shardGroup)
				return false;
		}
		
		if (peer.getAttemptedAt() > 0 && peer.getDisconnectedAt() > 0 && 
			Time.getSystemTime() - peer.getDisconnectedAt() < TimeUnit.SECONDS.toMillis(getContext().getConfiguration().get("network.peer.inactivity", 30)))
			return false;
		
		if (peer.getAttemptAt() > 0 && 
			Time.getSystemTime() < peer.getAttemptAt())
			return false;

		return super.filter(peer);
	}
}
