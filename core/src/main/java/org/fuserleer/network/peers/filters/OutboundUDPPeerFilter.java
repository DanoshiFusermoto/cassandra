package org.fuserleer.network.peers.filters;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.Context;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.time.Time;
import org.fuserleer.utils.UInt128;

public final class OutboundUDPPeerFilter extends StandardPeerFilter
{
	private final Set<UInt128>	shardGroups;
	
	public OutboundUDPPeerFilter(Context context, Set<UInt128> shardGroups)
	{
		super(context);
		
		if (Objects.requireNonNull(shardGroups, "Shard groups is null").isEmpty() == true)
			throw new IllegalArgumentException("Shard groups is empty");
		
		this.shardGroups = new HashSet<UInt128>(shardGroups);
	}

	@Override
	public boolean filter(Peer peer)
	{
		for (UInt128 shardGroup : this.shardGroups)
		{
			if (getContext().getLedger().getShardGroup(peer.getNode().getIdentity(), getContext().getLedger().getHead().getHeight()).compareTo(shardGroup) != 0)
				return true;
		}
		
		if (peer.getAttemptAt() > 0 && 
			Time.getSystemTime() < peer.getAttemptAt())
			return true;

		return super.filter(peer);
	}
}
