package org.fuserleer.ledger;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.UInt256;

public final class ShardMapper 
{
	public static long toShardGroup(final ECPublicKey identity, final long numShardGroups)
	{
		return toShardGroup(Objects.requireNonNull(identity, "Identity is null").asLong(), numShardGroups);
	}

	public static long toShardGroup(final Hash hash, final long numShardGroups)
	{
		return toShardGroup(Longs.fromByteArray(Objects.requireNonNull(hash, "Hash is null").toByteArray()), numShardGroups);
	}

	public static Set<Long> toShardGroups(final Collection<UInt256> shards, final long numShardGroups)
	{
		Set<Long> shardGroups = new HashSet<Long>();
		for (UInt256 shard : Objects.requireNonNull(shards, "Shards is null"))
			shardGroups.add(toShardGroup(shard, numShardGroups));
		
		return shardGroups;
	}

	// TODO test this heavily ... UInts may have low value ranges and so all end up in similar shards ... don't want that
	public static long toShardGroup(final UInt256 shard, final long numShardGroups)
	{
		Objects.requireNonNull(shard, "Shard is null");
		if (shard.equals(UInt256.ZERO) == true)
			throw new IllegalArgumentException("Shard is ZERO");
		
		return toShardGroup(shard.getHigh().getHigh(), numShardGroups);
	}
	
	private static long toShardGroup(final long truncatedShard, final long numShardGroups)
	{
		if (numShardGroups < 0)
			throw new IllegalArgumentException("Num shard groups is negative");

		return Math.abs(truncatedShard % numShardGroups);
		
/*		UInt256 divisor = UInt256.ZERO.invert().divide(UInt256.from(numShardGroups)).add(UInt256.ONE);
		if (divisor.compareTo(UInt256.ZERO) != 0)
			return shard.divide(divisor).getLow();
		else
			return UInt128.ZERO;*/
	}
}
