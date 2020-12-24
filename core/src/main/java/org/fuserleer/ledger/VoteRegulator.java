package org.fuserleer.ledger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.Context;
import org.fuserleer.Universe;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public final class VoteRegulator
{
	private static final Logger ledgerLog = Logging.getLogger("ledger");

	private final Context context;
	private final Map<Long, Long> totalVotePower;
	private final Map<ECPublicKey, Long> votePower;
	
	VoteRegulator(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.votePower = Collections.synchronizedMap(new HashMap<ECPublicKey, Long>());
		this.totalVotePower = Collections.synchronizedMap(new HashMap<Long, Long>());
		for (ECPublicKey genode : Universe.getDefault().getGenodes())
			this.votePower.put(genode, 1l);
		this.totalVotePower.put(0l, (long) Universe.getDefault().getGenodes().size());
	}
	
	public long getVotePower(ECPublicKey identity, long height)
	{
		return getVotePower(identity, height, Collections.emptyList());
	}

	long getVotePower(ECPublicKey identity, long height, List<BlockHeader> pending)
	{
		return this.votePower.getOrDefault(identity, 0l);
	}
	
	public VotePowerBloom getVotePowerBloom(long height)
	{
		if (height < 0)
			throw new IllegalArgumentException("Height is negative");

		VotePowerBloom votePowerBloom = new VotePowerBloom(height);
		synchronized(this.votePower)
		{
			for (Entry<ECPublicKey, Long> votePower : this.votePower.entrySet())
				votePowerBloom.add(votePower.getKey(), votePower.getValue());
		}

		return votePowerBloom;
	}

	public long getTotalVotePower(long height)
	{
		if (height < 0)
			throw new IllegalArgumentException("Height is negative");
		
		if (height > this.context.getLedger().getHead().getHeight())
			height = this.context.getLedger().getHead().getHeight();

		return this.totalVotePower.getOrDefault(height, 0l);
	}
	
	public long getVotePower(long height, Set<ECPublicKey> identities)
	{
		if (height < 0)
			throw new IllegalArgumentException("Height is negative");
		
		if (height > this.context.getLedger().getHead().getHeight())
			height = this.context.getLedger().getHead().getHeight();

		long votePower = 0l;
		for (ECPublicKey identity : identities)
			votePower += this.votePower.getOrDefault(identity, 0l);
		
		return votePower;
	}

	public long getVotePowerThreshold(long height)
	{
		return twoFPlusOne(getTotalVotePower(height));
	}
	
	long addVotePower(ECPublicKey identity, long height)
	{
		if (height < 0)
			throw new IllegalArgumentException("Height is negative");
		
		if (height > this.context.getLedger().getHead().getHeight())
			height = this.context.getLedger().getHead().getHeight();

		this.votePower.put(identity, this.votePower.getOrDefault(identity, 0l) + 1l);
		this.totalVotePower.put(height, this.totalVotePower.getOrDefault(height-1, 0l) + 1l);
		
		ledgerLog.info("Incrementing vote power for "+identity+"@"+height+" to "+this.votePower.get(identity)+" total vote power "+this.totalVotePower.get(height));
		
		return this.votePower.getOrDefault(identity, 0l);
	}

	private long twoFPlusOne(long power)
	{
		long F = power / 3;
		long T = F * 2;
		return T + 1;
	}
}
