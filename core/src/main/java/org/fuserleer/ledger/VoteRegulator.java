package org.fuserleer.ledger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.Context;
import org.fuserleer.Universe;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.UInt128;
import org.fuserleer.utils.UInt256;

public final class VoteRegulator
{
	private static final Logger ledgerLog = Logging.getLogger("ledger");

	private final Context context;
	private final Map<Long, UInt256> totalVotePower;
	private final Map<ECPublicKey, UInt128> votePower;
	
	VoteRegulator(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.votePower = Collections.synchronizedMap(new HashMap<ECPublicKey, UInt128>());
		this.totalVotePower = Collections.synchronizedMap(new HashMap<Long, UInt256>());
		for (ECPublicKey genode : Universe.getDefault().getGenodes())
			this.votePower.put(genode, UInt128.ONE);
		this.totalVotePower.put(0l, UInt256.ONE.multiply(UInt256.from(Universe.getDefault().getGenodes().size())));
	}
	
	public UInt128 getVotePower(ECPublicKey identity, long height)
	{
		return getVotePower(identity, height, Collections.emptyList());
	}

	UInt128 getVotePower(ECPublicKey identity, long height, List<BlockHeader> pending)
	{
		return this.votePower.getOrDefault(identity, UInt128.ZERO);
	}
	
	public UInt256 getTotalVotePower(long height)
	{
		if (height < 0)
			throw new IllegalArgumentException("Height is negative");
		
		if (height > this.context.getLedger().getHead().getHeight())
			height = this.context.getLedger().getHead().getHeight();

		return this.totalVotePower.getOrDefault(height, UInt256.ZERO);
	}
	
	public UInt256 getVotePower(long height, Set<ECPublicKey> identities)
	{
		if (height < 0)
			throw new IllegalArgumentException("Height is negative");
		
		if (height > this.context.getLedger().getHead().getHeight())
			height = this.context.getLedger().getHead().getHeight();

		UInt256 votePower = UInt256.ZERO;
		for (ECPublicKey identity : identities)
			votePower = votePower.add(this.votePower.getOrDefault(identity, UInt128.ZERO));
		
		return votePower;
	}

	public UInt256 getVotePowerThreshold(long height)
	{
		return twoFPlusOne(getTotalVotePower(height));
	}
	
	UInt128 addVotePower(ECPublicKey identity, long height)
	{
		if (height < 0)
			throw new IllegalArgumentException("Height is negative");
		
		if (height > this.context.getLedger().getHead().getHeight())
			height = this.context.getLedger().getHead().getHeight();

		this.votePower.put(identity, this.votePower.getOrDefault(identity, UInt128.ZERO).add(UInt128.ONE));
		this.totalVotePower.put(height, this.totalVotePower.getOrDefault(height-1, UInt256.ZERO).add(UInt256.ONE));
		
		ledgerLog.info("Incrementing vote power for "+identity+"@"+height+" to "+this.votePower.get(identity)+" total vote power "+this.totalVotePower.get(height));
		
		return this.votePower.getOrDefault(identity, UInt128.ZERO);
	}

	private UInt256 twoFPlusOne(UInt256 power)
	{
		UInt256 F = power.divide(UInt256.THREE);
		UInt256 T = F.multiply(UInt256.TWO);
		return T.add(UInt256.ONE);
	}
}
