package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.Context;
import org.fuserleer.Universe;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.utils.UInt128;
import org.fuserleer.utils.UInt256;

public final class VoteRegulator
{
	private final Context context;
	
	VoteRegulator(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
	}
	
	public UInt128 getVotePower(ECPublicKey identity, long height)
	{
		if (Universe.getDefault().getGenodes().contains(identity) == true)
			return UInt128.ONE;
		
		return UInt128.ZERO;
	}
	
	public UInt256 getTotalVotePower(long height)
	{
		return UInt256.THREE;
	}
	
	public UInt256 getVotePowerThreshold(long height)
	{
		return twoFPlusOne(UInt256.THREE);
	}
	
	private UInt256 twoFPlusOne(UInt256 power)
	{
		UInt256 F = power.divide(UInt256.THREE);
		UInt256 T = F.multiply(UInt256.TWO);
		return T.add(UInt256.ONE);
	}
}
