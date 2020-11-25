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
	
	public UInt256 totalVotePower(long height)
	{
		return UInt256.THREE;
	}
}
