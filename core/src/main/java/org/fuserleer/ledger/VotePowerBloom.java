package org.fuserleer.ledger;

import java.util.Objects;

import org.bouncycastle.util.Arrays;
import org.fuserleer.BasicObject;
import org.fuserleer.collections.Bloom;
import org.fuserleer.crypto.PublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Ints;

@SerializerId2("ledger.vote_power_bloom")
public final class VotePowerBloom extends BasicObject
{
	private static final int MAX_VOTE_POWER_SHIFTS = 16;
	private static final double VOTE_POWER_PROBABILITY = 0.000001;
	private static final int EXPECTED_VOTE_POWER_ENTRIES = 1000;
	
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
    private Hash block;

	@JsonProperty("group")
	@DsonOutput(Output.ALL)
    private long shardGroup;

	@JsonProperty("bloom")
	@DsonOutput(Output.ALL)
    private Bloom	bloom;

	@JsonProperty("total_power")
	@DsonOutput(Output.ALL)
    private long	totalPower;
	
	@SuppressWarnings("unused")
	private VotePowerBloom()
	{
		// FOR SERIALIZER 
	}
	
	public VotePowerBloom(final Hash block, final long shardGroup, final int numIdentities)
	{
		this.block = Objects.requireNonNull(block, "Block hash is null");
		this.bloom = new Bloom(VOTE_POWER_PROBABILITY, numIdentities*MAX_VOTE_POWER_SHIFTS);
		this.totalPower = 0l;
		
		Numbers.isNegative(shardGroup, "Shard group is negative");
		this.shardGroup = shardGroup;
	}
	
	void add(final PublicKey identity, final long power)
	{
		Objects.requireNonNull(identity, "Identity is null");
		Numbers.isNegative(power, "Power is negative");

		if (power == 0)
			return;
		
		for (int shift = 0 ; shift < MAX_VOTE_POWER_SHIFTS ; shift++)
			if (((power >> shift) & 1) == 1)
				this.bloom.add(Arrays.concatenate(identity.toByteArray(), Ints.toByteArray(shift)));
		
		this.totalPower += power;
	}
	
	public long power(final PublicKey identity)
	{
		Objects.requireNonNull(identity, "Identity is null");
		
		int power = 0;
		for (int shift = 0 ; shift < MAX_VOTE_POWER_SHIFTS ; shift++)
			if (this.bloom.contains(Arrays.concatenate(identity.toByteArray(), Ints.toByteArray(shift))) == true)
				power += (1 << shift);
		
		return power;
	}
	
	public Hash getBlock()
	{
		return this.block;
	}

	public long getShardGroup()
	{
		return this.shardGroup;
	}

	public long getTotalPower()
	{
		return this.totalPower;
	}

	public int count()
	{
		return this.bloom.count();
	}
}
