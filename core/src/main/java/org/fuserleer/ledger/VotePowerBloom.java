package org.fuserleer.ledger;

import java.util.Objects;

import org.bouncycastle.util.Arrays;
import org.fuserleer.collections.Bloom;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Ints;

@SerializerId2("ledger.vote_power_bloom")
public final class VotePowerBloom
{
	private static final int MAX_VOTE_POWER_SHIFTS = 16;
	private static final double VOTE_POWER_PROBABILITY = 0.000001;
	private static final int EXPECTED_VOTE_POWER_ENTRIES = 1000;
	
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("bloom")
	@DsonOutput(Output.ALL)
    private Bloom	bloom;

	@JsonProperty("total_power")
	@DsonOutput(Output.ALL)
    private long	totalPower;
	
	public VotePowerBloom()
	{
		this.bloom = new Bloom(VOTE_POWER_PROBABILITY, EXPECTED_VOTE_POWER_ENTRIES);
		this.totalPower = 0l;
	}
	
	void add(final ECPublicKey identity, final long power)
	{
		Objects.requireNonNull(identity, "Identity is null");
		if (power == 0)
			return;
		
		if (power < 0)
			throw new IllegalArgumentException("Power is negative");
		
		for (int shift = 0 ; shift < MAX_VOTE_POWER_SHIFTS ; shift++)
			if (((power >> shift) & 1) == 1)
				this.bloom.add(Arrays.concatenate(identity.getBytes(), Ints.toByteArray(shift)));
		
		this.totalPower += power;
	}
	
	public long power(final ECPublicKey identity)
	{
		Objects.requireNonNull(identity, "Identity is null");
		
		int power = 0;
		for (int shift = 0 ; shift < MAX_VOTE_POWER_SHIFTS ; shift++)
			if (this.bloom.contains(Arrays.concatenate(identity.getBytes(), Ints.toByteArray(shift))) == true)
				power += (1 << shift);
		
		return power;
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
