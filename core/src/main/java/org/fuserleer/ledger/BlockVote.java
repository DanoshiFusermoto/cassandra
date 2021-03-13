package org.fuserleer.ledger;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.Numbers;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

// FIXME needs to be a Hash, not block header, then use the inventory mechanism to fetch the block header if missing locally
@SerializerId2("ledger.vote.block")
public final class BlockVote extends Vote<Hash>
{
	@JsonProperty("clock")
	@DsonOutput(Output.ALL)
	private long clock;

	@SuppressWarnings("unused")
	private BlockVote()
	{
		// SERIALIZER
	}
	
	public BlockVote(final Hash object, final long clock, final ECPublicKey owner)
	{
		super(object, StateDecision.POSITIVE, owner);
		
		Numbers.notNegative(clock, "Clock is negative");
		
		this.clock = clock;
	}

	public long getClock()
	{
		return this.clock;
	}
	
	public Hash getBlock()
	{
		return getObject();
	}

	public long getHeight()
	{
		return Longs.fromByteArray(getObject().toByteArray());
	}
}
