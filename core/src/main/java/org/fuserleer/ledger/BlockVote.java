package org.fuserleer.ledger;

import org.fuserleer.crypto.BLSKeyPair;
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.crypto.BLSSignature;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.Numbers;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.block.vote")
public final class BlockVote extends Vote<Hash, BLSKeyPair, BLSPublicKey, BLSSignature>
{
	@JsonProperty("clock")
	@DsonOutput(Output.ALL)
	private long clock;

	@SuppressWarnings("unused")
	private BlockVote()
	{
		// SERIALIZER
	}
	
	public BlockVote(final Hash object, final long clock, final BLSPublicKey owner)
	{
		super(object, StateDecision.POSITIVE, owner);

		// FIXME clock is unguarded due to signing getTarget and not vote hash for BLS
		Numbers.isNegative(clock, "Clock is negative");
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

	@Override
	Hash getTarget() throws CryptoException
	{
		return getObject();
	}
}
