package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.collections.Bloom;
import org.fuserleer.crypto.BLSSignature;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.VoteCertificate;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;

@SerializerId2("ledger.block.certificate")
public final class BlockCertificate extends VoteCertificate
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;
	
	@SuppressWarnings("unused") 
	private BlockCertificate()
	{
		super();
	}
	
	BlockCertificate(final Hash block)
	{
		super();
		
		Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block is ZERO");

		this.block = block;
	}

	BlockCertificate(final Hash block, final Bloom signers, final BLSSignature signature) throws CryptoException
	{
		super(StateDecision.POSITIVE, signers, signature);
		
		Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block is ZERO");

		this.block = block;
	}

	public Hash getBlock()
	{
		return this.block;
	}
	
	public long getHeight()
	{
		return Longs.fromByteArray(this.block.toByteArray());
	}

	@Override
	public <T> T getObject()
	{
		return (T) this.block;
	}

	@Override
	protected Hash getTarget() throws CryptoException
	{
		return this.block;
	}
}
