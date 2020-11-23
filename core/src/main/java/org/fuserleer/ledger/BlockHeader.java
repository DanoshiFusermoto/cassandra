package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;

@SerializerId2("ledger.block.header")
public class BlockHeader implements Comparable<BlockHeader>, Hashable, Primitive
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("height")
	@DsonOutput(Output.ALL)
	private long height;

	@JsonProperty("previous")
	@DsonOutput(Output.ALL)
	private Hash previous;

	@JsonProperty("merkle")
	@DsonOutput(Output.ALL)
	private Hash merkle;
	
	@JsonProperty("step")
	@DsonOutput(Output.ALL)
	private long step;

	private transient Hash hash;
	
	BlockHeader()
	{
		super();
	}
	
	BlockHeader(final long height, final long step, final Hash previous, final Hash merkle)
	{
		if (height < 0)
			throw new IllegalArgumentException("Height is negative");

		if (step < 0)
			throw new IllegalArgumentException("Step is negative");

		Objects.requireNonNull(previous, "Previous block is null");
		if (height == 0 && previous.equals(Hash.ZERO) == false)
			throw new IllegalArgumentException("Previous block hash must be ZERO for genesis");
		
		if (height != 0 && previous.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Previous block hash is ZERO");

		this.merkle = Objects.requireNonNull(merkle, "Block merkle is null");
		this.previous = previous;
		this.height = height;
		this.step = step;
	}

	public final long getHeight() 
	{
		return this.height;
	}

	public final long getStep() 
	{
		return this.step;
	}
	
	@Override
	@JsonProperty("hash")
	@DsonOutput(value = Output.HASH, include = false)
	public synchronized Hash getHash()
	{
		if (this.hash == null)
			this.hash = computeHash();
		
		if (this.hash == null)
			throw new NullPointerException("Block hash is null");

		return this.hash;
	}

	final protected synchronized Hash computeHash()
	{
		try
		{
			byte[] contentBytes = Serialization.getInstance().toDson(this, Output.HASH);
			byte[] hashBytes = new byte[Hash.BYTES];
			System.arraycopy(Longs.toByteArray(getHeight()), 0, hashBytes, 0, Long.BYTES);
			System.arraycopy(new Hash(contentBytes, Mode.DOUBLE).toByteArray(), 0, hashBytes, Long.BYTES, Hash.BYTES - Long.BYTES);
			return new Hash(hashBytes);
		}
		catch (Exception e)
		{
			throw new RuntimeException("Error generating hash: " + e, e);
		}
	}

	@JsonProperty("hash")
	void setHash(Hash hash)
	{
		Objects.requireNonNull(hash);
		this.hash = hash;
	}


	public final Hash getMerkle() 
	{
		return this.merkle;
	}

	public final Hash getPrevious() 
	{
		return this.previous;
	}

	@Override
	public int hashCode() 
	{
		return this.hash.hashCode();
	}

	@Override
	public boolean equals(Object other) 
	{
		if (other == null || (other instanceof BlockHeader) == false)
			return false;
		
		if (other == this)
			return true;

		if (((BlockHeader) other).getHash().equals(getHash()) == true)
			return true;
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.height+" "+this.step+" "+getHash()+" "+this.previous+" "+this.merkle;
	}
	
	@Override
	public int compareTo(BlockHeader other)
	{
		return Long.compare(getHeight(), other.getHeight());
	}
}
