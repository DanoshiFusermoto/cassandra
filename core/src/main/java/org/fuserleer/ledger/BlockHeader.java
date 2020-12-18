package org.fuserleer.ledger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignature;
import org.fuserleer.crypto.ECSignatureBag;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.database.IndexablePrimitive;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.MathUtils;
import org.fuserleer.utils.UInt256;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;

@SerializerId2("ledger.block.header")
public final class BlockHeader implements Comparable<BlockHeader>, Hashable, IndexablePrimitive, Cloneable
{
	public final static int	MAX_ATOMS = 1024;

	private static final Logger blocksLog = Logging.getLogger("blocks");

	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("height")
	@DsonOutput(Output.ALL)
	private long height;

	@JsonProperty("index")
	@DsonOutput(Output.ALL)
	private long index;

	@JsonProperty("previous")
	@DsonOutput(Output.ALL)
	private Hash previous;

	@JsonProperty("stepped")
	@DsonOutput(Output.ALL)
	private UInt256 stepped;

	@JsonProperty("merkle")
	@DsonOutput(Output.ALL)
	private Hash merkle;
	
	@JsonProperty("timestamp")
	@DsonOutput(Output.ALL)
	private long timestamp;

	@JsonProperty("owner")
	@DsonOutput(Output.ALL)
	private ECPublicKey owner;

	// TODO inventory of atoms, inefficient, find a better method
	@JsonProperty("inventory")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private List<Hash> inventory;

	@JsonProperty("signature")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private ECSignature signature;
	
	// TODO BLS this later
	@JsonProperty("certificate")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private ECSignatureBag certificate;

	private transient Hash hash;
	private transient long step = -1;
	
	BlockHeader()
	{
		super();
	}
	
	BlockHeader(final long height, final Hash previous, final UInt256 stepped, final long index, final List<Hash> inventory, final Hash merkle, final long timestamp, final ECPublicKey owner)
	{
		if (height < 0)
			throw new IllegalArgumentException("Height is negative");

		if (index < 0)
			throw new IllegalArgumentException("Index is negative");

		Objects.requireNonNull(previous, "Previous block is null");
		if (height == 0 && previous.equals(Hash.ZERO) == false)
			throw new IllegalArgumentException("Previous block hash must be ZERO for genesis");
		
		if (height != 0 && previous.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Previous block hash is ZERO");
		
		if (timestamp < 0)
			throw new IllegalArgumentException("Timestamp is negative");

		this.owner = Objects.requireNonNull(owner, "Block owner is null");
		this.merkle = Objects.requireNonNull(merkle, "Block merkle is null");
		this.inventory = new ArrayList<Hash>(Objects.requireNonNull(inventory, "Block inventory is null"));
		this.stepped = Objects.requireNonNull(stepped, "Stepped is null");
		this.previous = previous;
		this.height = height;
		this.index = index;
		this.timestamp = timestamp;
	}

	public long getHeight() 
	{
		return this.height;
	}

	public long getIndex() 
	{
		return this.index;
	}

	long getNextIndex() 
	{
		return this.index + this.inventory.size();
	}

	public long getIndexOf(Hash hash)
	{
		return this.index + this.inventory.indexOf(hash);
	}

	public long getTimestamp() 
	{
		return this.timestamp;
	}
	
	public UInt256 getStepped()
	{
		return this.stepped.add(UInt256.from(getStep()));
	}
	
	public long getAverageStep()
	{
		if (this.height == 0)
			return 0;
		UInt256 stepped = getStepped();
		UInt256	average = stepped.divide(UInt256.from(this.height+1));
		return average.getLow().getLow();
	}

	public long getStep()
	{
		if (this.step == -1)
		{
			try
			{
				byte[] bytes = Serialization.getInstance().toDson(clone(), Output.HASH);
				Hash hash = new Hash(bytes, Mode.DOUBLE);
				this.step = MathUtils.ringDistance64(new Hash(this.previous.toByteArray(), Mode.STANDARD).asLong(), hash.asLong());
			}
			catch (SerializationException ex)
			{
				// TODO Catch but only report
				blocksLog.error("Step calculation failed", ex);
			}
		}
		
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

	protected synchronized Hash computeHash()
	{
		try
		{
			byte[] contentBytes = Serialization.getInstance().toDson(clone(), Output.HASH);
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

	public Hash getMerkle() 
	{
		return this.merkle;
	}

	public List<Hash> getInventory() 
	{
		return Collections.unmodifiableList(this.inventory);
	}

	public Hash getPrevious() 
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
		return this.height+" "+getStep()+"/"+getAverageStep()+" "+getHash()+" "+this.previous+" "+this.merkle+" "+this.timestamp;
	}
	
	@Override
	public int compareTo(BlockHeader other)
	{
		return Long.compare(getHeight(), other.getHeight());
	}
	
	public ECPublicKey getOwner()
	{
		return this.owner;
	}

	public synchronized void sign(ECKeyPair key) throws CryptoException
	{
		if (key.getPublicKey().equals(getOwner()) == false)
			throw new CryptoException("Attempting to sign block header with key that doesn't match owner");

		this.signature = key.sign(getHash());
	}

	public synchronized boolean verify(ECPublicKey key) throws CryptoException
	{
		if (this.signature == null)
			throw new CryptoException("Signature is not present");
		
		if (getOwner() == null)
			return false;

		if (key.equals(getOwner()) == false)
			return false;

		return key.verify(getHash(), this.signature);
	}

	boolean requiresSignature()
	{
		return true;
	}
	
	public synchronized ECSignature getSignature()
	{
		return this.signature;
	}
	
	@Override
	public BlockHeader clone()
	{
		BlockHeader blockHeader = new BlockHeader(this.height, this.previous, this.stepped, this.index, this.inventory, this.merkle, this.timestamp, this.owner);
		blockHeader.signature = this.signature;
		blockHeader.certificate = this.certificate;
		return blockHeader;
	}
	
	public final ECSignatureBag getCertificate()
	{
		return this.certificate;
	}

	final void setCertificate(ECSignatureBag certificate)
	{
		this.certificate = Objects.requireNonNull(certificate, "Certificate is null");
	}
}
