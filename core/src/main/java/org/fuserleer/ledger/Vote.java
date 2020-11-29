package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.BasicObject;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignature;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.vote")
public final class Vote<T> extends BasicObject
{
	@JsonProperty("object")
	@DsonOutput(Output.ALL)
	private T object;
	
	@JsonProperty("owner")
	@DsonOutput(Output.ALL)
	private ECPublicKey owner;

	@JsonProperty("signature")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private ECSignature signature;
	
	@JsonProperty("clock")
	@DsonOutput(Output.ALL)
	private long clock;
	
	private Vote()
	{
		// For serializer
	}
	
	public Vote(final T object, final long clock, final ECPublicKey owner)
	{
		if (clock < 0)
			throw new IllegalArgumentException("Clock is negative");

		this.object = Objects.requireNonNull(object, "Object is null");
		
		// TODO check object is serializable
		
		this.owner = Objects.requireNonNull(owner, "Owner is null");
	}

	public Vote(final T object, final long clock, final ECPublicKey owner, final ECSignature signature) throws CryptoException
	{
		if (clock < 0)
			throw new IllegalArgumentException("Clock is negative");
		
		this.object = Objects.requireNonNull(object, "Object is null");
		this.owner = Objects.requireNonNull(owner, "Owner is null");
		this.signature = Objects.requireNonNull(signature, "Signature is null");
		
		// TODO check object is serializable
		
		try
		{
			if (verify(owner) == false)
				throw new CryptoException("Vote invalid / not verified");
		}
		catch (SerializationException ex)
		{
			throw new CryptoException("Vote invalid / not verified", ex);
		}
	}

	public final T getObject()
	{
		return this.object;
	}

	public final long getClock()
	{
		return this.clock;
	}

	public final ECPublicKey getOwner()
	{
		return this.owner;
	}

	public final synchronized void sign(ECKeyPair key) throws CryptoException, SerializationException
	{
		if (key.getPublicKey().equals(getOwner()) == false)
			throw new CryptoException("Attempting to sign wrapped object with key that doesn't match owner");

		Hash objectHash;
		if (object instanceof Hashable)
			objectHash = ((Hashable)object).getHash();
		else
			objectHash = new Hash(Serialization.getInstance().toDson(object, Output.HASH), Mode.DOUBLE);
		
		this.signature = key.sign(objectHash);
	}

	public final synchronized boolean verify(ECPublicKey key) throws CryptoException, SerializationException
	{
		if (this.signature == null)
			throw new CryptoException("Signature is not present");
		
		if (getOwner() == null)
			return false;

		if (key.equals(getOwner()) == false)
			return false;

		Hash objectHash;
		if (object instanceof Hashable)
			objectHash = ((Hashable)object).getHash();
		else
			objectHash = new Hash(Serialization.getInstance().toDson(object, Output.HASH), Mode.DOUBLE);

		return key.verify(objectHash, this.signature);
	}

	boolean requiresSignature()
	{
		return true;
	}
	
	public final synchronized ECSignature getSignature()
	{
		return this.signature;
	}
	
	public String toString()
	{
		return super.toString()+" "+this.owner;
	}
}
