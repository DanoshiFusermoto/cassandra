package org.fuserleer.crypto;

import java.util.Objects;

import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class SignedObject<T>
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
	
	public SignedObject(T object, ECPublicKey owner)
	{
		this.object = Objects.requireNonNull(object);
		
		// TODO check object is serializable
		
		this.owner = Objects.requireNonNull(owner);
	}

	public final T getObject()
	{
		return this.getObject();
	}

	public final ECPublicKey getOwner()
	{
		return this.owner;
	}

	public final synchronized void sign(ECKeyPair key) throws CryptoException, SerializationException
	{
		if (key.getPublicKey().equals(getOwner()) == false)
			throw new CryptoException("Attempting to sign particle with key that doesn't match owner");

		Hash objectHash = new Hash(Serialization.getInstance().toDson(object, Output.HASH), Mode.STANDARD);
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

		Hash objectHash = new Hash(Serialization.getInstance().toDson(object, Output.HASH), Mode.STANDARD);
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
