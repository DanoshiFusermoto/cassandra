package org.fuserleer.crypto;

import java.util.Objects;

import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("crypto.signed_object")
public final class SignedObject<T>
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("object")
	@DsonOutput(Output.ALL)
	private T object;
	
	@JsonProperty("owner")
	@DsonOutput(Output.ALL)
	private ECPublicKey owner;

	@JsonProperty("signature")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private ECSignature signature;
	
	@SuppressWarnings("unused")
	private SignedObject()
	{
		// For serializer
	}
	
	public SignedObject(final T object, final ECPublicKey owner)
	{
		this.object = Objects.requireNonNull(object, "Object is null");
		
		// TODO check object is serializable
		
		this.owner = Objects.requireNonNull(owner, "Owner is null");
	}

	public SignedObject(final T object, final ECPublicKey owner, final ECSignature signature) throws CryptoException
	{
		this.object = Objects.requireNonNull(object, "Object is null");
		this.owner = Objects.requireNonNull(owner, "Owner is null");
		this.signature = Objects.requireNonNull(signature, "Signature is null");
		
		// TODO check object is serializable
		
		try
		{
			if (verify(owner) == false)
				throw new CryptoException("Signed object invalid / not verified");
		}
		catch (SerializationException ex)
		{
			throw new CryptoException("Signed object invalid / not verified", ex);
		}
	}

	public final T getObject()
	{
		return this.object;
	}

	public final ECPublicKey getOwner()
	{
		return this.owner;
	}

	public final synchronized void sign(final ECKeyPair key) throws CryptoException, SerializationException
	{
		Objects.requireNonNull(key, "Signing key is null");

		if (key.getPublicKey().equals(getOwner()) == false)
			throw new CryptoException("Attempting to sign wrapped object with key that doesn't match owner");

		Hash objectHash;
		if (this.object instanceof Hashable)
			objectHash = ((Hashable)this.object).getHash();
		else
			objectHash = new Hash(Serialization.getInstance().toDson(this.object, Output.HASH), Mode.DOUBLE);
		
		this.signature = key.sign(objectHash);
	}

	public final synchronized boolean verify(final ECPublicKey key) throws CryptoException, SerializationException
	{
		Objects.requireNonNull(key, "Verification key is null");
		
		if (this.signature == null)
			throw new CryptoException("Signature is not present");
		
		if (getOwner() == null)
			return false;

		if (key.equals(getOwner()) == false)
			return false;

		Hash objectHash;
		if (this.object instanceof Hashable)
			objectHash = ((Hashable)this.object).getHash();
		else
			objectHash = new Hash(Serialization.getInstance().toDson(this.object, Output.HASH), Mode.DOUBLE);

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
