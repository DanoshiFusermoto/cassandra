package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.KeyPair;
import org.fuserleer.crypto.PublicKey;
import org.fuserleer.crypto.Signature;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

abstract class Vote<T, KP extends KeyPair<?, K, S>, K extends PublicKey, S extends Signature> extends BasicObject implements Primitive
{
	@JsonProperty("object")
	@DsonOutput(Output.ALL)
	private T object;
	
	@JsonProperty("decision")
	@DsonOutput(Output.ALL)
	private StateDecision decision;

	@JsonProperty("owner")
	@DsonOutput(Output.ALL)
	private K owner;

	@JsonProperty("signature")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private S signature;
	
	Vote()
	{
		// For serializer
	}
	
	Vote(final T object, final StateDecision decision)
	{
		this.object = Objects.requireNonNull(object, "Object is null");
		
		// TODO check object is serializable
		
		this.decision = Objects.requireNonNull(decision, "Decision is null");
	}

	public Vote(final T object, final StateDecision decision, final K owner)
	{
		this.object = Objects.requireNonNull(object, "Object is null");
		
		// TODO check object is serializable
		
		this.owner = Objects.requireNonNull(owner, "Owner is null");
		this.decision = Objects.requireNonNull(decision, "Decision is null");
	}

	Vote(final T object, final StateDecision decision, final K owner, final S signature) throws CryptoException
	{
		this.object = Objects.requireNonNull(object, "Object is null");
		this.owner = Objects.requireNonNull(owner, "Owner is null");
		this.signature = Objects.requireNonNull(signature, "Signature is null");
		this.decision = Objects.requireNonNull(decision, "Decision is null");
		
		// TODO check object is serializable
	}

	abstract Hash getTarget() throws CryptoException;
	
	final T getObject()
	{
		return this.object;
	}

	public final StateDecision getDecision()
	{
		return this.decision;
	}

	public final K getOwner()
	{
		return this.owner;
	}

	public final synchronized void sign(final KP key) throws CryptoException, SerializationException
	{
		Objects.requireNonNull(key, "Key pair is null");
		
		if (this.signature != null)
			throw new IllegalStateException("Vote "+getClass()+" is already signed "+this);

		if (key.getPublicKey().equals(getOwner()) == false)
			throw new CryptoException("Attempting to sign with key that doesn't match owner");
		
		this.signature = (S) key.sign(getTarget());
	}

	public final synchronized boolean verify(final K key) throws CryptoException, SerializationException
	{
		Objects.requireNonNull(key, "Public key is null");
		
		if (this.signature == null)
			throw new CryptoException("Signature is not present");
		
		if (getOwner() == null)
			return false;

		if (key.equals(getOwner()) == false)
			return false;
		
		return key.verify(getTarget(), this.signature);
	}
	
	boolean requiresSignature()
	{
		return true;
	}
	
	public final synchronized S getSignature()
	{
		return this.signature;
	}
	
	public String toString()
	{
		return super.toString()+" "+this.owner;
	}
}
