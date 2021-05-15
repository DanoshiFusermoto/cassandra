package org.fuserleer.ledger.atoms;

import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.KeyPair;
import org.fuserleer.crypto.PublicKey;
import org.fuserleer.crypto.Signature;
import org.fuserleer.crypto.Identity;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class SignedParticle extends OwnedParticle
{
	@JsonProperty("signature")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private Signature signature;
	
	protected SignedParticle()
	{
		super();
	}
	
	protected SignedParticle(Spin spin, Identity owner)
	{
		super(spin, owner);
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException 
	{
		SignedParticle object = (SignedParticle) super.clone();
		object.signature = null;
		return object;
	}

	public final synchronized void sign(KeyPair<?, ?, ?> key) throws CryptoException
	{
		if (key.getIdentity().equals(getOwner()) == false)
			throw new CryptoException("Attempting to sign particle with key that doesn't match owner");

		this.signature = key.sign(getHash());
	}

	public final synchronized boolean verify(PublicKey key) throws CryptoException
	{
		if (this.signature == null)
			throw new CryptoException("Signature is not present");
		
		if (getOwner() == null)
			return false;

		if (key.getIdentity().equals(getOwner()) == false)
			return false;

		return key.verify(getHash(), this.signature);
	}

	public boolean requiresSignature()
	{
		return true;
	}
	
	public final synchronized Signature getSignature()
	{
		return this.signature;
	}
}
