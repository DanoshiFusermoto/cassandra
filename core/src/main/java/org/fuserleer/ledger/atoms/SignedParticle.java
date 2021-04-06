package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignature;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class SignedParticle extends Particle
{
	@JsonProperty("owner")
	@DsonOutput(Output.ALL)
	private ECPublicKey owner;

	@JsonProperty("signature")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private ECSignature signature;
	
	protected SignedParticle()
	{
		super();
	}
	
	protected SignedParticle(Spin spin, ECPublicKey owner)
	{
		super(spin);
		
		this.owner = Objects.requireNonNull(owner);
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException 
	{
		SignedParticle object = (SignedParticle) super.clone();
		object.signature = null;
		return object;
	}

	public final ECPublicKey getOwner()
	{
		return this.owner;
	}

	public final synchronized void sign(ECKeyPair key) throws CryptoException
	{
		if (key.getPublicKey().equals(getOwner()) == false)
			throw new CryptoException("Attempting to sign particle with key that doesn't match owner");

		this.signature = key.sign(getHash());
	}

	public final synchronized boolean verify(ECPublicKey key) throws CryptoException
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
	
	public final synchronized ECSignature getSignature()
	{
		return this.signature;
	}
	
	public String toString()
	{
		return super.toString()+" "+this.owner;
	}
	
	@Override
	public void prepare(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException 
	{
		if (this.owner == null)
			throw new ValidationException("Owner is null");
		
		super.prepare(stateMachine);
	}
	
	@Override
	public void execute(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException 
	{
		stateMachine.associate(getOwner().asHash(), this);
	}
}
