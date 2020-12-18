package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.Indexable;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.UInt256;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atoms.particles.transfer")
public final class TransferParticle extends SignedParticle 
{
	@JsonProperty("quantity")
	@DsonOutput(Output.ALL)
	private UInt256 quantity;

	@JsonProperty("token")
	@DsonOutput(Output.ALL)
	private Hash token;
	
	TransferParticle()
	{
		super();
	}
	
	public TransferParticle(UInt256 quantity, Hash token, Spin spin, ECPublicKey owner)
	{
		super(spin, owner);
		
		this.quantity = Objects.requireNonNull(quantity);
		this.token = Objects.requireNonNull(token);
	}

	@Override
	boolean requiresSignature()
	{
		if (getSpin().equals(Spin.DOWN) == true)
			return true;
		else
			return false;
	}

	public UInt256 getQuantity() 
	{
		return this.quantity;
	}

	public Hash getToken() 
	{
		return this.token;
	}

	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException
	{
		if (this.token == null)
			throw new ValidationException("Token is null");
		
		if (this.quantity == null)
			throw new ValidationException("Quantity is null");

		if (this.quantity.compareTo(UInt256.ZERO) == 0)
			throw new ValidationException("Quantity is zero");

		if (this.quantity.compareTo(UInt256.ZERO) < 0)
			throw new ValidationException("Quantity is negative");
	}

	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException
	{
		TokenSpecification token = stateMachine.get("token");
		// Discover the token if its not referenced in the state machine 
		if (token == null)
		{
			token = stateMachine.get(Indexable.from(this.token, TokenSpecification.class), TokenSpecification.class);
			if (token != null)
			{
				if (stateMachine.state(Indexable.from(token.getHash(Spin.DOWN), TokenSpecification.class)) == true)
					throw new ValidationException("Transfer token "+token+" is in DOWN state");
					
				stateMachine.put("token", token);
			}
			else
				throw new ValidationException("Transfer token "+token+" not found");
		}
		
		// Check all transfers within this state machine as using the same token
		if (token.getHash().equals(this.token) == false)
			throw new ValidationException("Transfer is not multi-token, expected token "+token+" but discovered "+this.token);

		// Check that "out" quantity does not exceed "in" quantity
		UInt256 spendable = stateMachine.get("spendable");
		UInt256 spent = stateMachine.get("spent");
		
		// FIXME take these out to produce an NPE that isn't caught correct / nor fails the unit test
		if (spendable == null)
			spendable = UInt256.ZERO;
		if (spent == null)
			spent = UInt256.ZERO;
		
		if (getSpin().equals(Spin.DOWN) == true)
			spendable = spendable.add(this.quantity);
		else
			spent = spent.add(this.quantity);
		
		if (spent.compareTo(spendable) > 0)
			throw new ValidationException("Transfer is invalid, over spending available token "+getToken()+" by "+spent.subtract(spendable));
		
		stateMachine.put("spendable", spendable);
		stateMachine.put("spent", spent);
	}

	@Override
	public boolean isConsumable()
	{
		return true;
	}
}