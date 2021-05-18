package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.crypto.Identity;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class OwnedParticle extends Particle
{
	@JsonProperty("owner")
	@DsonOutput(Output.ALL)
	private Identity owner;

	protected OwnedParticle()
	{
		super();
	}
	
	protected OwnedParticle(Spin spin, Identity owner)
	{
		super(spin);
		
		this.owner = Objects.requireNonNull(owner);
	}
	
	@Override
	public final boolean isEphemeral()
	{
		return false;
	}

	public final Identity getOwner()
	{
		return this.owner;
	}

	public String toString()
	{
		return super.toString()+" "+this.owner;
	}
	
	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException 
	{
		if (this.owner == null)
			throw new ValidationException("Owner is null");
		
		super.prepare(stateMachine);
	}
	
	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException 
	{
		stateMachine.associate(getOwner().asHash(), this);
	}
}
