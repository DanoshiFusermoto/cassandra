package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateAddress;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.StateOp;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atoms.particles.unique")
public final class UniqueParticle extends SignedParticle
{
	@JsonProperty("value")
	@DsonOutput(Output.ALL)
	private Hash value;
	
	UniqueParticle()
	{
		super();
	}
	
	public UniqueParticle(Hash value, ECPublicKey owner)
	{
		super(Spin.UP, owner);
		
		this.value = Objects.requireNonNull(value);
	}
	
	public Hash getValue()
	{
		return this.value;
	}
	
	@Override
	public void prepare(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException
	{
		stateMachine.sop(new StateOp(new StateAddress(UniqueParticle.class, this.value), StateOp.Instruction.NOT_EXISTS), this);
	}

	@Override
	public void execute(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException 
	{
		stateMachine.sop(new StateOp(new StateAddress(UniqueParticle.class, this.value), UInt256.from(this.value.toByteArray()), StateOp.Instruction.SET), this);
	}
	
	@Override
	public boolean isConsumable()
	{
		return false;
	}
}
