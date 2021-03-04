package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateAddress;
import org.fuserleer.ledger.StateField;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.StateOp;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atoms.particles.tip")
public final class TipParticle extends SignedParticle
{
	@JsonProperty("tipping")
	@DsonOutput(Output.ALL)
	private Hash tipping;

	@JsonProperty("transfer")
	@DsonOutput(Output.ALL)
	private Hash transfer;
	
	TipParticle()
	{
		super();
	}

	public TipParticle(Hash tipping, Hash transfer, ECPublicKey owner)
	{
		super(Spin.UP, owner);
		
		this.tipping = Objects.requireNonNull(tipping);
		Hash.notZero(this.tipping, "Tipping reference is zero");
		
		this.transfer = Objects.requireNonNull(transfer);
		Hash.notZero(this.transfer, "Transfer reference is zero");
	}

	public Hash getTipping() 
	{
		return this.tipping;
	}

	public Hash getTransfer() 
	{
		return this.transfer;
	}
	
	@Override
	public void prepare(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException 
	{
		if (this.tipping.equals(Hash.ZERO) == true)
			throw new ValidationException("Tipping reference is zero");
		
		if (this.transfer.equals(Hash.ZERO) == true)
			throw new ValidationException("Transfer reference is zero");
		
		if (this.tipping.equals(this.transfer) == true)
			throw new ValidationException("Can not tip a tip transfer");
		
		boolean hasTransfer = false;
		for (TransferParticle transferParticle : stateMachine.getAtom().getParticles(TransferParticle.class))
		{
			if (transferParticle.getSpin().equals(Spin.UP) == true && 
				transferParticle.getHash().equals(this.transfer) == true)
			{
				hasTransfer = true;
				break;
			}
		}
		
		if (hasTransfer == false)
			throw new ValidationException("Atom "+stateMachine.getAtom().getHash()+" doesn't contain transfer "+this.transfer+" for tip "+this.getHash());
		
		stateMachine.sop(new StateOp(new StateAddress(Particle.class, this.transfer), Instruction.EXISTS), this);
		stateMachine.sop(new StateOp(new StateAddress(Particle.class, this.tipping), Instruction.EXISTS), this);

		super.prepare(stateMachine);
	}
	
	@Override
	public void execute(StateMachine stateMachine, Object... arguments) throws ValidationException, IOException
	{
		stateMachine.sop(new StateOp(new StateField(this.tipping, "tip_total"), Instruction.SET), this);
		
		stateMachine.associate(this.tipping, this);
		stateMachine.associate(this.transfer, this);

		super.execute(stateMachine, arguments);
	}

	@Override
	public boolean isConsumable()
	{
		return false;
	}
}
