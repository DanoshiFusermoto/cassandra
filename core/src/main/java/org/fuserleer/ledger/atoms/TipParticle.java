package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.Field;
import org.fuserleer.database.Fields;
import org.fuserleer.database.Identifier;
import org.fuserleer.database.Indexable;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
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
		if (this.tipping.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Tipping reference is zero");
		
		this.transfer = Objects.requireNonNull(transfer);
		if (this.transfer.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Transfer reference is zero");
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
	public Set<Identifier> getIdentifiers() 
	{
		Set<Identifier> identifiers = super.getIdentifiers();
		identifiers.add(Identifier.from(this.tipping));
		identifiers.add(Identifier.from(this.transfer));
		return identifiers;
	}

	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException 
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
		
		super.prepare(stateMachine);
	}

	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException 
	{
		Atom tippingAtom = stateMachine.get(Indexable.from(this.tipping, Particle.class), Atom.class);
		if (tippingAtom != null)
		{
			Fields fields = tippingAtom.getFields(); //stateMachine.getCommitAccumulator().get(tippingAtom, Fields.class);
			Field field = fields.getOrDefault(Indexable.from(this.tipping, Particle.class), "tip_total", 0);
			
			TransferParticle transferParticle = stateMachine.getAtom().getParticle(this.transfer);
			fields.set(field.getScope(), field.getName(), ((int) field.getValue()) + transferParticle.getQuantity().getLow().getLow());
			stateMachine.set(tippingAtom.getHash(), fields);
		}
		else
			throw new ValidationException("Atom containing "+this.tipping+" not found");
	}
	
	@Override
	public void unexecute(StateMachine stateMachine) throws ValidationException, IOException 
	{
		Atom tippingAtom = stateMachine.get(Indexable.from(this.tipping, Particle.class), Atom.class);
		if (tippingAtom != null)
		{
			Fields fields = tippingAtom.getFields(); //stateMachine.getCommitAccumulator().get(tippingAtom, Fields.class);
			Field field = fields.getOrDefault(Indexable.from(this.tipping, Particle.class), "tip_total", 0);
			
			TransferParticle transferParticle = stateMachine.getAtom().getParticle(this.transfer);
			fields.set(field.getScope(), field.getName(), ((int) field.getValue()) - transferParticle.getQuantity().getLow().getLow());
			stateMachine.set(tippingAtom.getHash(), fields);
		}
		else
			throw new ValidationException("Atom containing "+this.tipping+" not found");
	}

	@Override
	public boolean isConsumable()
	{
		return false;
	}
}
