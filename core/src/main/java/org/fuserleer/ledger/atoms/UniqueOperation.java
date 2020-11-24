package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.Indexable;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atoms.particles.unique")
public final class UniqueOperation extends SignedParticle
{
	@JsonProperty("value")
	@DsonOutput(Output.ALL)
	private Hash value;
	
	UniqueOperation()
	{
		super();
	}
	
	public UniqueOperation(Hash value, ECPublicKey owner)
	{
		super(Spin.UP, owner);
		
		this.value = Objects.requireNonNull(value);
	}
	
	public Hash getValue()
	{
		return this.value;
	}

	@Override
	public Set<Indexable> getIndexables()
	{
		Set<Indexable> indexables = super.getIndexables();
		indexables.add(Indexable.from(this.value, getClass()));
		return indexables;
	}

	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException
	{
		/* DO NOTHING */
	}

	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException 
	{
		/* DO NOTHING */
	}
	
	@Override
	public boolean isConsumable()
	{
		return false;
	}
}
