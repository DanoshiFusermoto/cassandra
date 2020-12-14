package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.database.Indexable;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atoms.particles.token")
public final class TokenSpecification extends SignedParticle 
{
	public static int MAX_ISO_LENGTH = 8;
	public static int MAX_DESCRIPTION_LENGTH = 128;
	
	@JsonProperty("iso")
	@DsonOutput(Output.ALL)
	private String ISO;

	@JsonProperty("description")
	@DsonOutput(value = Output.HASH, include = false)
	private String description;
	
	TokenSpecification()
	{
		super();
	}
	
	public TokenSpecification(String ISO, String description, ECPublicKey owner)
	{
		super(Spin.UP, owner);
		
		if (Objects.requireNonNull(ISO).isEmpty() || ISO.length() > TokenSpecification.MAX_ISO_LENGTH)
			throw new IllegalArgumentException("ISO is greater than MAX_ISO_LENGTH "+TokenSpecification.MAX_ISO_LENGTH);

		if (Objects.requireNonNull(description).isEmpty() || description.length() > TokenSpecification.MAX_DESCRIPTION_LENGTH)
			throw new IllegalArgumentException("Description is greater than MAX_DESCRIPTION_LENGTH "+TokenSpecification.MAX_DESCRIPTION_LENGTH);

		this.ISO = Objects.requireNonNull(ISO.toUpperCase());
		this.description = description;
	}

	public String getISO() 
	{
		return this.ISO;
	}

	public String getDescription() 
	{
		return this.description;
	}
	
	@Override
	public Set<Indexable> getIndexables()
	{
		Set<Indexable> indexables = super.getIndexables();
		indexables.add(Indexable.from(this.ISO, getClass()));
		return indexables;
	}
	
	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException 
	{
		// TODO not sure if nulls in the prepare sections should be caught 
		// and thrown as a ValidatorException, or as a NullPointerException ... decide
		if (this.ISO == null)
			throw new ValidationException("ISO is null");
		
		if (this.ISO.isEmpty() == true)
			throw new ValidationException("ISO is empty");

		if (this.ISO.length() > MAX_ISO_LENGTH)
			throw new ValidationException("ISO is greater than MAX_ISO_LENGTH "+TokenSpecification.MAX_ISO_LENGTH);
		
		if (this.description == null)
			throw new ValidationException("Description is null");
		
		if (this.description.isEmpty() == true)
			throw new ValidationException("Description is empty");

		if (this.description.length() > MAX_DESCRIPTION_LENGTH)
			throw new ValidationException("Description is greater than MAX_DESCRIPTION_LENGTH "+TokenSpecification.MAX_DESCRIPTION_LENGTH);
	}

	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException
	{
		TokenSpecification token = stateMachine.get("token");
		if (token == null)
			stateMachine.put("token", this);
		else
			throw new ValidationException("State machine already has token "+token+" specified");
	}
	
	@Override
	public boolean isConsumable()
	{
		return false;
	}
}
