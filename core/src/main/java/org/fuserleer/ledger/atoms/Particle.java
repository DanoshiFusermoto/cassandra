package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.fuserleer.BasicObject;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.database.Field;
import org.fuserleer.database.Fields;
import org.fuserleer.database.Identifier;
import org.fuserleer.database.Indexable;
import org.fuserleer.database.IndexablePrimitive;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateExecutor;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

@SerializerId2("ledger.atoms.particle")
public abstract class Particle extends BasicObject implements StateExecutor, IndexablePrimitive
{
	public enum Spin
	{
		UP, DOWN, NEUTRAL, ANY;

		@JsonValue
		@Override
		public String toString() 
		{
			return this.name();
		}
	}
	
	@JsonProperty("nonce")
	@DsonOutput(value = {Output.ALL})
	private long nonce;

	@JsonProperty("spin")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private Spin spin;
	
	private Fields fields;
	
	private transient boolean prepared = false;
	private transient boolean executed = true;
	private transient boolean unexecuted = false;
	
	Particle()
	{
		super();
		
		this.fields = new Fields();
	}

	Particle(Spin spin)
	{
		this();

		if (Objects.requireNonNull(spin).equals(Spin.NEUTRAL) == true)
			throw new IllegalArgumentException("Spin NEUTRAL particles are implicit");
		
		if (spin.equals(Spin.DOWN) == true && isConsumable() == false)
			throw new IllegalArgumentException("Particle of type "+this.getClass()+" is not consumable");
		
		this.spin = spin;
		this.nonce = ThreadLocalRandom.current().nextLong();
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException 
	{
		return (Particle) super.clone();
	}

	@SuppressWarnings("unchecked")
	public final <T extends Particle> T get(Spin spin)
	{
		if (Objects.requireNonNull(spin).equals(Spin.NEUTRAL) == true)
			throw new IllegalArgumentException("Spin NEUTRAL particles are implicit");

		try
		{
			T cloned = (T) this.clone();
			((Particle)cloned).spin = spin;
			return cloned;
		}
		catch (CloneNotSupportedException cnsex)
		{
			// Should never happen, throw a runtime instead.
			throw new RuntimeException(cnsex);
		}
	}

	public final Hash getHash(Spin spin)
	{
		return computeHash(spin);
	}
	
	protected final synchronized Hash computeHash()
	{
		return computeHash(this.spin);
	}
	
	private Hash computeHash(Spin spin)
	{
		if (Objects.requireNonNull(spin).equals(Spin.NEUTRAL) == true)
			throw new IllegalArgumentException("Spin NEUTRAL particles are implicit");

		try
		{
			byte[] hashBytes = Serialization.getInstance().toDson(this, Output.HASH);
			
			if (spin.equals(Spin.DOWN) == true)
				return new Hash(hashBytes, Mode.DOUBLE).invert();
			else
				return new Hash(hashBytes, Mode.DOUBLE);
		}
		catch (Exception e)
		{
			throw new RuntimeException("Error generating hash: " + e, e);
		}
	}
	
	public final Spin getSpin()
	{
		return this.spin; 
	}
	
	@JsonProperty("consumable")
	@DsonOutput(Output.API)
	public abstract boolean isConsumable(); 
	
	public Set<Indexable> getIndexables()
	{
		return new HashSet<Indexable>(); 
	}

	public Set<Identifier> getIdentifiers()
	{
		return new HashSet<Identifier>(); 
	}
	
	public final boolean hasIndexable(Indexable indexable)
	{
		Set<Indexable> indexables = getIndexables();
		return indexables.contains(indexable);
	}
	
	public Field getField(Indexable scope, String name)
	{
		return this.fields.get(scope, name);
	}

	public Field getField(Indexable scope, String name, Object value)
	{
		return this.fields.getOrDefault(scope, name, value);
	}

	public Field setField(Field field)
	{
		Objects.requireNonNull(field);
		return this.fields.set(field);
	}

	public Field setField(Indexable scope, String name, Object value)
	{
		return setField(new Field(scope, name, value));
	}
	
	public Fields getFields()
	{
		return this.fields;
	}
	
	@JsonProperty("fields")
	@DsonOutput(Output.API)
	Map<String, Object> getJsonFields() 
	{
		Map<String, Object> jsonified = new LinkedHashMap<>();
		
		for (Field field : this.fields)
			jsonified.put(field.getName(), ImmutableMap.<String, Object>of("scope", field.getScope(), "value", field.getValue()));
			
		return jsonified;
	}

	public String toString()
	{
		return super.toString()+" "+this.spin;
	}

	// STATE MACHINE //
	// FIXME need a better way to ensure that all super class prepare / execute elements are
	// actually executed, and catch any failures.  Shouldn't rely on explicit super calls.
	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException 
	{
		if (this.spin == null)
			throw new ValidationException("Spin is null");
		
		this.prepared = true;
	}

	public final boolean isPrepared() 
	{
		return this.prepared;
	}

	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException 
	{
		this.executed = true;
	}
	
	public final boolean isExecuted() 
	{
		return this.executed;
	}
	
	@Override
	public void unexecute(StateMachine stateMachine) throws ValidationException, IOException 
	{
		this.unexecuted = true;
	}
	
	public final boolean isUnexecuted() 
	{
		return this.unexecuted;
	}
}
