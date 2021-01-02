package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.bouncycastle.util.Arrays;
import org.fuserleer.BasicObject;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.database.Field;
import org.fuserleer.database.Fields;
import org.fuserleer.database.Identifier;
import org.fuserleer.database.Indexable;
import org.fuserleer.database.IndexablePrimitive;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.StateOp;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

@SerializerId2("ledger.atoms.particle")
public abstract class Particle extends BasicObject implements IndexablePrimitive
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
		
		public static Hash spin(Hash hash, Spin spin)
		{
			byte[] hashBytes = Arrays.clone(hash.toByteArray());
			if (spin.equals(Spin.DOWN) == true)
				hashBytes[0] &= ~1;
			else if (spin.equals(Spin.UP) == true)
				hashBytes[0] |= 1;
			
			return new Hash(hashBytes);
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
			return Spin.spin(new Hash(hashBytes, Mode.DOUBLE), spin);
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
	
	public final Set<Hash> getStates()
	{
		final Set<Hash> states = new LinkedHashSet<Hash>();
		getStateOps().forEach(sop -> states.add(sop.key()));
		return states;
	}
	
	public Set<StateOp> getStateOps()
	{
		Set<StateOp> stateops = new LinkedHashSet<StateOp>();
		if (getSpin().equals(Spin.UP) == true)
		{
			stateops.add(new StateOp(getHash(Spin.UP), Instruction.NOT_EXISTS));
			stateops.add(new StateOp(getHash(Spin.UP), UInt256.from(getHash(Spin.UP).toByteArray()), Instruction.SET));
		}
		else if (getSpin().equals(Spin.DOWN) == true)
		{
			stateops.add(new StateOp(getHash(Spin.UP), Instruction.EXISTS));
			stateops.add(new StateOp(getHash(Spin.DOWN), Instruction.NOT_EXISTS));
			stateops.add(new StateOp(getHash(Spin.DOWN), UInt256.from(getHash(Spin.DOWN).toByteArray()), Instruction.SET));
		}

		return stateops;
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
	
	@JsonProperty("shards")
	@DsonOutput(Output.API)
	public final Set<UInt256> getShards()
	{
		Set<StateOp> stateOps = getStateOps();
		return Collections.unmodifiableSet(stateOps.stream().map(sop -> UInt256.from(sop.key().toByteArray())).collect(Collectors.toSet()));
	}

	public String toString()
	{
		return super.toString()+" "+this.spin;
	}

	// STATE MACHINE //
	// FIXME need a better way to ensure that all super class prepare / execute elements are
	// actually executed, and catch any failures.  Shouldn't rely on explicit super calls.
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

	public void execute(StateMachine stateMachine) throws ValidationException, IOException 
	{
		this.executed = true;
	}
	
	public final boolean isExecuted() 
	{
		return this.executed;
	}
	
	public void unexecute(StateMachine stateMachine) throws ValidationException, IOException 
	{
		this.unexecuted = true;
	}
	
	public final boolean isUnexecuted() 
	{
		return this.unexecuted;
	}
}
