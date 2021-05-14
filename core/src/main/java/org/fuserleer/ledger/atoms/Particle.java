package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.bouncycastle.util.Arrays;
import org.fuserleer.BasicObject;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.common.Primitive;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateInstruction;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

@SerializerId2("ledger.particle")
public abstract class Particle extends BasicObject implements Primitive, StateInstruction
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

	protected Particle()
	{
		super();
	}
	
	protected Particle(Spin spin)
	{
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
	
	public final Spin getSpin()
	{
		return this.spin; 
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

	@JsonProperty("consumable")
	@DsonOutput(Output.API)
	public abstract boolean isConsumable();
	
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException
	{
		if (this.spin == null)
			throw new ValidationException("Spin is null");
	}
	
	public abstract void execute(StateMachine stateMachine) throws ValidationException, IOException;
	
	public String toString()
	{
		return super.toString()+" "+this.spin;
	}
}
