package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

// TODO domain security required to limit StateOps
@SerializerId2("ledger.state.op")
public final class StateOp
{
	public static enum Instruction
	{
		TYPE(true),
		
		EQUAL(true), NOT_EQUAL(true), 
		LESS(true), GREATER(true), 
		EXISTS(true), NOT_EXISTS(true),

		ADD(false), SUBTRACT(false),
		MULTIPLY(false), DIVIDE(false),
		INCREMENT(false), DECREMENT(false), 
		SET(false);

		private final boolean evaluatable;
		
		Instruction(boolean evaluatable)
		{
			this.evaluatable = evaluatable;
		}
		
		@JsonValue
		@Override
		public String toString() 
		{
			return this.name();
		}

		public boolean evaluatable()
		{
			return this.evaluatable;
		}
	}
	
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;
	
	@JsonProperty("domain")
	@DsonOutput(Output.ALL)
	private Hash domain;

	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private Hash key;

	@JsonProperty("value")
	@DsonOutput(Output.ALL)
	private UInt256 value;
	
	@JsonProperty("ins")
	@DsonOutput(Output.ALL)
	private Instruction ins;
	
	private StateOp()
	{
		// FOR SERIALIZER
	}
	
	public StateOp(final Hash key, final Instruction ins)
	{
		Objects.requireNonNull(ins, "Instruction is null");
		Objects.requireNonNull(key, "Key is null");
		
		if (ins.equals(Instruction.EXISTS) == false && ins.equals(Instruction.NOT_EXISTS) == false)
			throw new IllegalArgumentException("Instruction "+ins+" requires a value");
		
		this.domain = Hash.ZERO;
		this.key = key;
		this.ins = ins;
		this.value = null;
	}
	
	public StateOp(final Hash domain, final Hash key, final Instruction ins)
	{
		Objects.requireNonNull(domain, "Domain is null");
		Objects.requireNonNull(ins, "Instruction is null");
		Objects.requireNonNull(key, "Key is null");
		
		if (domain.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Domain is ZERO");

		if (ins.equals(Instruction.EXISTS) == false && ins.equals(Instruction.NOT_EXISTS) == false)
			throw new IllegalArgumentException("Instruction "+ins+" requires a value");
		
		this.domain = domain;
		this.key = key;
		this.value = null;
		this.ins = ins;
	}

	public StateOp(final Hash key, final UInt256 value, final Instruction ins)
	{
		Objects.requireNonNull(ins, "Instruction is null");
		Objects.requireNonNull(key, "Key is null");
		Objects.requireNonNull(key, "Value is null");
		
		if (ins.equals(Instruction.EXISTS) == false && ins.equals(Instruction.NOT_EXISTS) == false)
			throw new IllegalArgumentException("Instruction "+ins+" requires a value");

		this.key = key;
		this.value = value;
		this.ins = ins;
		this.domain = Hash.ZERO;
	}

	public StateOp(final Hash domain, final Hash key, final UInt256 value, final Instruction ins)
	{
		Objects.requireNonNull(domain, "Domain is null");
		Objects.requireNonNull(ins, "Instruction is null");
		Objects.requireNonNull(key, "Key is null");
		Objects.requireNonNull(key, "Value is null");
		
		if (domain.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Domain is ZERO");
		
		if (ins.equals(Instruction.EXISTS) == false && ins.equals(Instruction.NOT_EXISTS) == false)
			throw new IllegalArgumentException("Instruction "+ins+" requires a value");
		
		this.domain = domain;
		this.key = key;
		this.value = value;
		this.ins = ins;
	}

	public Hash key()
	{
		return this.key;
	}

	public UInt256 value()
	{
		return this.value;
	}

	public Instruction ins()
	{
		return this.ins;
	}

	public Hash domain()
	{
		return this.domain;
	}

	@Override
	public boolean equals(Object other)
	{
		if (other == null)
			return false;
		if (other == this)
			return true;

		if (other instanceof StateOp)
		{
			if (((StateOp)other).key.equals(this.key) == true &&
				((StateOp)other).ins.equals(this.ins) == true &&
				((StateOp)other).domain.equals(this.domain) == true && 
				((((StateOp)other).value == null && this.value == null) || ((StateOp)other).value.compareTo(this.value) == 0))
				return true;
		}
		
		return false;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(this.domain, this.key, this.ins, this.value);
	}

	@Override
	public String toString()
	{
		return this.ins+" "+this.domain+":"+this.key+(this.value == null ? "" : " "+this.value);
	}
}
