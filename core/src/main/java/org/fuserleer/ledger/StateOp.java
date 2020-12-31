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

@SerializerId2("ledger.state.op")
public final class StateOp
{
	public static enum Type
	{
		EQUAL, NOT_EQUAL, EXISTS, NOT_EXISTS, SET;

		@JsonValue
		@Override
		public String toString() 
		{
			return this.name();
		}
	}
	
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;
	
	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private Hash key;

	@JsonProperty("value")
	@DsonOutput(Output.ALL)
	private UInt256 value;
	
	@JsonProperty("op")
	@DsonOutput(Output.ALL)
	private Type op;
	
	private StateOp()
	{
		// FOR SERIALIZER
	}
	
	public StateOp(final Hash key, final Type op)
	{
		Objects.requireNonNull(op, "Op type is null");
		Objects.requireNonNull(key, "Key is null");
		
		if (op.equals(Type.EXISTS) == false && op.equals(Type.NOT_EXISTS) == false)
			throw new IllegalArgumentException("Op type "+op+" requires a value");
		
		this.key = key;
	}
	
	public StateOp(final Hash key, final UInt256 value, final Type op)
	{
		Objects.requireNonNull(op, "Op type is null");
		Objects.requireNonNull(key, "Key is null");
		Objects.requireNonNull(key, "Value is null");
		
		if (op.equals(Type.EXISTS) == true && op.equals(Type.NOT_EXISTS) == false)
			throw new IllegalArgumentException("Op type "+op+" is valueless");
		
		this.key = key;
	}

	public Hash getKey()
	{
		return this.key;
	}

	public UInt256 getValue()
	{
		return this.value;
	}

	public Type getOp()
	{
		return this.op;
	}
}
