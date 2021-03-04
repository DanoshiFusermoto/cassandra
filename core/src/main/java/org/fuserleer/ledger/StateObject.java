package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.UInt256;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.state.object")
public final class StateObject implements Hashable
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;
	
	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private StateKey<?, ?> key;

	@JsonProperty("value")
	@DsonOutput(Output.ALL)
	private UInt256 value;
	
	@SuppressWarnings("unused")
	private StateObject()
	{
		// FOR SERIALIZER 
	}

	public StateObject(final StateKey<?, ?> key)
	{
		Objects.requireNonNull(key, "Key is null");
		
		this.value = null;
		this.key = key;
	}

	public StateObject(final StateKey<?, ?> key, final UInt256 value)
	{
		Objects.requireNonNull(key, "Key is null");
		Objects.requireNonNull(value, "Value is null");
		
		this.value = value;
		this.key = key;
	}
	
	@Override
	public Hash getHash()
	{
		return this.key.get();
	}
	
	public StateKey<?, ?> getKey()
	{
		return this.key;
	}

	public UInt256 getValue()
	{
		return this.value;
	}
	
	@Override
	public boolean equals(Object other)
	{
		if (other == null)
			return false;
		if (other == this)
			return true;

		if (other instanceof StateObject)
		{
			if (((StateObject)other).key.equals(this.key) == true &&
				((StateObject)other).value.equals(this.value) == true)
				return true;
		}
		
		return false;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(this.key, this.value);
	}

	@Override
	public String toString()
	{
		return this.key+" = "+this.value;
	}
}
