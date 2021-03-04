package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class StateKey<S, K>
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;
	
	@JsonProperty("scope")
	@DsonOutput(Output.ALL)
	private S scope;

	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private K key;
	
	StateKey()
	{
		// FOR SERIALIZER 
	}

	public StateKey(final K key)
	{
		Objects.requireNonNull(key, "Key is null");
		
		this.scope = null;
		this.key = key;
	}
	
	public StateKey(final S scope, final K key)
	{
		Objects.requireNonNull(scope, "Scope is null");
		Objects.requireNonNull(key, "Key is null");
		
		this.scope = scope;
		this.key = key;
	}

	public abstract Hash get();

	public K key()
	{
		return this.key;
	}

	void key(K key)
	{
		this.key = key;
	}

	public S scope()
	{
		return this.scope;
	}

	void scope(S scope)
	{
		this.scope = scope;
	}

	@Override
	public boolean equals(Object other)
	{
		if (other == null)
			return false;
		if (other == this)
			return true;

		if (other instanceof StateKey)
		{
			if (((StateKey<?, ?>)other).key.equals(this.key) == true &&
				((StateKey<?, ?>)other).scope.equals(this.scope) == true)
				return true;
		}
		
		return false;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(this.scope, this.key);
	}

	@Override
	public String toString()
	{
		return get()+" "+this.scope+":"+this.key;
	}
	
	// TODO not sure if want to keep this, or if the benefits outweigh the complexity of it
	abstract void fromByteArray(byte[] bytes) throws IOException;
	abstract byte[] toByteArray() throws IOException;
}
