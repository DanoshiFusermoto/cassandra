package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.state.inputs")
class StateInput implements Primitive
{
	static Hash getHash(final Hash atom, final StateKey<?, ?> key)
	{
		Objects.requireNonNull(key, "State key is null");
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is zero");
		
		return Hash.from(atom, key.get());
	}

	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;
		
	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private StateKey<?, ?> key;
	
	@JsonProperty("value")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private UInt256 value;
	
	@SuppressWarnings("unused")
	private StateInput()
	{
		// FOR SERIALIZER
	}
	
	StateInput(final Hash atom, final StateKey<?, ?> key, final UInt256 value)
	{
		Objects.requireNonNull(key, "State key is null");
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom hash is zero");

		this.atom = atom;
		this.key = key;
		this.value = value;
	}
	
	@Override
	public Hash getHash()
	{
		return getHash(this.atom, this.key);
	}

	public Hash getAtom()
	{
		return this.atom;
	}
	
	public StateKey<?, ?> getKey()
	{
		return this.key;
	}

	public UInt256 getValue()
	{
		return this.value;
	}
	
	public final String toString()
	{
		return getHash()+" "+getAtom()+" "+getKey();
	}
}
