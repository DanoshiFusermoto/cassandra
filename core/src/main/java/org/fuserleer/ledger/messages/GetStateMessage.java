package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.ledger.StateKey;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.state.get")
public final class GetStateMessage extends Message
{
	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private StateKey<?, ?> key;

	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;

	@SuppressWarnings("unused")
	private GetStateMessage()
	{
		super();
	}
	
	public GetStateMessage(final Hash atom, final StateKey<?, ?> key)
	{
		super();
		
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		
		this.atom = atom;
		this.key = Objects.requireNonNull(key, "Key is null");
	}

	public Hash getAtom()
	{
		return this.atom;
	}
	
	public StateKey<?, ?> getKey()
	{
		return this.key;
	}
}
