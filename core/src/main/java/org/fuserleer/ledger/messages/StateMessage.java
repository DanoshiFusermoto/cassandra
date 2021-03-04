package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.ledger.CommitStatus;
import org.fuserleer.ledger.StateKey;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.UInt256;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.state")
public final class StateMessage extends Message
{
	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;

	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private StateKey<?, ?> key;

	@JsonProperty("value")
	@DsonOutput(Output.ALL)
	private UInt256 value;

	@JsonProperty("status")
	@DsonOutput(Output.ALL)
	private CommitStatus status;

	@SuppressWarnings("unused")
	private StateMessage()
	{
		super();
	}
	
	public StateMessage(final Hash atom, final StateKey<?, ?> key, final UInt256 value, final CommitStatus status)
	{
		super();
		
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		
		this.atom = atom;
		this.key = Objects.requireNonNull(key, "State key is null");
		this.value = value;
		this.status = Objects.requireNonNull(status, "Status is null");
	}

	public Hash getAtom()
	{
		return this.atom;
	}

	public CommitStatus getStatus()
	{
		return this.status;
	}

	public StateKey<?, ?> getKey()
	{
		return this.key;
	}

	public UInt256 getValue()
	{
		return this.value;
	}
}
