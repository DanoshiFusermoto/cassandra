package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.vote.state")
public final class StateVote extends Vote<Hash>
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;

	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;

	private StateVote()
	{
		// SERIALIZER
	}
	
	public StateVote(final Hash state, final Hash atom, final Hash block, final boolean decision, final ECPublicKey owner)
	{
		super(state, decision, owner);
		
		if (Objects.requireNonNull(atom, "Block is null").equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Block is ZERO");

		if (Objects.requireNonNull(atom, "Atom is null").equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Atom is ZERO");
		
		this.atom = atom;
		this.block = block;
	}
	
	public Hash getAtom()
	{
		return this.atom;
	}

	public Hash getBlock()
	{
		return this.block;
	}
	
	public Hash getState()
	{
		return this.getObject();
	}
}
