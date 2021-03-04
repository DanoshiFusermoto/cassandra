package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignature;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.vote.state")
public final class StateVote extends Vote<StateKey<?, ?>>
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;

	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;
	
	@JsonProperty("input")
	@DsonOutput(Output.ALL)
	private UInt256 input;

	@JsonProperty("output")
	@DsonOutput(Output.ALL)
	private UInt256 output;

	@JsonProperty("execution")
	@DsonOutput(Output.ALL)
	private Hash execution;
	
	@SuppressWarnings("unused")
	private StateVote()
	{
		// SERIALIZER
	}
	
	public StateVote(final StateKey<?, ?> state, final Hash atom, final Hash block, final UInt256 input, final UInt256 output, final Hash execution, final ECPublicKey owner)
	{
		super(state, Objects.requireNonNull(execution, "Execution is null").equals(Hash.ZERO) == false ? StateDecision.POSITIVE : StateDecision.NEGATIVE, owner);

		Objects.requireNonNull(atom, "Block is null");
		Hash.notZero(block, "Block is ZERO");

		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		
		this.atom = atom;
		this.block = block;
		this.input = input;
		this.output = output;
		this.execution = execution;
	}

	public StateVote(final StateKey<?, ?> state, final Hash atom, final Hash block, final UInt256 input, final UInt256 output, final Hash execution, final ECPublicKey owner, final ECSignature signature) throws CryptoException
	{
		super(state, Objects.requireNonNull(execution, "Execution is null").equals(Hash.ZERO) == false ? StateDecision.POSITIVE : StateDecision.NEGATIVE, owner, signature);

		Objects.requireNonNull(atom, "Block is null");
		Hash.notZero(block, "Block is ZERO");

		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		
		this.atom = atom;
		this.block = block;
		this.input = input;
		this.output = output;
		this.execution = execution;
	}
	
	public Hash getAtom()
	{
		return this.atom;
	}

	public Hash getBlock()
	{
		return this.block;
	}
	
	public <T extends StateKey<?, ?>> T getState()
	{
		return (T) this.getObject();
	}

	public UInt256 getInput()
	{
		return this.input;
	}

	public UInt256 getOutput()
	{
		return this.output;
	}

	public Hash getExecution()
	{
		return this.execution;
	}
}
