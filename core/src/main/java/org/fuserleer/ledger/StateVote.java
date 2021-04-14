package org.fuserleer.ledger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;

import org.fuserleer.crypto.BLSKeyPair;
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.crypto.BLSSignature;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;

@SerializerId2("ledger.state.vote")
public final class StateVote extends Vote<StateKey<?, ?>, BLSKeyPair, BLSPublicKey, BLSSignature>
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;

	/** Carries the producer of the block for this state key.
	 * 	<br><br>
	 * 	The producer is agreed upon via the votes and included in the state certificate.
	 *  As the state certificates are sent to remote shard groups, it allows validators in
	 *  the other groups to maintain an accurate set of the validators present in all groups.
	 *  Including the producer also promotes a correct re-play for syncing validators.
	 * 	<br><br>
	 *  TODO check edge cases on this ... 
	 * 	<br><br>
	 *  if a block is sparsely populated it may not touch all other shard groups
	 *  which may in turn leave "gaps" in remote validators knowledge the global validator set.
	 *  This is particularly risky if the block producer is a new validator who doesn't produce another
	 *  block for some time after the initial block and may cause a "vote power" discrepancy large enough
	 *  to prevent validation of state certificates.
	 */
	@JsonProperty("producer")
	@DsonOutput(Output.ALL)
	private BLSPublicKey producer;

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
	
	StateVote(final StateKey<?, ?> state, final Hash atom, final Hash block, final BLSPublicKey producer, final UInt256 input, final UInt256 output, final Hash execution)
	{
		super(state, Objects.requireNonNull(execution, "Execution is null").equals(Hash.ZERO) == false ? StateDecision.POSITIVE : StateDecision.NEGATIVE);

		Objects.requireNonNull(atom, "Block is null");
		Hash.notZero(block, "Block is ZERO");
		Objects.requireNonNull(producer, "Producer is null");

		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		
		this.atom = atom;
		this.block = block;
		this.producer = producer;
		this.input = input;
		this.output = output;
		this.execution = execution;
	}


	public StateVote(final StateKey<?, ?> state, final Hash atom, final Hash block, final BLSPublicKey producer, final UInt256 input, final UInt256 output, final Hash execution, final BLSPublicKey owner)
	{
		super(state, Objects.requireNonNull(execution, "Execution is null").equals(Hash.ZERO) == false ? StateDecision.POSITIVE : StateDecision.NEGATIVE, owner);

		Objects.requireNonNull(atom, "Block is null");
		Hash.notZero(block, "Block is ZERO");
		Objects.requireNonNull(producer, "Producer is null");

		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		
		this.atom = atom;
		this.block = block;
		this.producer = producer;
		this.input = input;
		this.output = output;
		this.execution = execution;
	}

	public StateVote(final StateKey<?, ?> state, final Hash atom, final Hash block, final BLSPublicKey producer, final UInt256 input, final UInt256 output, final Hash execution, final BLSPublicKey owner, final BLSSignature signature, BLSSignature aggregatable) throws CryptoException
	{
		super(state, Objects.requireNonNull(execution, "Execution is null").equals(Hash.ZERO) == false ? StateDecision.POSITIVE : StateDecision.NEGATIVE, owner, signature);

		Objects.requireNonNull(atom, "Block is null");
		Hash.notZero(block, "Block is ZERO");
		Objects.requireNonNull(producer, "Producer is null");

		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		
		this.atom = atom;
		this.block = block;
		this.producer = producer;
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
	
	public BLSPublicKey getProducer()
	{
		return this.producer;
	}
	
	public long getHeight()
	{
		return Longs.fromByteArray(this.block.toByteArray());
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

	@Override
	Hash getTarget() throws CryptoException
	{
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write(this.getState().toByteArray());
			baos.write(this.atom.toByteArray());
			baos.write(this.block.toByteArray());
			baos.write(this.producer.toByteArray());
			baos.write(this.execution.toByteArray());
			
			// TODO input AND output can be null??
			if (this.output != null)
				baos.write(this.output.toByteArray());
			if (this.input != null)
				baos.write(this.input.toByteArray());
			return Hash.from(baos.toByteArray());
		}
		catch (IOException ioex)
		{
			throw new CryptoException(ioex);
		}
	}
}
