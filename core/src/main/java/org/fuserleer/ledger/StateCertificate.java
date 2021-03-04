package org.fuserleer.ledger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.fuserleer.crypto.Certificate;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignature;
import org.fuserleer.crypto.ECSignatureBag;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.MerkleProof;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.UInt256;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;

@SerializerId2("ledger.atoms.state.certificate")
public final class StateCertificate extends Certificate
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;

	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;
	
	@JsonProperty("state")
	@DsonOutput(Output.ALL)
	private StateKey<?, ?> state;

	@JsonProperty("input")
	@DsonOutput(Output.ALL)
	private UInt256 input;

	@JsonProperty("output")
	@DsonOutput(Output.ALL)
	private UInt256 output;

	@JsonProperty("execution")
	@DsonOutput(Output.ALL)
	private Hash execution;

	@JsonProperty("merkle")
	@DsonOutput(Output.ALL)
	private Hash merkle;

	@JsonProperty("audit")
	@DsonOutput(Output.ALL)
	private List<MerkleProof> audit;

	// FIXME need to implement some way to have agreement on powers as weakly-subjective and dishonest actors can attempt to inject vote power
	@JsonProperty("powers")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private Hash powers;

	@JsonProperty("power_bloom")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private VotePowerBloom powerBloom;
	
	@SuppressWarnings("unused")
	private StateCertificate()
	{
		super();
		
		// FOR SERIALIZER //
	}

	public StateCertificate(final StateKey<?, ?> state, final Hash atom, final Hash block, final UInt256 input, final UInt256 output, final Hash execution, final Hash merkle, final List<MerkleProof> audit, final VotePowerBloom powers, final ECSignatureBag signatures) throws CryptoException
	{
		this(state, atom, block, input, output, execution, merkle, audit, powers.getHash(), signatures);
		
		if (Objects.requireNonNull(powers, "Powers is null").count() == 0)
			throw new IllegalArgumentException("Powers is empty");

		if (Objects.requireNonNull(powers, "Total vote power null").getTotalPower() == 0)
			throw new IllegalArgumentException("Total vote power is ZERO");

		this.powerBloom = powers;
	}

	public StateCertificate(final StateKey<?, ?> state, final Hash atom, final Hash block, final UInt256 input, final UInt256 output, final Hash execution, final Hash merkle, final List<MerkleProof> audit, final Hash powers, final ECSignatureBag signatures) throws CryptoException
	{
		super(Objects.requireNonNull(execution, "Execution is null").equals(Hash.ZERO) == false ? StateDecision.POSITIVE : StateDecision.NEGATIVE, signatures);
		
		Objects.requireNonNull(state, "State is null");
		Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block is ZERO");

		Objects.requireNonNull(powers, "Powers is null");
		Hash.notZero(powers, "Block is ZERO");
		
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");

		Objects.requireNonNull(merkle, "Merkle is null");
		Hash.notZero(merkle, "Merkle is ZERO");

		if (Objects.requireNonNull(audit, "Audit is null").isEmpty() == true)
			Objects.requireNonNull(merkle, "Audit is empty");

		this.state = state;
		this.atom = atom;
		this.block = block;
		this.input = input;
		this.output = output;
		this.execution = execution;
		this.merkle = merkle;
		this.powers = powers;
		this.audit = new ArrayList<MerkleProof>(audit);
		this.powers = powers;
	}

	public Hash getBlock()
	{
		return this.block;
	}
	
	public long getHeight()
	{
		return Longs.fromByteArray(this.block.toByteArray());
	}

	public Hash getAtom()
	{
		return this.atom;
	}

	public <T extends StateKey<?, ?>> T getState()
	{
		return (T) this.state;
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
	public <T> T getObject()
	{
		return (T) this.state;
	}
	
	public Hash getMerkle()
	{
		return this.merkle;
	}
	
	public List<MerkleProof> getAudit()
	{
		return Collections.unmodifiableList(this.audit);
	}

	public Hash getPowers()
	{
		return this.powers;
	}

	public VotePowerBloom getPowerBloom()
	{
		return this.powerBloom;
	}

	@Override
	public boolean verify(final ECPublicKey signer, final ECSignature signature)
	{
		StateVote vote = new StateVote(getState(), getAtom(), getBlock(), getInput(), getOutput(), getExecution(), signer);
		return signer.verify(vote.getHash(), signature);
	}
}
