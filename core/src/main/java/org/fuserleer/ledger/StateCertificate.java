package org.fuserleer.ledger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.fuserleer.collections.Bloom;
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.crypto.BLSSignature;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.MerkleProof;
import org.fuserleer.crypto.VoteCertificate;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;
import org.fuserleer.utils.UInt256;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;

@SerializerId2("ledger.state.certificate")
public final class StateCertificate extends VoteCertificate
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

	// FIXME need to implement some way to have agreement on producers as maybe weakly-subjective and dishonest actors can attempt to inject vote power
	@JsonProperty("producer")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private BLSPublicKey producer;

	// FIXME merkle and audit are for remote block proofing
	//		 not included in certificate hash currently so that aggregated state vote signatures can be verified
	@JsonProperty("merkle")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private Hash merkle;

	@JsonProperty("audit")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
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

	public StateCertificate(final StateKey<?, ?> state, final Hash atom, final Hash block, final BLSPublicKey producer, final UInt256 input, final UInt256 output, final Hash execution, final Hash merkle, final List<MerkleProof> audit, final VotePowerBloom powers, final Bloom signers, final BLSSignature signature) throws CryptoException
	{
		this(state, atom, block, producer, input, output, execution, merkle, audit, powers.getHash(), signers, signature);
		
		Objects.requireNonNull(powers, "Powers is null");
		Numbers.isZero(powers.count(), "Powers is empty");
		Numbers.isZero(powers.getTotalPower(), "Total vote power is zero");

		this.powerBloom = powers;
	}

	public StateCertificate(final StateKey<?, ?> state, final Hash atom, final Hash block, final BLSPublicKey producer, final UInt256 input, final UInt256 output, final Hash execution, final Hash merkle, final List<MerkleProof> audit, final Hash powers, final Bloom signers, final BLSSignature signature) throws CryptoException
	{
		super(Objects.requireNonNull(execution, "Execution is null").equals(Hash.ZERO) == false ? StateDecision.POSITIVE : StateDecision.NEGATIVE, signers, signature);
		
		Objects.requireNonNull(state, "State is null");
		Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block is ZERO");

		Objects.requireNonNull(powers, "Powers is null");
		Hash.notZero(powers, "Block is ZERO");
		Objects.requireNonNull(producer, "Producer is null");
		
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");

		Objects.requireNonNull(merkle, "Merkle is null");
		Hash.notZero(merkle, "Merkle is ZERO");

		Objects.requireNonNull(audit, "Audit is null");
		Numbers.isZero(audit.size(), "Audit is empty");

		this.state = state;
		this.atom = atom;
		this.block = block;
		this.producer = producer;
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
	
	public BLSPublicKey getProducer()
	{
		return this.producer;
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
	protected Hash getTarget() throws CryptoException
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
