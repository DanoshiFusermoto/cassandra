package org.fuserleer.ledger;

import java.util.Collection;
import java.util.Objects;

import org.fuserleer.crypto.Certificate;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignature;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

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
	private Hash state;

	@JsonProperty("powers")
	@DsonOutput(Output.ALL)
	private VotePowerBloom powers;
	
	private StateCertificate()
	{
		super();
		
		// FOR SERIALIZER //
	}

	public StateCertificate(final Hash state, final Hash atom, final Hash block, final boolean decision, final VotePowerBloom powers, final Collection<StateVote> votes) throws CryptoException
	{
		super(decision);
		
		if (Objects.requireNonNull(atom, "Block is null").equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Block is ZERO");

		if (Objects.requireNonNull(atom, "Atom is null").equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Atom is ZERO");
		
		if (Objects.requireNonNull(state, "State is null").equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("State is ZERO");

		if (Objects.requireNonNull(powers, "Powers is null").count() == 0)
			throw new IllegalArgumentException("Powers is empty");

		if (Objects.requireNonNull(powers, "Total vote power null").getTotalPower() == 0)
			throw new IllegalArgumentException("Total vote power is ZERO");

		if (Objects.requireNonNull(votes, "Votes is null").size() == 0)
			throw new IllegalArgumentException("Votes is empty");

		this.state = state;
		this.atom = atom;
		this.block = block;
		this.powers = powers;
		
		for (StateVote vote : votes)
			add(vote.getOwner(), vote.getSignature());
	}

	public Hash getBlock()
	{
		return this.block;
	}

	public Hash getAtom()
	{
		return this.atom;
	}

	public Hash getState()
	{
		return this.state;
	}

	@Override
	public <T> T getObject()
	{
		return (T) this.state;
	}

	public VotePowerBloom getVotePowers()
	{
		return this.powers;
	}

	@Override
	public boolean verify(final ECPublicKey signer, final ECSignature signature)
	{
		StateVote vote = new StateVote(getState(), getAtom(), getBlock(), getDecision(), signer);
		return signer.verify(vote.getHash(), signature);
	}
}
