package org.fuserleer.ledger.atoms;

import java.util.Collection;
import java.util.Objects;

import org.fuserleer.crypto.Certificate;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignature;
import org.fuserleer.crypto.Hash;
import org.fuserleer.ledger.ParticleVote;
import org.fuserleer.ledger.VotePowerBloom;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atoms.particles.certificate")
public final class ParticleCertificate extends Certificate
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;

	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;
	
	@JsonProperty("particle")
	@DsonOutput(Output.ALL)
	private Hash particle;

	@JsonProperty("powers")
	@DsonOutput(Output.ALL)
	private VotePowerBloom powers;
	
	private ParticleCertificate()
	{
		super();
		
		// FOR SERIALIZER //
	}

	public ParticleCertificate(final Hash particle, final Hash atom, final Hash block, final boolean decision, final VotePowerBloom powers, final Collection<ParticleVote> votes) throws CryptoException
	{
		super(decision);
		
		if (Objects.requireNonNull(atom, "Block is null").equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Block is ZERO");

		if (Objects.requireNonNull(atom, "Atom is null").equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Atom is ZERO");
		
		if (Objects.requireNonNull(particle, "Particle is null").equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Particle is ZERO");

		if (Objects.requireNonNull(powers, "Powers is null").count() == 0)
			throw new IllegalArgumentException("Powers is empty");

		if (Objects.requireNonNull(powers, "Total vote power null").getTotalPower() == 0)
			throw new IllegalArgumentException("Total vote power is ZERO");

		if (Objects.requireNonNull(votes, "Votes is null").size() == 0)
			throw new IllegalArgumentException("Votes is empty");

		this.particle = particle;
		this.atom = atom;
		this.block = block;
		this.powers = powers;
		
		for (ParticleVote vote : votes)
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

	public Hash getParticle()
	{
		return this.particle;
	}

	@Override
	public <T> T getObject()
	{
		return (T) this.particle;
	}

	public VotePowerBloom getVotePowers()
	{
		return this.powers;
	}

	@Override
	public boolean verify(final ECPublicKey signer, final ECSignature signature)
	{
		ParticleVote vote = new ParticleVote(getParticle(), getAtom(), getBlock(), getDecision(), signer);
		return signer.verify(vote.getHash(), signature);
	}
}
