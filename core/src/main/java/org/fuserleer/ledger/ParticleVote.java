package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.vote.particle")
public final class ParticleVote extends Vote<Hash>
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;

	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;

	private ParticleVote()
	{
		// SERIALIZER
	}
	
	public ParticleVote(final Hash particle, final Hash atom, final Hash block, final boolean decision, final ECPublicKey owner)
	{
		super(particle, decision, owner);
		
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
	
	public Hash getParticle()
	{
		return this.getObject();
	}
}
