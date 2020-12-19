package org.fuserleer.ledger;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.vote.particle")
public final class ParticleVote extends Vote<Hash>
{
	private ParticleVote()
	{
		// SERIALIZER
	}
	
	public ParticleVote(final Hash object, final boolean decision, final long clock, final ECPublicKey owner)
	{
		super(object, decision, clock, owner);
	}

}
