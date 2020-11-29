package org.fuserleer.ledger;

import org.fuserleer.collections.Bloom;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.vote.atom.pool")
public final class AtomPoolVote extends Vote<Bloom>
{
	private AtomPoolVote()
	{
		// SERIALIZER
	}
	
	public AtomPoolVote(final Bloom object, final long clock, final ECPublicKey owner)
	{
		super(object, clock, owner);
	}

}
