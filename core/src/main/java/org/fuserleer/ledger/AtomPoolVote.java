package org.fuserleer.ledger;

import java.util.List;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.vote.atom.pool")
public final class AtomPoolVote extends Vote<List<Hash>>
{
	private AtomPoolVote()
	{
		// SERIALIZER
	}
	
	public AtomPoolVote(final List<Hash> object, final long clock, final ECPublicKey owner)
	{
		super(object, true, clock, owner);
	}

}
