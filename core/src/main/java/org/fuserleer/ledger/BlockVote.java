package org.fuserleer.ledger;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

// FIXME needs to be a Hash, not block header, then use the inventory mechanism to fetch the block header if missing locally
@SerializerId2("ledger.vote.block")
public final class BlockVote extends Vote<Hash>
{
	private BlockVote()
	{
		// SERIALIZER
	}
	
	public BlockVote(final Hash object, final long clock, final ECPublicKey owner)
	{
		super(object, true, clock, owner);
	}

}
