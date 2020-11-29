package org.fuserleer.ledger;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.vote.block")
public final class BlockVote extends Vote<BlockHeader>
{
	private BlockVote()
	{
		// SERIALIZER
	}
	
	public BlockVote(final BlockHeader object, final long clock, final ECPublicKey owner)
	{
		super(object, clock, owner);
	}

}
