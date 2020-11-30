package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.block.vote.get")
public final class GetBlockVoteMessage extends InventoryMessage
{
	GetBlockVoteMessage()
	{
		super();
	}

	public GetBlockVoteMessage(final Collection<Hash> inventory)
	{
		super(inventory);
	}
}
