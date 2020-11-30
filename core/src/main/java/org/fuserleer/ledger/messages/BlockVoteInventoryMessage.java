package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.block.vote.inv")
public final class BlockVoteInventoryMessage extends InventoryMessage
{
	BlockVoteInventoryMessage()
	{
		super();
	}

	public BlockVoteInventoryMessage(final Collection<Hash> inventory)
	{
		super(inventory);
	}
}
