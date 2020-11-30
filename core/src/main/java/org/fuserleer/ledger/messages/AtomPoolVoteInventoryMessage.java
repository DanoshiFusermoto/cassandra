package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.atom.pool.vote.inv")
public final class AtomPoolVoteInventoryMessage extends InventoryMessage
{
	AtomPoolVoteInventoryMessage()
	{
		super();
	}

	public AtomPoolVoteInventoryMessage(final Collection<Hash> inventory)
	{
		super(inventory);
	}
}
