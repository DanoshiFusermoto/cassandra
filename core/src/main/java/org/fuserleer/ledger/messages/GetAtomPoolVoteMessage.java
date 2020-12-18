package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.atom.pool.vote.get")
public final class GetAtomPoolVoteMessage extends InventoryMessage
{
	GetAtomPoolVoteMessage()
	{
		super();
	}

	public GetAtomPoolVoteMessage(final Collection<Hash> inventory)
	{
		super(inventory);
	}
}