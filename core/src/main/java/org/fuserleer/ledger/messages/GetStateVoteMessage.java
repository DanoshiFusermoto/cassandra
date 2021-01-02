package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.state.vote.get")
public final class GetStateVoteMessage extends InventoryMessage
{
	GetStateVoteMessage()
	{
		super();
	}

	public GetStateVoteMessage(final Collection<Hash> inventory)
	{
		super(inventory);
	}
}
