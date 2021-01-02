package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.state.vote.inv")
public final class StateVoteInventoryMessage extends InventoryMessage
{
	StateVoteInventoryMessage()
	{
		super();
	}

	public StateVoteInventoryMessage(final Collection<Hash> inventory)
	{
		super(inventory);
	}
}
