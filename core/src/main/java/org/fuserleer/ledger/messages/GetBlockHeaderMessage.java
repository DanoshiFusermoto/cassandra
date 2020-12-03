package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.block.header.get")
public final class GetBlockHeaderMessage extends InventoryMessage
{
	GetBlockHeaderMessage()
	{
		super();
	}

	public GetBlockHeaderMessage(final Collection<Hash> inventory)
	{
		super(inventory);
	}
}