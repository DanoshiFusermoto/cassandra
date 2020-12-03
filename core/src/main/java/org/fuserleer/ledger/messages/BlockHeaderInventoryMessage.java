package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.block.header.inv")
public final class BlockHeaderInventoryMessage extends InventoryMessage
{
	BlockHeaderInventoryMessage()
	{
		super();
	}

	public BlockHeaderInventoryMessage(final Collection<Hash> inventory)
	{
		super(inventory);
	}
}
