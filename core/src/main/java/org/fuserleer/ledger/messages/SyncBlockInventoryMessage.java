package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.block.sync.inv")
public final class SyncBlockInventoryMessage extends InventoryMessage
{
	SyncBlockInventoryMessage()
	{
		super();
	}

	public SyncBlockInventoryMessage(final Collection<Hash> inventory)
	{
		super(inventory);
	}
}
