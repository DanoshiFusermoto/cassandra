package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.block.sync.get")
public class GetSyncBlockMessage extends InventoryMessage
{
	GetSyncBlockMessage()
	{
		// Serializer only
	}

	public GetSyncBlockMessage(Collection<Hash> blocks)
	{
		super(blocks);
	}
}

