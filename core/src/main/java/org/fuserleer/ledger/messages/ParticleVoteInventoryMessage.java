package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.particle.vote.inv")
public final class ParticleVoteInventoryMessage extends InventoryMessage
{
	ParticleVoteInventoryMessage()
	{
		super();
	}

	public ParticleVoteInventoryMessage(final Collection<Hash> inventory)
	{
		super(inventory);
	}
}
