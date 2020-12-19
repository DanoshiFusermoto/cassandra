package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.particle.vote.get")
public final class GetParticleVoteMessage extends InventoryMessage
{
	GetParticleVoteMessage()
	{
		super();
	}

	public GetParticleVoteMessage(final Collection<Hash> inventory)
	{
		super(inventory);
	}
}
