package org.fuserleer.ledger.messages;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.collections.Bloom;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.SignedObject;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.atom.pool.vote.inv")
public class AtomPoolVoteInventoryMessage extends Message
{
	public final static int MAX_INVENTORY = 1024;

	@JsonProperty("inventory")
	@DsonOutput(Output.ALL)
	private Set<Hash> inventory;

	AtomPoolVoteInventoryMessage()
	{
		super();
	}

	public AtomPoolVoteInventoryMessage(final Set<Hash> inventory)
	{
		super();

		Objects.requireNonNull(inventory, "Inventory is null");
		if (inventory.size() > AtomPoolVoteInventoryMessage.MAX_INVENTORY == true)
			throw new IllegalArgumentException("Too many inventory items");
		
		this.inventory = inventory;
	}

	public Set<Hash> getInventory()
	{
		return Collections.unmodifiableSet(this.inventory);
	}
}
