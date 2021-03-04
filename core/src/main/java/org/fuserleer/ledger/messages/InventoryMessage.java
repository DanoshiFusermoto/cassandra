package org.fuserleer.ledger.messages;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.crypto.Hash;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public abstract class InventoryMessage extends Message
{
	public final static int MAX_INVENTORY = 64;

	@JsonProperty("inventory")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=LinkedHashSet.class)
	private Set<Hash> inventory;

	InventoryMessage()
	{
		super();
	}

	InventoryMessage(final Collection<Hash> inventory)
	{
		super();

		Objects.requireNonNull(inventory, "Inventory is null");
		if (inventory.isEmpty() == true)
			throw new IllegalArgumentException("Inventory is empty");
		if (inventory.size() > InventoryMessage.MAX_INVENTORY == true)
			throw new IllegalArgumentException("Too many inventory items");
		
		this.inventory = new LinkedHashSet<Hash>(inventory);
	}

	public Set<Hash> getInventory()
	{
		return Collections.unmodifiableSet(this.inventory);
	}
}
