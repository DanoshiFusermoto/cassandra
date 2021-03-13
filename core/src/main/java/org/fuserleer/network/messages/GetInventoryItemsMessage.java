package org.fuserleer.network.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.gossip.inventory.get")
public class GetInventoryItemsMessage extends Message
{
	public final static int MAX_ITEMS = 64;
	
	@JsonProperty("type")
	@DsonOutput(Output.ALL)
	private String type;

	@JsonProperty("items")
	@DsonOutput(Output.ALL)
	private List<Hash> items;

	GetInventoryItemsMessage()
	{
		// Serializer only
	}

	public GetInventoryItemsMessage(final Collection<Hash> items, final Class<? extends Primitive> type)
	{
		this(items, Serialization.getInstance().getIdForClass(Objects.requireNonNull(type, "Type is null")));
	}
	
	public GetInventoryItemsMessage(final Collection<Hash> items, final String type)
	{
		super();

		Objects.requireNonNull(type, "Type is null");
		Numbers.notZero(type.length(), "Type is empty");

		Objects.requireNonNull(items, "Items is null");
		if (items.isEmpty() == true)
			throw new IllegalArgumentException("Items is empty");
		Numbers.greaterThan(items.size(), MAX_ITEMS, "Items is greater than allowed max of "+MAX_ITEMS);

		this.type = type;
		this.items = new ArrayList<Hash>(items);
	}

	public List<Hash> getItems()
	{
		return this.items;
	}
	
	public Class<? extends Primitive> getType()
	{
		Class<? extends Primitive> type = (Class<? extends Primitive>) Serialization.getInstance().getClassForId(this.type);
		if (type == null)
			throw new IllegalArgumentException(this.type+" is not a known registered class");
		
		return type;
	}
}
