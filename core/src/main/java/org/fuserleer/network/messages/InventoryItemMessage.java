package org.fuserleer.network.messages;

import java.util.Objects;

import org.fuserleer.common.Primitive;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.gossip.inventory.item")
public class InventoryItemMessage extends Message
{
	@JsonProperty("type")
	@DsonOutput(Output.ALL)
	private String type;

	@JsonProperty("data")
	@DsonOutput(Output.ALL)
	private byte[] data;
	
	private transient Primitive item;

	InventoryItemMessage()
	{
		// Serializer only
	}

	public InventoryItemMessage(Primitive item, final Class<? extends Primitive> type) throws SerializationException
	{
		this(item, Serialization.getInstance().getIdForClass(Objects.requireNonNull(type, "Type is null")));
	}
	
	public InventoryItemMessage(final Primitive item, final String type) throws SerializationException
	{
		super();

		Objects.requireNonNull(type, "Type is null");
		if (type.length() == 0)
			throw new IllegalArgumentException("Type is empty");

		Objects.requireNonNull(item, "Item is null");

		this.type = type;
		this.item = item;
		this.data = Serialization.getInstance().toDson(item, Output.WIRE);
	}
	
	public Class<? extends Primitive> getType()
	{
		Class<? extends Primitive> type = (Class<? extends Primitive>) Serialization.getInstance().getClassForId(this.type);
		if (type == null)
			throw new IllegalArgumentException(this.type+" is not a known registered class");
		
		return type;
	}
	
	public <T extends Primitive> T getItem() throws SerializationException
	{
		if (this.item == null)
			this.item = Serialization.getInstance().fromDson(this.data, getType());
		
		return (T) this.item;
	}
}

