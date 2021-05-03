package org.fuserleer.network.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.fuserleer.common.Primitive;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("gossip.inventory.items")
public class InventoryItemsMessage extends Message
{
	public static final int TRANSMIT_AT_SIZE = 1<<20;	// Transmit if size is 1M or greater
	
	@JsonProperty("type")
	@DsonOutput(Output.ALL)
	private String type;

	@JsonProperty("data")
	@DsonOutput(Output.ALL)
	private List<byte[]> data;
	
	private transient List<Primitive> items;

	InventoryItemsMessage()
	{
		// Serializer only
	}

	public InventoryItemsMessage(final Class<? extends Primitive> type) throws SerializationException
	{
		this(Serialization.getInstance().getIdForClass(Objects.requireNonNull(type, "Type is null")));
	}
	
	public InventoryItemsMessage(final String type) throws SerializationException
	{
		super();

		Objects.requireNonNull(type, "Type is null");
		Numbers.isZero(type.length(), "Type is empty");

		this.type = type;
		this.data = new ArrayList<byte[]>();
	}
	
	public void add(final Primitive item) throws SerializationException
	{
		this.data.add(Serialization.getInstance().toDson(item, Output.WIRE));
	}
	
	public int size()
	{
		return this.data.stream().collect(Collectors.summingInt(d -> d.length));
	}
	
	public Class<? extends Primitive> getType()
	{
		Class<? extends Primitive> type = (Class<? extends Primitive>) Serialization.getInstance().getClassForId(this.type);
		if (type == null)
			throw new IllegalArgumentException(this.type+" is not a known registered class");
		
		return type;
	}
	
	public <T extends Primitive> Collection<T> getItems() throws SerializationException
	{
		if (this.items == null)
		{
			this.items = new ArrayList<Primitive>();
			
			for (byte[] bytes : this.data)
			{
				Primitive item = Serialization.getInstance().fromDson(bytes, getType());
				this.items.add((T) item);
			}
		}
		
		return (Collection<T>) this.items;
	}
}
