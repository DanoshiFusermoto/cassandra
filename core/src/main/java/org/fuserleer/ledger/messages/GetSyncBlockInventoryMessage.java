package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.block.sync.inv.get")
public final class GetSyncBlockInventoryMessage extends Message
{
	@JsonProperty("head")
	@DsonOutput(Output.ALL)
	private Hash head;
	
	GetSyncBlockInventoryMessage()
	{
		super();
	}

	public GetSyncBlockInventoryMessage(final Hash head)
	{
		this();
		
		this.head = Objects.requireNonNull(head, "Block head hash is null");
		if (head.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Head hash is ZERO");
	}
	
	public Hash getHead()
	{
		return this.head;
	}
}