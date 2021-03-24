package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.block.sync.get")
public class GetSyncBlockMessage extends Message
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;

	GetSyncBlockMessage()
	{
		// Serializer only
	}

	public GetSyncBlockMessage(Hash block)
	{
		super();

		this.block = Objects.requireNonNull(block, "Block is null");
	}
	
	public Hash getBlock()
	{
		return this.block;
	}
}
