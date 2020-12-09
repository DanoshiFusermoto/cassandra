package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.Block;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.block")
public class BlockMessage extends Message
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Block block;

	BlockMessage()
	{
		// Serializer only
	}

	public BlockMessage(Block block)
	{
		super();

		this.block = Objects.requireNonNull(block, "Block is null");
	}
	
	public Block getBlock()
	{
		return this.block;
	}
}

