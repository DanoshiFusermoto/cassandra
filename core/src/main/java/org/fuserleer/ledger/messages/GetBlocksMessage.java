package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.blocks.get")
public class GetBlocksMessage extends Message
{
	@JsonProperty("head")
	@DsonOutput(Output.ALL)
	private Hash head;

	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;

	GetBlocksMessage()
	{
		// Serializer only
	}

	public GetBlocksMessage(final Hash head, final Hash block)
	{
		super();

		this.head = Objects.requireNonNull(head, "Head is null");
		Hash.notZero(head, "Head is ZERO");
		this.block = Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block is ZERO");
	}
	
	public Hash getHead()
	{
		return this.head;
	}

	public Hash getBlock()
	{
		return this.block;
	}
}