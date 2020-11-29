package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.block.candidate")
public class BlockCandidateMessage extends Message
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private BlockHeader blockHeader;

	BlockCandidateMessage()
	{
		// Serializer only
	}

	public BlockCandidateMessage(BlockHeader blockHeader)
	{
		super();

		this.blockHeader = Objects.requireNonNull(blockHeader, "Block header is null");
	}
	
	public BlockHeader getBlockHeader()
	{
		return this.blockHeader;
	}
}
