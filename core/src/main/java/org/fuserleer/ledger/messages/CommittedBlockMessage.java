package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.block.committed")
public class CommittedBlockMessage extends Message
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private BlockHeader blockHeader;

	CommittedBlockMessage()
	{
		// Serializer only
	}

	public CommittedBlockMessage(BlockHeader blockHeader)
	{
		super();

		this.blockHeader = Objects.requireNonNull(blockHeader, "Block header is null");
		if (this.blockHeader.getCertificate() == null)
			throw new IllegalArgumentException("Expected block header with certificate");
	}
	
	public BlockHeader getBlockHeader()
	{
		return this.blockHeader;
	}
}

