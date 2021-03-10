package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.sync.acquired")
public final class SyncAcquiredMessage extends Message
{
	@JsonProperty("head")
	@DsonOutput(Output.ALL)
	private BlockHeader head;
	
	@SuppressWarnings("unused")
	private SyncAcquiredMessage()
	{
		super();
	}
	
	public SyncAcquiredMessage(final BlockHeader head)
	{
		super();
		
		this.head = Objects.requireNonNull(head, "Block head is null");
	}
	
	public BlockHeader getHead()
	{
		return this.head;
	}
}