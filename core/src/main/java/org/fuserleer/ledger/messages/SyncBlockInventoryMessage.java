package org.fuserleer.ledger.messages;

import java.util.Collection;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.block.sync.inv")
public final class SyncBlockInventoryMessage extends InventoryMessage
{
	@JsonProperty("response_seq")
	@DsonOutput(Output.ALL)
	private long responseSeq;
	
	SyncBlockInventoryMessage()
	{
		super();
	}

	public SyncBlockInventoryMessage(final long responseSeq)
	{
		super();
		
		this.responseSeq = responseSeq;
	}

	public SyncBlockInventoryMessage(final long responseSeq, final Collection<Hash> inventory)
	{
		super(inventory);
		
		this.responseSeq = responseSeq;
	}
	
	public long getResponseSeq()
	{
		return this.responseSeq;
	}
}
