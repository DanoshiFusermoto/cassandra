package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.BlockVote;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

// TODO embed the block vote into the atom pool votes?
@SerializerId2("ledger.messages.block.vote")
public final class BlockVoteMessage extends Message
{
	@JsonProperty("vote")
	@DsonOutput(Output.ALL)
	private BlockVote vote;

	BlockVoteMessage()
	{
		super();
	}

	public BlockVoteMessage(BlockVote vote)
	{
		super();

		Objects.requireNonNull(vote, "Vote is null");
		this.vote = vote;
	}

	public BlockVote getVote()
	{
		return this.vote;
	}
}
