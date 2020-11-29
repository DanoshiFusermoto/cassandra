package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.ledger.Vote;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

// TODO embed the block vote into the atom pool votes?
@SerializerId2("ledger.messages.block.candidate.vote")
public class BlockCandidateVoteMessage extends Message
{
	@JsonProperty("vote")
	@DsonOutput(Output.ALL)
	private Vote<BlockHeader> vote;

	BlockCandidateVoteMessage()
	{
		super();
	}

	public BlockCandidateVoteMessage(Vote<BlockHeader> vote)
	{
		super();

		Objects.requireNonNull(vote, "Vote is null");
		this.vote = vote;
	}

	public Vote<BlockHeader> getVote()
	{
		return this.vote;
	}
}
