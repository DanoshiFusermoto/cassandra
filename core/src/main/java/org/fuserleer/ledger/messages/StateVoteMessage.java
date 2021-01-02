package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.StateVote;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.state.vote")
public final class StateVoteMessage extends Message
{
	@JsonProperty("vote")
	@DsonOutput(Output.ALL)
	private StateVote vote;

	StateVoteMessage()
	{
		super();
	}

	public StateVoteMessage(StateVote vote)
	{
		super();

		Objects.requireNonNull(vote, "Vote is null");
		this.vote = vote;
	}

	public StateVote getVote()
	{
		return this.vote;
	}
}
