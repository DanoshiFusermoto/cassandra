package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.AtomPoolVote;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.atom.pool.vote")
public final class AtomPoolVoteMessage extends Message
{
	public final static int MAX_VOTES = 1024;

	@JsonProperty("votes")
	@DsonOutput(Output.ALL)
	private AtomPoolVote votes;

	AtomPoolVoteMessage()
	{
		super();
	}

	public AtomPoolVoteMessage(AtomPoolVote votes)
	{
		super();

		Objects.requireNonNull(votes, "Votes is null");
		if (votes.getObject().getExpectedNumberOfElements() > AtomPoolVoteMessage.MAX_VOTES == true)
			throw new IllegalArgumentException("Too many votes cast");
		
		this.votes = votes;
	}

	public AtomPoolVote getVotes()
	{
		return this.votes;
	}
}
