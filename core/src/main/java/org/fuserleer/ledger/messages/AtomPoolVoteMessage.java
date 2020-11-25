package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.collections.Bloom;
import org.fuserleer.crypto.SignedObject;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.atom.pool.vote")
public class AtomPoolVoteMessage extends Message
{
	public final static int MAX_VOTES = 1024;

	@JsonProperty("votes")
	@DsonOutput(Output.ALL)
	private SignedObject<Bloom> votes;

	AtomPoolVoteMessage()
	{
		super();
	}

	public AtomPoolVoteMessage(SignedObject<Bloom> votes)
	{
		super();

		Objects.requireNonNull(votes, "Votes is null");
		if (votes.getObject().getExpectedNumberOfElements() > AtomPoolVoteMessage.MAX_VOTES == true)
			throw new IllegalArgumentException("Too many votes cast");
		
		this.votes = votes;
	}

	public SignedObject<Bloom> getVotes()
	{
		return this.votes;
	}
}
