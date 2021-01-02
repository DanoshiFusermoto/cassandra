package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.ParticleVote;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.particle.vote")
public final class ParticleVoteMessage extends Message
{
	@JsonProperty("vote")
	@DsonOutput(Output.ALL)
	private ParticleVote vote;

	ParticleVoteMessage()
	{
		super();
	}

	public ParticleVoteMessage(ParticleVote vote)
	{
		super();

		Objects.requireNonNull(vote, "Vote is null");
		this.vote = vote;
	}

	public ParticleVote getVote()
	{
		return this.vote;
	}
}
