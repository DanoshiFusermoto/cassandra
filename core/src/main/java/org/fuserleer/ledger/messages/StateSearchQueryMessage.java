package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.StateSearchQuery;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.search.query.state")
public class StateSearchQueryMessage extends Message
{
	@JsonProperty("query")
	@DsonOutput(Output.ALL)
	private StateSearchQuery query;

	StateSearchQueryMessage()
	{
		super();
	}

	public StateSearchQueryMessage(final StateSearchQuery query)
	{
		super();

		this.query = Objects.requireNonNull(query, "State query is null");
	}

	public StateSearchQuery getQuery()
	{
		return this.query;
	}
}
