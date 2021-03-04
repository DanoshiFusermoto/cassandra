package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.StateSearchResponse;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.search.response.state")
public class StateSearchResponseMessage extends Message
{
	@JsonProperty("response")
	@DsonOutput(Output.ALL)
	private StateSearchResponse response;

	StateSearchResponseMessage()
	{
		super();
	}

	public StateSearchResponseMessage(final StateSearchResponse response)
	{
		super();

		this.response = Objects.requireNonNull(response, "Response is null");
	}

	public StateSearchResponse getResponse()
	{
		return this.response;
	}
}

