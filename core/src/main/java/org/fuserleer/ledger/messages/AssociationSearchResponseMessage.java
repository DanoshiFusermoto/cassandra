package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.AssociationSearchResponse;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.search.response.association")
public class AssociationSearchResponseMessage extends Message
{
	@JsonProperty("response")
	@DsonOutput(Output.ALL)
	private AssociationSearchResponse response;

	AssociationSearchResponseMessage()
	{
		super();
	}

	public AssociationSearchResponseMessage(final AssociationSearchResponse response)
	{
		super();

		this.response = Objects.requireNonNull(response, "Response is null");
	}

	public AssociationSearchResponse getResponse()
	{
		return this.response;
	}
}

