package org.fuserleer.ledger.messages;

import java.util.Objects;

import org.fuserleer.ledger.AssociationSearchQuery;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.search.query.association")
public class AssociationSearchQueryMessage extends Message
{
	@JsonProperty("query")
	@DsonOutput(Output.ALL)
	private AssociationSearchQuery query;

	AssociationSearchQueryMessage()
	{
		super();
	}

	public AssociationSearchQueryMessage(final AssociationSearchQuery query)
	{
		super();

		this.query = Objects.requireNonNull(query, "Association query is null");
	}

	public AssociationSearchQuery getQuery()
	{
		return this.query;
	}
}
