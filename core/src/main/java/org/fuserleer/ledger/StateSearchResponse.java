package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.search.response.state")
public class StateSearchResponse extends BasicObject
{
	@JsonProperty("query")
	@DsonOutput(Output.ALL)
	private StateSearchQuery query;

	@JsonProperty("result")
	@DsonOutput(Output.ALL)
	private SearchResult result;
	
	StateSearchResponse()
	{ 
		super();
	}
	
	public StateSearchResponse(final StateSearchQuery query)
	{ 
		super();
		
		this.query = Objects.requireNonNull(query, "State search query is null");
	}

	public StateSearchResponse(final StateSearchQuery query, final Commit commit, final Primitive primitive, final Class<? extends Primitive> type) throws SerializationException
	{ 
		this(query, commit, primitive, Serialization.getInstance().getIdForClass(Objects.requireNonNull(type, "Type is null")));
	}
	
	public StateSearchResponse(final StateSearchQuery query, final Commit commit, final Primitive primitive, final String type) throws SerializationException
	{ 
		this(query, new SearchResult(commit, primitive, type));
	}

	public StateSearchResponse(final StateSearchQuery query, final SearchResult result) throws SerializationException
	{ 
		super();
		
		this.query = Objects.requireNonNull(query, "State search query is null");
		this.result = Objects.requireNonNull(result, "Result is null");
	}

	public StateSearchQuery getQuery()
	{
		return this.query;
	}
	
	public SearchResult getResult()
	{
		return this.result;
	}

	@Override
	public int hashCode() 
	{
		if (this.result != null)
			return Objects.hash(this.query, this.result);
		
		return this.query.hashCode();
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof StateSearchResponse)
		{
			if (hashCode() != object.hashCode())
				return false;

			if (this.query.equals(((StateSearchResponse)object).getQuery()) == false)
				return false;

			if (this.result != null && this.result.equals(((StateSearchResponse)object).getResult()))
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.query+" "+(this.result != null ? this.result.toString() : "");
	}
}
