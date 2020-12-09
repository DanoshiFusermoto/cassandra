package org.fuserleer.ledger;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.search.response")
public class SearchResponse<T>
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;
	
	@JsonProperty("query")
	@DsonOutput(Output.ALL)
	private SearchQuery query;
	
	@JsonProperty("next_offset")
	@DsonOutput(Output.ALL)
	private long nextOffset;

	@JsonProperty("results")
	@DsonOutput(Output.ALL)
	private Set<T> results;

	@JsonProperty("eor")
	@DsonOutput(Output.ALL)
	private boolean EOR;

	SearchResponse()
	{ 
		super();
	}
	
	public SearchResponse(SearchQuery query)
	{ 
		super();
		
		this.nextOffset = -1;
		
		this.query = Objects.requireNonNull(query);
		this.results = Collections.emptySet();
		this.EOR = true;
	}


	public SearchResponse(SearchQuery query, long nextOffset, Collection<T> results, boolean EOR)
	{ 
		super();
		
		this.nextOffset = nextOffset;
		this.query = Objects.requireNonNull(query);
		this.results = new LinkedHashSet<T>(Objects.requireNonNull(results));
		this.EOR = EOR;
	}

	public SearchQuery getQuery()
	{
		return this.query;
	}
	
	public long getNextOffset()
	{
		return this.nextOffset;
	}

	public boolean isEmpty()
	{
		return this.results.isEmpty();
	}
	
	public boolean isEOR()
	{
		return this.EOR;
	}

	public int size()
	{
		return this.results.size();
	}

	public Collection<T> getResults()
	{
		return this.results;
	}
	
	@Override
	public int hashCode() 
	{
		return (int) (this.query.hashCode() * this.nextOffset) + this.results.hashCode() + (this.EOR == true ? 1 : 0);
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof SearchResponse)
		{
			if (hashCode() != object.hashCode())
				return false;

			if (this.query.equals(((SearchResponse<?>)object).getQuery()) == false)
				return false;

			if (this.nextOffset != ((SearchResponse<?>)object).getNextOffset())
				return false;

			if (this.EOR != ((SearchResponse<?>)object).isEOR())
				return false;
			
			if (this.results.equals(((SearchResponse<?>)object).getResults()) == false)
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.query+" "+this.nextOffset+" "+this.results.size();
	}
}
