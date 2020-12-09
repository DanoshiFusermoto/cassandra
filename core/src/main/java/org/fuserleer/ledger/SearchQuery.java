package org.fuserleer.ledger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.fuserleer.common.Match;
import org.fuserleer.common.Order;
import org.fuserleer.database.Identifier;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.search.query")
public class SearchQuery
{
	public static final int MAX_LIMIT = 1024;
	
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;
	
	@JsonProperty("identifiers")
	@DsonOutput(Output.ALL)
	private List<Identifier> identifiers;

	@JsonProperty("matchon")
	@DsonOutput(Output.ALL)
	private Match matchOn;

	@JsonProperty("container")
	@DsonOutput(Output.ALL)
	private Class<?> container;

	@JsonProperty("order")
	@DsonOutput(Output.ALL)
	private Order order;

	@JsonProperty("offset")
	@DsonOutput(Output.ALL)
	private long offset;

	@JsonProperty("limit")
	@DsonOutput(Output.ALL)
	private int limit;

	private transient int hashCode;
	
	SearchQuery()
	{ 
		super();
	}

	public SearchQuery(final Identifier identifier, final Class<?> container, final Order order)
	{ 
		this(Collections.singletonList(Objects.requireNonNull(identifier, "Identifier is null")), Match.ANY, container, order, -1, SearchQuery.MAX_LIMIT);
	}

	public SearchQuery(final Identifier identifier, final Class<?> container, final Order order, final int limit)
	{ 
		this(Collections.singletonList(Objects.requireNonNull(identifier, "Identifier is null")), Match.ANY, container, order, -1, limit);
	}
	
	public SearchQuery(final Identifier identifier, final Class<?> container, final Order order, final long offset, final int limit)
	{ 
		this(Collections.singletonList(Objects.requireNonNull(identifier, "Identifier is null")), Match.ANY, container, order, offset, limit);
	}

	public SearchQuery(final Identifier identifier, final Match matchOn, final Class<?> container, final Order order, final long offset, final int limit)
	{ 
		this(Collections.singletonList(Objects.requireNonNull(identifier, "Identifier is null")), matchOn, container, order, offset, limit);
	}

	public SearchQuery(final Collection<Identifier> identifiers, final Match matchOn, final Class<?> container, final Order order, final int limit)
	{ 
		this(identifiers, matchOn, container, order, -1, limit);
	}
	
	// TODO can offset limit just be zero for new searches?
	public SearchQuery(final Collection<Identifier> identifiers, final Match matchOn, final Class<?> container, final Order order, final long offset, final int limit)
	{ 
		super();
		
		if (offset < -1)
			throw new IllegalArgumentException("Offset is less than -1");

		if (limit < 0)
			throw new IllegalArgumentException("Limit is negative");

		if (limit > SearchQuery.MAX_LIMIT)
			throw new IllegalArgumentException("Limit is greater than "+MAX_LIMIT);

		this.matchOn = Objects.requireNonNull(matchOn, "Match is null");
		this.identifiers = new ArrayList<Identifier>(Objects.requireNonNull(identifiers));
		this.container = Objects.requireNonNull(container, "Container is null");
		this.order = Objects.requireNonNull(order, "Order is null");
		this.offset = offset;
		this.limit = limit;
		
		if (this.offset < 0)
		{
			if (this.order.equals(Order.ASCENDING) == true)
				this.offset = -1;
			else
				this.offset = Long.MAX_VALUE;
		}
	}

	public List<Identifier> getIdentifiers()
	{
		return this.identifiers;
	}

	public long getOffset()
	{
		return this.offset;
	}

	public Order getOrder()
	{
		return this.order;
	}

	public int getLimit()
	{
		return this.limit;
	}

	public Class<?> getContainer()
	{
		return this.container;
	}
	
	public Match getMatchOn()
	{
		return this.matchOn;
	}
	
	@Override
	public int hashCode() 
	{
		if (this.hashCode != 0)
			return this.hashCode;
		
		this.hashCode = (int) ((this.order.ordinal() + 1) * this.limit * this.offset * this.identifiers.hashCode() * (this.matchOn.ordinal() + 1) * this.container.hashCode());
		return hashCode;
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof SearchQuery)
		{
			if (this.container.equals(((SearchQuery)object).getContainer()) == false)
				return false;

			if (hashCode() != object.hashCode())
				return false;
			
			if (this.identifiers.equals(((SearchQuery)object).getIdentifiers()) == false)
				return false;
			
			if (this.matchOn.equals(((SearchQuery)object).getMatchOn()) == false)
				return false;

			if (this.offset != ((SearchQuery)object).getOffset())
				return false;

			if (this.limit != ((SearchQuery)object).getLimit())
				return false;

			if (this.order.equals(((SearchQuery)object).getOrder()) == false)
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.identifiers.toString()+" "+this.matchOn+" "+this.container+" "+this.offset+" "+this.limit;
	}
}

