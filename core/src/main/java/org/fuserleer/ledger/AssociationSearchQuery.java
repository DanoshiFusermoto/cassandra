package org.fuserleer.ledger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Match;
import org.fuserleer.common.Order;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.search.query.association")
public class AssociationSearchQuery extends BasicObject
{
	public static final int MAX_LIMIT = 1024;
	
	@JsonProperty("associations")
	@DsonOutput(Output.ALL)
	private List<Hash> associations;

	@JsonProperty("matchon")
	@DsonOutput(Output.ALL)
	private Match matchOn;

	@JsonProperty("type")
	@DsonOutput(Output.ALL)
	private String type;

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
	
	AssociationSearchQuery()
	{ 
		super();
	}

	public AssociationSearchQuery(final Hash association, final Class<? extends Primitive> type, final Order order)
	{ 
		this(Collections.singletonList(Objects.requireNonNull(association, "Association is null")), Match.ANY, type, order, -1, AssociationSearchQuery.MAX_LIMIT);
	}

	public AssociationSearchQuery(final Hash association, final Class<? extends Primitive> type, final Order order, final int limit)
	{ 
		this(Collections.singletonList(Objects.requireNonNull(association, "Association is null")), Match.ANY, type, order, -1, limit);
	}
	
	public AssociationSearchQuery(final Hash association, final Class<? extends Primitive> type, final Order order, final long offset, final int limit)
	{ 
		this(Collections.singletonList(Objects.requireNonNull(association, "Association is null")), Match.ANY, type, order, offset, limit);
	}

	public AssociationSearchQuery(final Hash association, final Match matchOn, final Class<? extends Primitive> type, final Order order, final long offset, final int limit)
	{ 
		this(Collections.singletonList(Objects.requireNonNull(association, "Association is null")), matchOn, type, order, offset, limit);
	}

	public AssociationSearchQuery(final Collection<Hash> associations, final Match matchOn, final Class<? extends Primitive> type, final Order order, final int limit)
	{ 
		this(associations, matchOn, type, order, -1, limit);
	}
	
	// TODO can offset limit just be zero for new searches?
	public AssociationSearchQuery(final Collection<Hash> associations, final Match matchOn, final Class<? extends Primitive> type, final Order order, final long offset, final int limit)
	{ 
		super();
		
		Numbers.lessThan(offset, -1, "Offset is less than -1");
		Numbers.notNegative(limit, "Limit is negative");
		Numbers.greaterThan(limit, AssociationSearchQuery.MAX_LIMIT, "Limit is greater than "+MAX_LIMIT);

		this.matchOn = Objects.requireNonNull(matchOn, "Match is null");
		this.associations = new ArrayList<Hash>(Objects.requireNonNull(associations));
		this.type = Serialization.getInstance().getIdForClass(Objects.requireNonNull(type, "Type class is null"));
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

	public List<Hash> getAssociations()
	{
		return this.associations;
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

	public Class<? extends Primitive> getType()
	{
		Class<? extends Primitive> type = (Class<? extends Primitive>) Serialization.getInstance().getClassForId(this.type);
		if (type == null)
			throw new IllegalArgumentException(this.type+" is not a known registered class");
		
		return type;
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
		
		this.hashCode = Objects.hash(this.order, this.limit, this.offset, this.associations, this.matchOn, this.type);
		return hashCode;
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof AssociationSearchQuery)
		{
			if (this.type.equals(((AssociationSearchQuery)object).type) == false)
				return false;

			if (hashCode() != object.hashCode())
				return false;
			
			if (this.associations.equals(((AssociationSearchQuery)object).getAssociations()) == false)
				return false;
			
			if (this.matchOn.equals(((AssociationSearchQuery)object).getMatchOn()) == false)
				return false;

			if (this.offset != ((AssociationSearchQuery)object).getOffset())
				return false;

			if (this.limit != ((AssociationSearchQuery)object).getLimit())
				return false;

			if (this.order.equals(((AssociationSearchQuery)object).getOrder()) == false)
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.associations.toString()+" "+this.matchOn+" "+this.type+" "+this.offset+" "+this.limit;
	}
}

