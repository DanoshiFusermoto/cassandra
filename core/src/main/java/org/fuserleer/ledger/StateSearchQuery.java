package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.search.query.state")
public class StateSearchQuery extends BasicObject
{
	public static final int MAX_LIMIT = 1024;
	
	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private StateKey<?, ?> key;

	@JsonProperty("type")
	@DsonOutput(Output.ALL)
	private String type;

	StateSearchQuery()
	{ 
		super();
	}

	public StateSearchQuery(final StateKey<?, ?> key, final Class<? extends Primitive> type)
	{ 
		this(key, Serialization.getInstance().getIdForClass(Objects.requireNonNull(type, "Type class is null")));
	}

	public StateSearchQuery(final StateKey<?, ?> key, final String type)
	{ 
		this.key = Objects.requireNonNull(key, "State key is null");
		this.type = Objects.requireNonNull(type, "Type is null");
	}

	public StateKey<?, ?> getKey()
	{
		return this.key;
	}
	
	public Class<? extends Primitive> getType()
	{
		Class<? extends Primitive> type = (Class<? extends Primitive>) Serialization.getInstance().getClassForId(this.type);
		if (type == null)
			throw new IllegalArgumentException(this.type+" is not a known registered class");
		
		return type;
	}

	@Override
	public int hashCode() 
	{
		return Objects.hash(this.key, this.type);
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof StateSearchQuery)
		{
			if (hashCode() != object.hashCode())
				return false;
			
			if (this.key.equals(((StateSearchQuery)object).getKey()) == false)
				return false;
			
			if (this.type.equals(((StateSearchQuery)object).type) == false)
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.key+" "+this.type;
	}
}

