package org.fuserleer.ledger;

import java.util.Objects;

import org.bouncycastle.util.Arrays;
import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.search.query.result")
public final class SearchResult extends BasicObject
{
	@JsonProperty("commit")
	@DsonOutput(Output.ALL)
	private Commit commit;
	
	@JsonProperty("type")
	@DsonOutput(Output.ALL)
	private String type;

	@JsonProperty("data")
	@DsonOutput(Output.ALL)
	private byte[] data;
	
	private transient Primitive primitive;

	private SearchResult()
	{
		// FOR SERIALIZER //
		super();
	}
	
	public SearchResult(final Commit commit, final Primitive primitive, final Class<? extends Primitive> type) throws SerializationException
	{ 
		this(commit, primitive, Serialization.getInstance().getIdForClass(Objects.requireNonNull(type, "Type is null")));
	}
	
	public SearchResult(final Commit commit, final Primitive primitive, final String type) throws SerializationException
	{ 
		super();
		
		this.primitive = Objects.requireNonNull(primitive, "Primitive is null");
		this.type = Objects.requireNonNull(type, "Type is null");
		this.commit = Objects.requireNonNull(commit, "Commit is null");
		this.data = Serialization.getInstance().toDson(primitive, Output.WIRE);
	}

	public Commit getCommit()
	{
		return this.commit;
	}

	public Class<? extends Primitive> getType()
	{
		Class<? extends Primitive> type = (Class<? extends Primitive>) Serialization.getInstance().getClassForId(this.type);
		if (type == null)
			throw new IllegalArgumentException(this.type+" is not a known registered class");
		
		return type;
	}

	public <T> T getPrimitive() throws SerializationException
	{
		if (this.primitive == null)
		{
			if (this.data == null)
				return null;

			this.primitive = Serialization.getInstance().fromDson(this.data, getType());
		}
		
		return (T) this.primitive;
	}

	@Override
	public int hashCode() 
	{
		return Objects.hash(this.commit, this.primitive, this.type);
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof SearchResult)
		{
			if (hashCode() != object.hashCode())
				return false;

			if (this.commit != null && this.commit.equals(((SearchResult)object).getCommit()))
				return false;

			if (this.type != null && this.type.equals(((SearchResult)object).type))
				return false;

			if (this.data != null && (((SearchResult)object).data == null || Arrays.areEqual(this.data, ((SearchResult)object).data) == false))
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.type+" "+(this.commit != null ? this.commit.toString() : "");
	}

}
