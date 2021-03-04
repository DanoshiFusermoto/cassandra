package org.fuserleer;

import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerConstants;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class BasicObject implements Cloneable, Comparable<Object>, Hashable
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	private	transient Hash	hash = Hash.ZERO;
	
	protected BasicObject()
	{
		super();
	}

	@Override
	protected Object clone() throws CloneNotSupportedException 
	{
		BasicObject object = (BasicObject) super.clone();
		object.hash = null;
		return object;
	}

	@Override
	public boolean equals(final Object other)
	{
		if (other == null) return false;
		if (other == this) return true;

		if (getClass().isInstance(other) && getHash().equals(((BasicObject)other).getHash()))
				return true;

		return super.equals(other);
	}

	@Override
	public int hashCode()
	{
		return getHash().hashCode();
	}

	// HASHABLE //
	@Override
	@JsonProperty("hash")
	@DsonOutput(Output.API)
	public synchronized Hash getHash()
	{
		try
		{
			if (this.hash == null || this.hash.equals(Hash.ZERO))
				this.hash = computeHash();

			return this.hash;
		}
		catch (Exception e)
		{
			throw new RuntimeException("Error generating hash: " + e, e);
		}
	}

	protected synchronized Hash computeHash()
	{
		try
		{
			byte[] hashBytes = Serialization.getInstance().toDson(this, Output.HASH);
			return new Hash(hashBytes, Mode.DOUBLE);
		}
		catch (Exception e)
		{
			throw new RuntimeException("Error generating hash: " + e, e);
		}
	}
	
	@Override
	public String toString()
	{
		return this.getClass().toString()+": "+getHash().toString();
	}

	@Override
	public int compareTo(final Object object)
	{
		Objects.requireNonNull(object, "Object to compare to is null");
		
		if (object instanceof BasicObject)
			return getHash().compareTo(((BasicObject)object).getHash());

		return 0;
	}
}
