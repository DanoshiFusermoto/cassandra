package org.fuserleer;

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
	
	public BasicObject()
	{
		super();
	}

	/**
	 * Copy constructor.
	 * @param copy {@link BasicContainer} to copy values from.
	 */
	public BasicObject(BasicObject copy) 
	{
		this();

		this.hash = copy.hash;
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException 
	{
		BasicObject object = (BasicObject) super.clone();
		object.hash = null;
		return object;
	}

	@Override
	public boolean equals(Object o)
	{
		if (o == null) return false;
		if (o == this) return true;

		if (getClass().isInstance(o) && getHash().equals(((BasicObject)o).getHash()))
				return true;

		return super.equals(o);
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
	public int compareTo(Object object)
	{
		if (object instanceof BasicObject)
			return getHash().compareTo(((BasicObject)object).getHash());

		return 0;
	}
}
