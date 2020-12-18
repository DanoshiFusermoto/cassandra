package org.fuserleer.database;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.bouncycastle.util.Arrays;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.Serialization;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("database.indexable")
public final class Indexable implements Hashable
{
	public final static int DEFAULT_COMPRESSED_INDEXABLE_BYTES = 16;
	public final static int MIN_COMPRESSED_INDEXABLE_BYTES = 4;
	public final static int MAX_COMPRESSED_INDEXABLE_BYTES = Hash.BYTES;

	public static byte[] toByteArray(Hash key, Class<? extends IndexablePrimitive> container)
	{
		return toByteArray(key, container, Indexable.DEFAULT_COMPRESSED_INDEXABLE_BYTES);
	}

	public static byte[] toByteArray(Hash key, Class<? extends IndexablePrimitive> container, int size) 
	{
		if (size < MIN_COMPRESSED_INDEXABLE_BYTES)
			throw new IllegalArgumentException("Size is less than minimum "+Indexable.MIN_COMPRESSED_INDEXABLE_BYTES);

		if (size > MAX_COMPRESSED_INDEXABLE_BYTES)
			throw new IllegalArgumentException("Size is greater than maximum "+Indexable.MAX_COMPRESSED_INDEXABLE_BYTES);
		
		Objects.requireNonNull(key, "Key is null");
		if (key.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Key is ZERO");

		Objects.requireNonNull(container, "Container is null");
		
		String IDForContainer = Serialization.getInstance().getIdForClass(container);
		if (IDForContainer == null)
			throw new IllegalStateException("No class ID found for "+container);

		Hash containerizedKey = new Hash(IDForContainer.getBytes(StandardCharsets.UTF_8), key.toByteArray(), Mode.STANDARD);
		byte[] digest = containerizedKey.toByteArray();
		return Arrays.copyOf(digest, size);
	}

	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;
	
	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private Hash key;
	
	@JsonProperty("container")
	@DsonOutput(Output.ALL)
	private Class<? extends IndexablePrimitive> container;

	private transient Hash hash;
	private transient byte[] bytes;
	
	public static Indexable from(Object object, Class<? extends IndexablePrimitive> container)
	{
		Objects.requireNonNull(object, "Object is null");
		Objects.requireNonNull(container, "Container is null");
		
		if (Hashable.class.isAssignableFrom(object.getClass()) == true)
			return new Indexable(((Hashable) object).getHash(), container);

		if (Hash.class.isAssignableFrom(object.getClass()) == true)
			return new Indexable((Hash) object, container);

		if (String.class.isAssignableFrom(object.getClass()) == true)
			return new Indexable((String) object, container);
		
		if (byte[].class.isAssignableFrom(object.getClass()) == true)
			return new Indexable((byte[]) object, container);
		
		throw new IllegalArgumentException("Objects of type "+object.getClass()+" not supported for Indexables");
	}

	Indexable()
	{ 
		super();
	}

	private Indexable(String key, Class<? extends IndexablePrimitive> container)
	{
		this();
		
		if (Objects.requireNonNull(key, "Key is null").isEmpty() == true)
			throw new IllegalArgumentException("Key is empty");
		
		byte[] bytes = key.toLowerCase().getBytes(StandardCharsets.UTF_8);
		this.key = new Hash(bytes, Mode.STANDARD);
		if (this.key.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Key is ZERO");

		this.container = Objects.requireNonNull(container);
		this.bytes = Indexable.toByteArray(this.key, container);
	}

	private Indexable(byte[] key, Class<? extends IndexablePrimitive> container)
	{
		this();
		
		if (Objects.requireNonNull(key, "Key is null").length == 0)
			throw new IllegalArgumentException("Key is empty");

		this.key = new Hash(key, Mode.STANDARD);
		if (this.key.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Key is ZERO");

		this.container = Objects.requireNonNull(container);
		this.bytes = Indexable.toByteArray(this.key, container);
	}

	private Indexable(Hash key, Class<? extends IndexablePrimitive> container)
	{ 
		this();
		
		this.key = Objects.requireNonNull(key, "Key is null");
		if (this.key.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Key is ZERO");

		this.container = Objects.requireNonNull(container);
		this.bytes = Indexable.toByteArray(key, container);
	}

	@Override
	public synchronized Hash getHash() 
	{
	    if (this.hash == null) 
	    {
	    	String IDForContainer = Serialization.getInstance().getIdForClass(this.container);
	    	if (IDForContainer == null)
	    		throw new IllegalStateException("No class ID found for " + this.container); 
	      
	    	this.hash = new Hash(IDForContainer.getBytes(StandardCharsets.UTF_8), this.key.toByteArray(), Hash.Mode.STANDARD);
	    } 
	    
	    return this.hash;
	}
	
	public Hash getKey()
	{
		return this.key;
	}

	public Class<? extends IndexablePrimitive> getContainer()
	{
		return this.container;
	}
	
	@Override
	public int hashCode() 
	{
		return getHash().hashCode();
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof Indexable)
		{
			if (this.container.equals(((Indexable)object).getContainer()) == false)
				return false;

			if (getHash().equals(((Indexable)object).getHash()) == false)
				return false;
			
			if (this.key.equals(((Indexable)object).getKey()) == false)
				return false;
			
			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.key.toString()+" "+this.container;
	}
	
	public byte[] toByteArray()
	{
		if (this.bytes == null)
			this.bytes = Indexable.toByteArray(this.key, this.container);
		
		return this.bytes;
	}
	
	public byte[] toByteArray(int size) 
	{
		return Indexable.toByteArray(this.key, this.container, size);
	}
}
