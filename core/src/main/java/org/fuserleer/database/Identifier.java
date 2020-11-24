package org.fuserleer.database;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("database.identifier")
public class Identifier implements Comparable<Identifier>
{
	public final static int DEFAULT_COMPRESSED_IDENTIFIER_BYTES = 16;
	public final static int MIN_COMPRESSED_IDENTIFIER_BYTES = 4;
	public final static int MAX_COMPRESSED_IDENTIFIER_BYTES = Hash.BYTES;

	public static byte[] toByteArray(Hash key) 
	{
		return toByteArray(key, Identifier.DEFAULT_COMPRESSED_IDENTIFIER_BYTES);
	}
	
	public static byte[] toByteArray(Hash key, int size) 
	{
		if (size < MIN_COMPRESSED_IDENTIFIER_BYTES)
			throw new IllegalArgumentException("Size is less than minimum "+Identifier.MIN_COMPRESSED_IDENTIFIER_BYTES);

		if (size > MAX_COMPRESSED_IDENTIFIER_BYTES)
			throw new IllegalArgumentException("Size is greater than maximum "+Identifier.MAX_COMPRESSED_IDENTIFIER_BYTES);
		
		Objects.requireNonNull(key, "Key is null");
		if (key.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Key is ZERO");
		
		byte[] bytes = new byte[size];
		System.arraycopy(key.toByteArray(), 0, bytes, 0, size);
		return bytes;
	}

	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;
	
	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private Hash key;
	
	private transient int hashCode;
	
	Identifier()
	{ 
		super();
	}

	public static Identifier from(Object object)
	{
		Objects.requireNonNull(object, "Object is null");

		if (Hashable.class.isAssignableFrom(object.getClass()) == true)
			return new Identifier(((Hashable) object).getHash());
		
		if (Hash.class.isAssignableFrom(object.getClass()) == true)
			return new Identifier((Hash) object);

		if (String.class.isAssignableFrom(object.getClass()) == true)
			return new Identifier((String) object);
		
		if (byte[].class.isAssignableFrom(object.getClass()) == true)
			return new Identifier((byte[]) object);
		
		throw new IllegalArgumentException("Objects of type "+object.getClass()+" not supported for identifiers");
	}

	public static Identifier from(Identifier ... identifiers)
	{
		if (Objects.requireNonNull(identifiers, "Identifiers is null").length == 0)
			throw new IllegalArgumentException("Identifiers is empty");

		return from(Arrays.asList(identifiers));
	}
	
	public static Identifier from(List<Identifier> identifiers)
	{
		if (Objects.requireNonNull(identifiers, "Identifiers is null").isEmpty() == true)
			throw new IllegalArgumentException("Identifiers is empty");

		List<Identifier> sortedIdentifiers = new ArrayList<Identifier>(identifiers);
		Collections.sort(sortedIdentifiers);
		byte[] concatenatedData = new byte[Hash.BYTES * sortedIdentifiers.size()];
		int concatenatedOffset = 0;
		for (Identifier identifier : sortedIdentifiers)
		{
			System.arraycopy(identifier.key.toByteArray(), 0, concatenatedData, concatenatedOffset, Hash.BYTES);
			concatenatedOffset += Hash.BYTES;
		}

		Hash concatenatedKey = new Hash(concatenatedData, Mode.STANDARD);
		return new Identifier(concatenatedKey);
	}
	
	// FIXME temp change to public from package-private
	private Identifier(byte[] key)
	{ 
		super();
		
		if (Objects.requireNonNull(key, "Key is null").length == 0)
			throw new IllegalArgumentException("Key is empty");

		this.key = new Hash(key, Mode.STANDARD);
		if (this.key.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Key is ZERO");
	}

	private Identifier(Hash key)
	{ 
		super();
		
		this.key = Objects.requireNonNull(key, "Key is null");
		if (this.key.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Key is ZERO");
	}
	
	private Identifier(String key) 
	{
		if (Objects.requireNonNull(key, "Key is null").isEmpty() == true)
			throw new IllegalArgumentException("Key is empty");
		
		byte[] bytes = key.toLowerCase().getBytes(StandardCharsets.UTF_8);
		this.key = new Hash(bytes, Mode.STANDARD);
		if (this.key.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Key is ZERO");
	}
	
	public Hash getKey()
	{
		return this.key;
	}

	@Override
	public int hashCode() 
	{
		if (this.hashCode != 0)
			return this.hashCode;
		
		this.hashCode = this.key.hashCode();
		return hashCode;
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof Identifier)
		{
			if (hashCode() != object.hashCode())
				return false;
			
			if (this.key.equals(((Identifier)object).getKey()) == false)
				return false;
			
			return true;
		}
		
		return false;
	}

	@Override
	public int compareTo(Identifier other)
	{
		return this.key.compareTo(other.key);
	}

	@Override
	public String toString() 
	{
		return this.key.toString();
	}
	
	public byte[] toByteArray()
	{
		return toByteArray(Identifier.DEFAULT_COMPRESSED_IDENTIFIER_BYTES);
	}
	
	public byte[] toByteArray(int size) 
	{
		if (size < MIN_COMPRESSED_IDENTIFIER_BYTES)
			throw new IllegalArgumentException("Size is less than minimum "+Identifier.MIN_COMPRESSED_IDENTIFIER_BYTES);

		if (size > MAX_COMPRESSED_IDENTIFIER_BYTES)
			throw new IllegalArgumentException("Size is greater than maximum "+Identifier.MAX_COMPRESSED_IDENTIFIER_BYTES);

		byte[] bytes = new byte[size];
		System.arraycopy(this.key.toByteArray(), 0, bytes, 0, size);
		return bytes;
	}
}
