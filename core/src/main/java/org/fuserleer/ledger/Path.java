package org.fuserleer.ledger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;

import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.path")
public final class Path
{
	public enum Elements
	{
		BLOCK(0),
		ATOM(1),
		PARTICLE(2),
		CERTIFICATE(3);

		private final int id;
		
		private Elements(int id)
		{
			this.id = id;
		}
		
		public int getID()
		{
			return this.id;
		}
		
		@JsonValue
		@Override
		public String toString() 
		{
			return this.name();
		}
		
		public static Elements get(int id)
		{
			for (int e = 0 ; e < Elements.values().length ; e++)
				if (Elements.values()[e].id == id)
					return Elements.values()[e];
			
			return null;
		}
	}
	
	public static Path from(final byte[] bytes) throws IOException
	{
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(bais);
		byte[] endpointBytes = new byte[Hash.BYTES];
		dis.read(endpointBytes);
		Hash endpoint = new Hash(endpointBytes);
		Map<Elements, Hash> elements = new HashMap<Elements, Hash>();
		int numElements = dis.read();
		for (int e = 0 ; e < numElements ; e++)
		{
			Elements element = Elements.get(dis.read());
			boolean isEndpoint = dis.readBoolean();
			if (isEndpoint == false)
			{
				byte[] pathHashBytes = new byte[Hash.BYTES];
				dis.read(pathHashBytes);
				elements.put(element, new Hash(pathHashBytes));
			}
			else
				elements.put(element, endpoint);
		}
		if (elements.isEmpty() == true)
			return new Path(endpoint);
		else
			return new Path(endpoint, elements);
	}
	
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("endpoint")
	@DsonOutput(Output.ALL)
	private Hash endpoint;

	@JsonProperty("elements")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=LinkedHashMap.class)
	private Map<Elements, Hash> elements;
	
	@SuppressWarnings("unused")
	private Path()
	{
		// FOR SERIALIZER
	}

	Path(final Hash endpoint)
	{
		Objects.requireNonNull(endpoint, "Endpoint is null");
		Hash.notZero(endpoint, "Endpoint is ZERO");

		this.elements = Collections.emptyMap();
		this.endpoint = endpoint;
	}
	
	Path(final Hash endpoint, final Map<Elements, Hash> elements)
	{
		Objects.requireNonNull(endpoint, "Endpoint is null");
		Hash.notZero(endpoint, "Endpoint is ZERO");

		Objects.requireNonNull(elements, "Path elements is null");
		if (elements.isEmpty() == true)
			throw new IllegalArgumentException("Path elements is empty");
		
		this.elements = new TreeMap<>(elements);
		this.endpoint = endpoint;
	}
	
	void validate()
	{
		if (this.elements.containsKey(Elements.BLOCK) == false)
			throw new IllegalStateException("Block path element must always be present");

		if (this.elements.containsKey(Elements.CERTIFICATE) == true && this.elements.containsKey(Elements.ATOM) == false)
			throw new IllegalStateException("Atom path element must be present with certificate path element");

		if (this.elements.containsKey(Elements.PARTICLE) == true && this.elements.containsKey(Elements.ATOM) == false)
			throw new IllegalStateException("Atom path element must be present with block path element");
	}
	
	public final Hash endpoint()
	{
		return this.endpoint;
	}
	
	public final Hash get(final Elements element)
	{
		Objects.requireNonNull(element, "Path element is null");
		return this.elements.get(element);
	}

	public final void add(final Elements element, final Hash object)
	{
		Objects.requireNonNull(element, "Path element is null");
		Objects.requireNonNull(object, "Object hash is null");
		Hash.notZero(object, "Element object hash is ZERO");
		
		this.elements.put(element, object);
	}

	@Override
	public boolean equals(Object other)
	{
		if (other == null)
			return false;
		
		if (other == this)
			return true;
		
		if (other instanceof Path)
		{
			if (this.elements.equals(((Path)other).elements) == false)
				return false;
			
			if (this.endpoint.equals(((Path)other).endpoint) == false)
				return false;
				
			return true;
		}
		
		return false;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(this.endpoint, this.elements);
	}

	@Override
	public String toString()
	{
		return this.endpoint+" <> "+this.elements.toString();
	}
	
	public final byte[] toByteArray() throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		dos.write(this.endpoint.toByteArray());
		dos.write(this.elements.size());
		for (Entry<Elements, Hash> element : this.elements.entrySet())
		{
			dos.write(element.getKey().getID());
			if (element.getValue().equals(this.endpoint) == false)
			{
				dos.writeBoolean(false);
				dos.write(element.getValue().toByteArray());
			}
			else
				dos.writeBoolean(true);
		}
		return baos.toByteArray();
	}
}
