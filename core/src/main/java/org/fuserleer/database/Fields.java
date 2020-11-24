package org.fuserleer.database;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.SerializerConstants;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("database.fields")
public class Fields implements Iterable<Field>, Primitive // StatePrimitive TODO not sure if this needs to be state primitive as it doesn't affect state but is a representation of output 
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("fields")
	@DsonOutput(Output.ALL)
	private LinkedHashMap<Hash, Field> fields;
	
	public Fields()
	{
		super();
	}
	
	public Fields(Fields other)
	{
		super();
		
		this.fields = new LinkedHashMap<Hash, Field>(other.fields);
	}

	public Fields(Collection<Field> fields)
	{
		super();
		
		this.fields = new LinkedHashMap<Hash, Field>();
		for (Field field : Objects.requireNonNull(fields))
			this.fields.put(field.getPointer(), field);
	}

	public synchronized Set<Field> clear()
	{
		if (this.fields == null)
			this.fields = new LinkedHashMap<Hash, Field>();
		
		Set<Field> fields = new HashSet<Field>(this.fields.values());
		this.fields.clear();
		return fields;
	}
	
	public synchronized Field get(Indexable scope, String name)
	{
		if (this.fields == null)
			this.fields = new LinkedHashMap<Hash, Field>();

		return this.fields.get(Field.pointer(scope, name));
	}
	
	public synchronized Field getOrDefault(Indexable scope, String name, Object value)
	{
		if (this.fields == null)
			this.fields = new LinkedHashMap<Hash, Field>();

		Field field = this.fields.get(Field.pointer(scope, name));
		if (field == null)
			field = new Field(scope, name, value);
		return field;
	}

	
	public synchronized Field set(Indexable scope, String name, Object value)
	{
		if (this.fields == null)
			this.fields = new LinkedHashMap<Hash, Field>();

		Field field = new Field(scope, name, value);
		return this.fields.put(field.getPointer(), field);
	}
	
	public synchronized Field set(Field field)
	{
		if (this.fields == null)
			this.fields = new LinkedHashMap<Hash, Field>();

		return this.fields.put(field.getPointer(), field);
	}

	public synchronized int size()
	{
		if (this.fields == null)
			return 0;

		return this.fields.size();
	}
	
	public synchronized boolean isEmpty()
	{
		if (this.fields == null)
			return true;
		
		return this.fields.isEmpty();
	}

	@Override
	public Iterator<Field> iterator()
	{
		if (this.fields == null)
			this.fields = new LinkedHashMap<Hash, Field>();
		
		return this.fields.values().iterator();
	}
}
