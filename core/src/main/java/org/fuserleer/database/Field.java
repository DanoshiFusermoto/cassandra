package org.fuserleer.database;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.SerializerConstants;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("database.field")
public final class Field 
{
	private static final int MAX_NAME_SIZE = 16;
	private static final int MAX_VALUE_SIZE = 32; // TODO this is not used currently, exploit possible with no field value checking 
	
	public final static Hash pointer(Indexable scope, String name)
	{
		Objects.requireNonNull(scope);
		if (Field.class.isAssignableFrom(scope.getContainer()) == true)
			throw new IllegalArgumentException("Field scope references another field");
		
		if (Particle.class.isAssignableFrom(scope.getContainer()) == false)
			throw new IllegalArgumentException("Field scope "+scope.getContainer()+" is not a particle");

		Objects.requireNonNull(name);
		
		if (name.length() > Field.MAX_NAME_SIZE)
			throw new IllegalArgumentException("Field name "+name+" is greater than max name size "+Field.MAX_NAME_SIZE);
		
		if (name.length() == 0)
			throw new IllegalArgumentException("Field name "+name+" is empty");

		return new Hash(scope.getHash().toByteArray(), name.getBytes(StandardCharsets.UTF_8), Mode.STANDARD);
	}
	
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;
	
	@JsonProperty("scope")
	@DsonOutput(Output.ALL)
	private Indexable scope;

	@JsonProperty("name")
	@DsonOutput(Output.ALL)
	private String name;

	@JsonProperty("value")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private Object value;

	Field()
	{ 
		super();
	}

	public Field(Indexable scope, String name, Object value)
	{ 
		Objects.requireNonNull(scope);
		if (Field.class.isAssignableFrom(scope.getContainer()) == true)
			throw new IllegalArgumentException("Field scope references another field");
		
		if (Particle.class.isAssignableFrom(scope.getContainer()) == false)
			throw new IllegalArgumentException("Field scope "+scope.getContainer()+" is not a particle");

		Objects.requireNonNull(name);
		Objects.requireNonNull(value);
		
		if (name.length() > Field.MAX_NAME_SIZE)
			throw new IllegalArgumentException("Field name "+name+" is greater than max name size "+Field.MAX_NAME_SIZE);
		
		if (name.length() == 0)
			throw new IllegalArgumentException("Field name "+name+" is empty");

		this.scope = scope;
		this.name = name;
		this.value = value;
	}

	public Hash getPointer()
	{
		return new Hash(this.scope.getHash().toByteArray(), this.name.getBytes(StandardCharsets.UTF_8), Mode.STANDARD);
	}
	
	public Indexable getScope()
	{
		return this.scope;
	}

	public String getName()
	{
		return this.name;
	}
	
	@SuppressWarnings("unchecked")
	public <V> V getValue()
	{
		return (V) this.value;
	}
	
	@Override
	public int hashCode() 
	{
		return getPointer().hashCode();
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof Field)
		{
			if (this.scope.equals(((Field)object).scope) == false)
				return false;

			if (this.name.equals(((Field)object).name) == false)
				return false;
			
			if (this.value.equals(((Field)object).value) == false)
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.scope.toString()+" "+this.name+" "+this.value;
	}
}
