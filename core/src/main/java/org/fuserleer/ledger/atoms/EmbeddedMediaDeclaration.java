package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atoms.particles.media.embedded")
public final class EmbeddedMediaDeclaration extends MediaDeclaration
{
	public static int MAX_DATA_SIZE = 1<<20; // 1MB :-)
	
	@JsonProperty("data")
	@DsonOutput(Output.ALL)
	private byte[] data;
	
	EmbeddedMediaDeclaration() 
	{
		super();
	}
	
	public EmbeddedMediaDeclaration(String contentType, byte[] data, ECPublicKey owner) 
	{
		super(contentType, owner);
		
		Objects.requireNonNull(data);
		if (this.data.length == 0)
			throw new IllegalArgumentException("Data is empty");
		
		if (this.data.length > EmbeddedMediaDeclaration.MAX_DATA_SIZE)
			throw new IllegalArgumentException("Data size "+this.data.length+"is greater than MAX_DATA_SIZE "+EmbeddedMediaDeclaration.MAX_DATA_SIZE);

		this.data = Arrays.copyOf(data, data.length);
	}
	
	public byte[] getData()
	{
		return Arrays.copyOf(this.data, this.data.length);
	}

	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException 
	{
		if (this.data == null)
			throw new ValidationException("Data is null");
			
		if (this.data.length == 0)
			throw new ValidationException("Data is empty");
		
		if (this.data.length > EmbeddedMediaDeclaration.MAX_DATA_SIZE)
			throw new ValidationException("Data size "+this.data.length+"is greater than MAX_DATA_SIZE "+EmbeddedMediaDeclaration.MAX_DATA_SIZE);

		super.prepare(stateMachine);
	}
	
	@Override
	public boolean isConsumable()
	{
		return false;
	}
}
