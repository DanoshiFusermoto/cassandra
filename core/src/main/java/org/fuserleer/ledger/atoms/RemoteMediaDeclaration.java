package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.crypto.Identity;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.particle.media.remote")
public final class RemoteMediaDeclaration extends MediaDeclaration 
{
	public static int MAX_URL_LENGTH = 256;

	@JsonProperty("url")
	@DsonOutput(Output.ALL)
	private String URL;

	RemoteMediaDeclaration() 
	{
		super();
	}

	public RemoteMediaDeclaration(String contentType, String URL, Identity owner) 
	{
		super(contentType, owner);
		
		this.URL = Objects.requireNonNull(URL);
		if (this.URL.isEmpty())
			throw new IllegalArgumentException("URL is empty");
			
		if (this.URL.length() > RemoteMediaDeclaration.MAX_URL_LENGTH)
			throw new IllegalArgumentException("URL "+this.URL+" length "+this.URL.length()+" is greater than MAX_URL_LENGTH "+RemoteMediaDeclaration.MAX_URL_LENGTH);
	}

	public String getURL() 
	{
		return this.URL;
	}

	@Override
	public void prepare(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException 
	{
		if (this.URL == null)
			throw new ValidationException("URL is null");
		
		if (this.URL.isEmpty() == true)
			throw new ValidationException("URL is empty");

		if (this.URL.length() > RemoteMediaDeclaration.MAX_URL_LENGTH)
			throw new ValidationException("URL "+this.URL+" length "+this.URL.length()+" is greater than MAX_URL_LENGTH "+RemoteMediaDeclaration.MAX_URL_LENGTH);

		super.prepare(stateMachine);
	}

	@Override
	public void execute(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException 
	{
		super.execute(stateMachine);
	}
	
	@Override
	public boolean isConsumable()
	{
		return false;
	}
}
