package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.crypto.Identity;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class MediaDeclaration extends SignedParticle
{
	public static int MAX_CONTENT_TYPE_LENGTH = 32;

	@JsonProperty("content_type")
	@DsonOutput(Output.ALL)
	private String contentType;
	
	MediaDeclaration() 
	{
		super();
	}

	MediaDeclaration(String contentType, Identity owner)
	{
		super(Spin.UP, owner);
		
		this.contentType = Objects.requireNonNull(contentType).toLowerCase();
		if (this.contentType.isEmpty())
			throw new IllegalArgumentException("Content-type is empty");
			
		if (this.contentType.length() > MediaDeclaration.MAX_CONTENT_TYPE_LENGTH)
			throw new IllegalArgumentException("Content-type "+this.contentType+" length "+this.contentType.length()+"is greater than MAX_CONTENT_TYPE_LENGTH "+MediaDeclaration.MAX_CONTENT_TYPE_LENGTH);
	}
	
	public String getContentType() 
	{
		return this.contentType;
	}

	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException 
	{
		if (this.contentType == null)
			throw new ValidationException("Content-type is null");
		
		if (this.contentType.isEmpty() == true)
			throw new ValidationException("Content-type is empty");

		if (this.contentType.length() > MediaDeclaration.MAX_CONTENT_TYPE_LENGTH)
			throw new ValidationException("Content-type "+this.contentType+" length "+this.contentType.length()+" is greater than MAX_CONTENT_TYPE_LENGTH "+MediaDeclaration.MAX_CONTENT_TYPE_LENGTH);

		super.prepare(stateMachine);
	}

	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException 
	{
	}
}
