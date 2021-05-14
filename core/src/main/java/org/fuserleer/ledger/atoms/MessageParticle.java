package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.Universe;
import org.fuserleer.crypto.Identity;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.particle.message")
public class MessageParticle extends SignedParticle
{
	public static int MAX_SUBJECT_LENGTH = 64;
	public static int MAX_MESSAGE_LENGTH = 2048;

	@JsonProperty("subject")
	@DsonOutput(Output.ALL)
	private String subject;

	@JsonProperty("message")
	@DsonOutput(Output.ALL)
	private String message;
	
	@JsonProperty("recipient")
	@DsonOutput(Output.ALL)
	private Identity recipient;

	@JsonProperty("created_at")
	@DsonOutput(Output.ALL)
	private long createdAt;

	MessageParticle()
	{
		super();
	}
	
	public MessageParticle(String message, Identity sender, Identity recipient, long createdAt)
	{
		super(Spin.UP, sender);
		
		this.recipient = Objects.requireNonNull(recipient);
		
		this.message = Objects.requireNonNull(message);
		if (this.message.isEmpty())
			throw new IllegalArgumentException("Message is empty");
		if (this.message.length() > MessageParticle.MAX_MESSAGE_LENGTH)
			throw new IllegalArgumentException("Message length "+this.message.length()+" is greater than MAX_MESSAGE_LENGTH "+MessageParticle.MAX_MESSAGE_LENGTH);
		
		this.createdAt = createdAt;
		if (this.createdAt <= Universe.getDefault().getTimestamp())
			throw new IllegalArgumentException("Created time "+this.createdAt+" is before genesis time "+Universe.getDefault().getTimestamp());
	}

	public MessageParticle(String subject, String message, Identity sender, Identity recipient, long createdAt)
	{
		super(Spin.UP, sender);
		
		this.recipient = Objects.requireNonNull(recipient);

		this.message = Objects.requireNonNull(message);
		if (this.message.isEmpty())
			throw new IllegalArgumentException("Message is empty");
		if (this.message.length() > MessageParticle.MAX_MESSAGE_LENGTH)
			throw new IllegalArgumentException("Message length "+this.message.length()+" is greater than MAX_MESSAGE_LENGTH "+MessageParticle.MAX_MESSAGE_LENGTH);

		this.subject = Objects.requireNonNull(subject);
		if (this.subject.isEmpty())
			throw new IllegalArgumentException("Subject is empty");
		if (this.subject.length() > MessageParticle.MAX_SUBJECT_LENGTH)
			throw new IllegalArgumentException("Subject length "+this.subject.length()+" is greater than MAX_SUBJECT_LENGTH "+MessageParticle.MAX_SUBJECT_LENGTH);

		this.createdAt = createdAt;
		Numbers.lessThan(this.createdAt, Universe.getDefault().getTimestamp(), "Created time "+this.createdAt+" is before genesis time "+Universe.getDefault().getTimestamp());
	}

	public String getSubject() 
	{
		return this.subject;
	}

	public String getMessage() 
	{
		return this.message;
	}

	public Identity getSender() 
	{
		return getOwner();
	}

	public Identity getRecipient() 
	{
		return this.recipient;
	}

	public long getCreatedAt() 
	{
		return this.createdAt;
	}

	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException 
	{
		if (this.message == null)
			throw new ValidationException("Message is null");
		
		if (this.message.isEmpty() == true)
			throw new ValidationException("Message is empty");

		if (this.message.length() > MessageParticle.MAX_MESSAGE_LENGTH)
			throw new ValidationException("Message length "+this.message.length()+" is greater than MAX_MESSAGE_LENGTH "+MessageParticle.MAX_MESSAGE_LENGTH);
		
		if (this.createdAt <= Universe.getDefault().getTimestamp())
			throw new ValidationException("Created time "+this.createdAt+" is before genesis time "+Universe.getDefault().getTimestamp());
		
		if (this.recipient == null)
			throw new ValidationException("Recipient is null");
	}

	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException 
	{
		super.execute(stateMachine);

		stateMachine.associate(getRecipient().asHash(), this);
	}

	@Override
	public boolean isConsumable()
	{
		return false;
	}
}
