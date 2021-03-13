package org.fuserleer.apps.twitter;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.Universe;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateAddress;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.StateOp;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.ledger.atoms.SignedParticle;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("apps.twitter.user")
public class TwitterUserRegistration extends SignedParticle
{
	public static int MIN_NAME_LENGTH = 3;
	public static int MIN_HANDLE_LENGTH = 3;
	public static int MIN_LOCATION_LENGTH = 3;
	public static int MIN_DESCRIPTION_LENGTH = 3;
	public static int MIN_URL_LENGTH = 3;

	public static int MAX_NAME_LENGTH = 128;
	public static int MAX_HANDLE_LENGTH = 48;
	public static int MAX_LOCATION_LENGTH = 256;
	public static int MAX_DESCRIPTION_LENGTH = 256;
	public static int MAX_URL_LENGTH = 256;
	
	@JsonProperty("id")
	@DsonOutput(Output.ALL)
	private long id;
	
	@JsonProperty("created_at")
	@DsonOutput(Output.ALL)
	private long createdAt;

	@JsonProperty("name")
	@DsonOutput(value = Output.HASH, include = false)
	private String name;

	@JsonProperty("location")
	@DsonOutput(value = Output.HASH, include = false)
	private String location;
	
	@JsonProperty("url")
	@DsonOutput(value = Output.HASH, include = false)
	private String URL;

	@JsonProperty("description")
	@DsonOutput(value = Output.HASH, include = false)
	private String description;
	
	@JsonProperty("num_followers")
	@DsonOutput(value = Output.HASH, include = false)
	private long numFollowers;

	@JsonProperty("num_friends")
	@DsonOutput(value = Output.HASH, include = false)
	private long numFriends;
	
	@JsonProperty("num_statuses")
	@DsonOutput(value = Output.HASH, include = false)
	private long numStatuses;

	private transient String handle;
	
	TwitterUserRegistration()
	{
		super();
	}
	
	public TwitterUserRegistration(final long id, final String handle, final long createdAt, final ECPublicKey owner)
	{
		super(Spin.UP, owner);
		
		this.id = id;

		setHandle(handle);
		
		this.createdAt = createdAt;
		Numbers.lessThan(this.createdAt, Universe.getDefault().getTimestamp(), "Created time "+this.createdAt+" is before genesis time "+Universe.getDefault().getTimestamp());
	}

	public long getID() 
	{
		return this.id;
	}

	@JsonProperty("handle")
	@DsonOutput(Output.ALL)
	public String getHandle() 
	{
		return this.handle.trim().toLowerCase();
	}
	
	@JsonProperty("handle")
	private void setHandle(final String handle)
	{
		String trimmedHandle = Objects.requireNonNull(handle, "Handle is null").trim().toLowerCase();
		Numbers.inRange(trimmedHandle.length(), TwitterUserRegistration.MIN_HANDLE_LENGTH, TwitterUserRegistration.MAX_HANDLE_LENGTH, "Handle "+trimmedHandle+" length "+trimmedHandle.length()+" is not in range "+TwitterUserRegistration.MIN_HANDLE_LENGTH+" -> "+TwitterUserRegistration.MAX_HANDLE_LENGTH);
		this.handle = trimmedHandle;
	}

	public long getCreatedAt() 
	{
		return this.createdAt;
	}

	public String getName() 
	{
		return this.name;
	}

	public void setName(final String name) 
	{
		Objects.requireNonNull(name, "Name is null");
		Numbers.inRange(name.length(), TwitterUserRegistration.MIN_NAME_LENGTH, TwitterUserRegistration.MAX_NAME_LENGTH, "Name "+name+" length "+name.length()+" is not in range "+TwitterUserRegistration.MIN_NAME_LENGTH+" -> "+TwitterUserRegistration.MAX_NAME_LENGTH);
		this.name = name;
	}

	public String getLocation() 
	{
		return this.location;
	}

	public void setLocation(final String location) 
	{
		Objects.requireNonNull(location, "Location is null");
		Numbers.inRange(location.length(), TwitterUserRegistration.MIN_LOCATION_LENGTH, TwitterUserRegistration.MAX_LOCATION_LENGTH, "Location "+location+" length "+location.length()+" is not in range "+TwitterUserRegistration.MIN_LOCATION_LENGTH+" -> "+TwitterUserRegistration.MAX_LOCATION_LENGTH);
		this.location = location;
	}

	public String getURL() 
	{
		return this.URL;
	}

	public void setURL(final String URL) 
	{
		Objects.requireNonNull(URL, "URL is null");
		Numbers.inRange(URL.length(), TwitterUserRegistration.MIN_URL_LENGTH, TwitterUserRegistration.MAX_URL_LENGTH, "URL "+URL+" length "+URL.length()+" is not in range "+TwitterUserRegistration.MIN_URL_LENGTH+" -> "+TwitterUserRegistration.MAX_URL_LENGTH);
		this.URL = URL;
	}

	public String getDescription() 
	{
		return this.description;
	}

	public void setDescription(final String description) 
	{
		Objects.requireNonNull(description, "Description is null");
		Numbers.inRange(description.length(), TwitterUserRegistration.MIN_DESCRIPTION_LENGTH, TwitterUserRegistration.MAX_DESCRIPTION_LENGTH, "Description "+description+" length "+description.length()+" is not in range "+TwitterUserRegistration.MIN_DESCRIPTION_LENGTH+" -> "+TwitterUserRegistration.MAX_DESCRIPTION_LENGTH);
		this.description = description;
	}

	public long numFollowers() 
	{
		return this.numFollowers;
	}

	public void setNumFollowers(final long numFollowers) 
	{
		Numbers.isNegative(numFollowers, "Followers can not be negative");
		this.numFollowers = numFollowers;
	}

	public long numFriends() 
	{
		return this.numFriends;
	}

	public void setNumFriends(long numFriends) 
	{		
		Numbers.isNegative(numFriends, "Friends can not be negative");
		this.numFriends = numFriends;
	}
	
	public long numStatuses() 
	{
		return this.numStatuses;
	}

	public void setNumStatuses(long numStatuses) 
	{		
		Numbers.isNegative(numStatuses, "Status count can not be negative");
		this.numStatuses = numStatuses;
	}
	
	@Override
	public void prepare(final StateMachine stateMachine, final Object ... arguments) throws ValidationException, IOException 
	{
		Objects.requireNonNull(this.handle, "Handle is null");
		Numbers.inRange(this.handle.length(), TwitterUserRegistration.MIN_HANDLE_LENGTH, TwitterUserRegistration.MAX_HANDLE_LENGTH, "Handle "+this.handle+" length "+this.handle.length()+" is not in range "+TwitterUserRegistration.MIN_HANDLE_LENGTH+" -> "+TwitterUserRegistration.MAX_HANDLE_LENGTH);
		
		Numbers.lessThan(this.createdAt, Universe.getDefault().getTimestamp(), "Created time "+this.createdAt+" is before genesis time "+Universe.getDefault().getTimestamp());

		if (this.name != null)		
			Numbers.inRange(this.name.length(), TwitterUserRegistration.MIN_NAME_LENGTH, TwitterUserRegistration.MAX_NAME_LENGTH, "Name "+this.name+" length "+this.name.length()+" is not in range "+TwitterUserRegistration.MIN_NAME_LENGTH+" -> "+TwitterUserRegistration.MAX_NAME_LENGTH);

		if (this.location != null)		
			Numbers.inRange(this.location.length(), TwitterUserRegistration.MIN_LOCATION_LENGTH, TwitterUserRegistration.MAX_LOCATION_LENGTH, "Location "+this.location+" length "+this.location.length()+" is not in range "+TwitterUserRegistration.MIN_LOCATION_LENGTH+" -> "+TwitterUserRegistration.MAX_LOCATION_LENGTH);

		if (this.URL != null)		
			Numbers.inRange(this.URL.length(), TwitterUserRegistration.MIN_URL_LENGTH, TwitterUserRegistration.MAX_URL_LENGTH, "URL "+this.URL+" length "+this.URL.length()+" is not in range "+TwitterUserRegistration.MIN_URL_LENGTH+" -> "+TwitterUserRegistration.MAX_URL_LENGTH);

		// TODO URL form verification
		
		if (this.description != null)
			Numbers.inRange(this.description.length(), TwitterUserRegistration.MIN_DESCRIPTION_LENGTH, TwitterUserRegistration.MAX_DESCRIPTION_LENGTH, "Description "+this.description+" length "+this.description.length()+" is not in range "+TwitterUserRegistration.MIN_DESCRIPTION_LENGTH+" -> "+TwitterUserRegistration.MAX_DESCRIPTION_LENGTH);

		Numbers.isNegative(this.numFriends, "Friends can not be negative");
		Numbers.isNegative(this.numFollowers, "Followers can not be negative");
		Numbers.isNegative(this.numStatuses, "Status count can not be negative");
		
		stateMachine.sop(new StateOp(new StateAddress(TwitterUserRegistration.class, Hash.from(UInt256.from(this.id))), Instruction.NOT_EXISTS), this);
		stateMachine.sop(new StateOp(new StateAddress(TwitterUserRegistration.class, Hash.from(this.handle.toLowerCase())), Instruction.NOT_EXISTS), this);
		stateMachine.sop(new StateOp(new StateAddress(TwitterUserRegistration.class, getOwner().asHash()), Instruction.NOT_EXISTS), this);
		
		super.prepare(stateMachine);
	}

	@Override
	public void execute(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException 
	{
		stateMachine.sop(new StateOp(new StateAddress(TwitterUserRegistration.class, Hash.from(UInt256.from(this.id))), UInt256.from(this.id), Instruction.SET), this);
		stateMachine.sop(new StateOp(new StateAddress(TwitterUserRegistration.class, Hash.from(this.handle.trim().toLowerCase())), UInt256.from(Hash.from(this.handle.trim().toLowerCase()).toByteArray()), Instruction.SET), this);
		stateMachine.sop(new StateOp(new StateAddress(TwitterUserRegistration.class, getOwner().asHash()), UInt256.from(getOwner().asHash().toByteArray()), Instruction.SET), this);
	}
	
	@Override
	public boolean isConsumable()
	{
		return false;
	}

	@Override
	public String toString() 
	{
		return super.toString()+" "+this.id+" "+this.handle+" "+this.createdAt;
	}
}
