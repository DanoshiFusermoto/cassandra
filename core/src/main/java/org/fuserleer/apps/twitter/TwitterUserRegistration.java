package org.fuserleer.apps.twitter;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.Universe;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.database.Indexable;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.atoms.SignedParticle;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("apps.twitter.user")
public class TwitterUserRegistration extends SignedParticle
{
	public static int MAX_NAME_LENGTH = 128;
	public static int MAX_SCREENNAME_LENGTH = 48;
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
	
	public TwitterUserRegistration(long id, String handle, long createdAt, ECPublicKey owner)
	{
		super(Spin.UP, owner);
		
		this.id = id;

		this.handle = Objects.requireNonNull(handle).toLowerCase();
		if (this.handle.isEmpty() || this.handle.length() > TwitterUserRegistration.MAX_SCREENNAME_LENGTH)
			throw new IllegalArgumentException("Handle "+this.handle+" length is greater than MAX_NAME_LENGTH "+TwitterUserRegistration.MAX_SCREENNAME_LENGTH);
		
		this.createdAt = createdAt;
		if (this.createdAt <= Universe.getDefault().getTimestamp())
			throw new IllegalArgumentException("Created time "+this.createdAt+" is before genesis time "+Universe.getDefault().getTimestamp());
	}

	public long getID() 
	{
		return this.id;
	}

	@JsonProperty("handle")
	@DsonOutput(Output.ALL)
	public String getHandle() 
	{
		return this.handle.toLowerCase();
	}
	
	@JsonProperty("handle")
	private void setHandle(String handle)
	{
		this.handle = handle.toLowerCase();
	}

	public long getCreatedAt() 
	{
		return this.createdAt;
	}

	public String getName() 
	{
		return this.name;
	}

	public void setName(String name) 
	{
		if (Objects.requireNonNull(name).isEmpty() == true)
			return;

		if (Objects.requireNonNull(name).length() > TwitterUserRegistration.MAX_NAME_LENGTH)
			throw new IllegalArgumentException("Name "+name+" is greater than MAX_NAME_LENGTH "+TwitterUserRegistration.MAX_NAME_LENGTH);

		this.name = name;
	}

	public String getLocation() 
	{
		return this.location;
	}

	public void setLocation(String location) 
	{
		if (Objects.requireNonNull(location).isEmpty() == true)
			return;

		if (Objects.requireNonNull(location).length() > TwitterUserRegistration.MAX_LOCATION_LENGTH)
			throw new IllegalArgumentException("Location "+location+" is greater than MAX_LOCATION_LENGTH "+TwitterUserRegistration.MAX_LOCATION_LENGTH);

		this.location = location;
	}

	public String getURL() 
	{
		return this.URL;
	}

	public void setURL(String URL) 
	{
		if (Objects.requireNonNull(URL).isEmpty() == true)
			return;

		if (Objects.requireNonNull(URL).length() > TwitterUserRegistration.MAX_URL_LENGTH)
			throw new IllegalArgumentException("URL "+URL+" length is greater than MAX_URL_LENGTH "+TwitterUserRegistration.MAX_URL_LENGTH);

		this.URL = URL;
	}

	public String getDescription() 
	{
		return this.description;
	}

	public void setDescription(String description) 
	{
		if (Objects.requireNonNull(description).isEmpty() == true)
			return;
		
		if (Objects.requireNonNull(description).length() > TwitterUserRegistration.MAX_DESCRIPTION_LENGTH)
			throw new IllegalArgumentException("Description "+description+" length is greater than MAX_DESCRIPTION_LENGTH "+TwitterUserRegistration.MAX_DESCRIPTION_LENGTH);

		this.description = description;
	}

	public long numFollowers() 
	{
		return this.numFollowers;
	}

	public void setNumFollowers(long numFollowers) 
	{
		if (numFollowers < 0)
			throw new IllegalArgumentException("Followers can not be negative");
		
		this.numFollowers = numFollowers;
	}

	public long numFriends() 
	{
		return this.numFriends;
	}

	public void setNumFriends(long numFriends) 
	{		
		if (numFriends < 0)
			throw new IllegalArgumentException("Friends can not be negative");

		this.numFriends = numFriends;
	}
	
	public long numStatuses() 
	{
		return this.numStatuses;
	}

	public void setNumStatuses(long numStatuses) 
	{		
		if (numStatuses < 0)
			throw new IllegalArgumentException("Status count can not be negative");

		this.numStatuses = numStatuses;
	}

	@Override
	public Set<Indexable> getIndexables()
	{
		Set<Indexable> indexables = super.getIndexables();
		indexables.add(Indexable.from(this.handle, getClass()));
		indexables.add(Indexable.from(getOwner().asHash(), getClass()));
		return indexables;
	}

	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException 
	{
		if (this.handle == null)
			throw new ValidationException("Handle is null");
		
		if (this.handle.isEmpty() == true)
			throw new ValidationException("Handle is empty");

		if (this.handle.length() > TwitterUserRegistration.MAX_SCREENNAME_LENGTH)
			throw new ValidationException("Handle "+this.handle+" length "+this.handle.length()+" is greater than MAX_SCREENNAME_LENGTH "+TwitterUserRegistration.MAX_SCREENNAME_LENGTH);
		
		if (this.name != null && this.name.length() > TwitterUserRegistration.MAX_NAME_LENGTH)
			throw new ValidationException("Name length "+this.name.length()+" is greater than MAX_NAME_LENGTH "+TwitterUserRegistration.MAX_NAME_LENGTH);

		if (this.createdAt <= Universe.getDefault().getTimestamp())
			throw new ValidationException("Created time "+this.createdAt+" is before genesis time "+Universe.getDefault().getTimestamp());
		
		if (this.location != null && this.location.length() > TwitterUserRegistration.MAX_LOCATION_LENGTH)
			throw new ValidationException("Location length "+this.location.length()+" is greater than MAX_LOCATION_LENGTH "+TwitterUserRegistration.MAX_LOCATION_LENGTH);

		if (this.URL != null && this.URL.length() > TwitterUserRegistration.MAX_URL_LENGTH)
			throw new ValidationException("URL length "+this.URL.length()+" is greater than MAX_URL_LENGTH "+TwitterUserRegistration.MAX_URL_LENGTH);
		// TODO URL form verification

		if (this.numFriends < 0)
			throw new ValidationException("Friends can not be negative");

		if (this.numFollowers < 0)
			throw new ValidationException("Followers can not be negative");

		if (this.numStatuses < 0)
			throw new ValidationException("Status count can not be negative");
		
		super.prepare(stateMachine);
	}

	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException 
	{
		String user = stateMachine.get("user");
		if (user == null)
			stateMachine.put("user", this.handle);
		else
			throw new ValidationException("State machine already has user "+user+" specified");
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
