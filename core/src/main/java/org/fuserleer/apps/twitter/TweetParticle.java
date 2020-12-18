package org.fuserleer.apps.twitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.fuserleer.Universe;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.Field;
import org.fuserleer.database.Fields;
import org.fuserleer.database.Identifier;
import org.fuserleer.database.Indexable;
import org.fuserleer.exceptions.DependencyNotFoundException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.SignedParticle;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Longs;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("apps.twitter.tweet")
public class TweetParticle extends SignedParticle
{
	private static final Logger twitterLog = Logging.getLogger("twitter");

	public static int MAX_TEXT_LENGTH = 512;
	public static int MAX_HASH_TAGS = 32;
	public static int MAX_MENTIONS = 32;
	public static int MAX_MEDIA = 4;
	
	@JsonProperty("id")
	@DsonOutput(Output.ALL)
	private Hash id;

	@JsonProperty("user")
	@DsonOutput(Output.ALL)
	private String user;

	@JsonProperty("created_at")
	@DsonOutput(Output.ALL)
	private long createdAt;
	
	@JsonProperty("text")
	@DsonOutput(Output.ALL)
	private String text;
	
	@JsonProperty("reply_to")
	@DsonOutput(Output.ALL)
	private Hash replyTo;

	@JsonProperty("retweet_of")
	@DsonOutput(Output.ALL)
	private Hash retweetOf;

	@JsonProperty("media")
	@JsonDeserialize(as=LinkedHashSet.class)
	@DsonOutput(Output.ALL)
	private Set<Hash> media;

	private transient Set<String> hashtags;
	private transient Set<String> mentions;

	TweetParticle()
	{
		super();
	}

	public TweetParticle(long id, String user, String text, Collection<Hash> media, Hash replyTo, Hash retweetOf, long createdAt, ECPublicKey owner)
	{
		super(Spin.UP, owner);
		
		this.id = new Hash(Arrays.copyOf(Longs.toByteArray(id), Hash.BYTES));
		this.user = user;
		this.text = text;
		this.replyTo = replyTo;
		this.retweetOf = retweetOf;
		this.createdAt = createdAt;
		
		if (media != null && media.isEmpty() == false)
			this.media = new LinkedHashSet<Hash>(media);

/*		this.text = new String(Objects.requireNonNull(text, "Text is null").getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
		if (this.text.isEmpty())
			throw new IllegalArgumentException("Text is empty");
		if (this.text.length() > TweetOperation.MAX_TEXT_LENGTH)
			throw new IllegalArgumentException("Text length "+this.text.length()+" is greater than MAX_TEXT_LENGTH "+TweetOperation.MAX_TEXT_LENGTH);
		
		this.user = new String(Objects.requireNonNull(user, "User is null").toLowerCase().getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
		if (this.user.isEmpty())
			throw new IllegalArgumentException("User is empty");
		if (this.user.length() > TwitterUserRegistration.MAX_SCREENNAME_LENGTH)
			throw new IllegalArgumentException("User "+this.user+" length "+this.user.length()+" is greater than MAX_NAME_LENGTH "+TwitterUserRegistration.MAX_SCREENNAME_LENGTH);
		
		this.createdAt = createdAt;
		if (this.createdAt <= Deployment.getDefault().getTimestamp())
			throw new IllegalArgumentException("Created time "+this.createdAt+" is before genesis time "+Deployment.getDefault().getTimestamp());
		
		if (replyTo != null)
		{
			if (replyTo.equals(Hash.ZERO) == true)
				throw new IllegalArgumentException("Reply hash is ZERO");

			this.replyTo = replyTo;
		}

		if (retweetOf != null)
		{
			if (this.replyTo != null)
				throw new IllegalStateException("Tweets can not be a reply and retweet");
			
			if (retweetOf.equals(Hash.ZERO) == true)
				throw new IllegalArgumentException("Retweet hash is ZERO");

			this.retweetOf = retweetOf;
		}

		if (media != null && media.isEmpty() == false)
		{
			if (media.size() > TweetOperation.MAX_MEDIA)
				throw new IllegalArgumentException("Maximum media items is "+TweetOperation.MAX_MEDIA);

			this.media = new LinkedHashSet<Hash>(media);
		}*/
		
		extractHashTags();
		extractMentions();
		verifyArguments();
	}

	public Hash getID()
	{
		return this.id;
	}

	public String getUser()
	{
		return this.user;
	}

	public long getCreatedAt()
	{
		return this.createdAt;
	}

	public String getText()
	{
		return this.text;
	}

	private void extractHashTags()
	{
		if (this.text == null)
			return;
		
		this.hashtags = new LinkedHashSet<String>();
		// TODO Trims the # from the match... seems sensible as we know what it is, but maybe it should be included in full form?
//		Pattern pattern = Pattern.compile("(?<=^#|\\s+#)([A-Za-z0-9_-]+)");
		Pattern pattern = Pattern.compile("#[A-Za-z0-9_-]+");
		Matcher matcher = pattern.matcher(this.text);
		
		while(matcher.find() == true)
		{
			String hashtag = matcher.group();
			if (this.hashtags.size() == TweetParticle.MAX_HASH_TAGS)
				throw new IllegalStateException("Max hashtags of "+TweetParticle.MAX_HASH_TAGS+" reached");
				
			this.hashtags.add(hashtag.substring(1).toLowerCase());
		}
	}
	
	private void extractMentions()
	{
		if (this.text == null)
			return;

		this.mentions = new LinkedHashSet<String>();
		// TODO Trims the @ from the match leaving the user name only, as @ symbols 
		// are usually an indicator of a use, and not included in the user name
//		Pattern pattern = Pattern.compile("(?<=^@|\\s+@)([A-Za-z0-9_-]+)");
		Pattern pattern = Pattern.compile("@[A-Za-z0-9_-]+");
		Matcher matcher = pattern.matcher(this.text);
		
		while(matcher.find() == true)
		{
			String mention = matcher.group();
			if (this.mentions.size() == TweetParticle.MAX_MENTIONS)
				throw new IllegalStateException("Max mentions of "+TweetParticle.MAX_MENTIONS+" reached");
				
			this.mentions.add(mention.substring(1).toLowerCase());
		}
	}

	@SuppressWarnings("unchecked")
	@JsonProperty("hashtags")
	@DsonOutput(Output.ALL)
	public Collection<String> getHashTags()
	{
		if (this.hashtags == null)
			return Collections.EMPTY_LIST;

		return new ArrayList<String>(this.hashtags);
	}
	
	@JsonProperty("hashtags")
	private void setHashTags(Collection<String> hashtags)
	{
		if (hashtags == null || hashtags.isEmpty())
			return;

		this.hashtags = new LinkedHashSet<String>();
		hashtags.forEach(hashtag -> this.hashtags.add(hashtag.toLowerCase()));
	}

	public int hashTagCount() 
	{
		if (this.hashtags == null)
			return 0;

		return this.hashtags.size();
	}

	@SuppressWarnings("unchecked")
	@JsonProperty("mentions")
	@DsonOutput(Output.ALL)
	public Collection<String> getMentions()
	{
		if (this.mentions == null)
			return Collections.EMPTY_LIST;
		
		return new ArrayList<String>(this.mentions);
	}
	
	@JsonProperty("mentions")
	private void setMentions(Collection<String> mentions)
	{
		if (mentions == null || mentions.isEmpty())
			return;
		
		this.mentions = new LinkedHashSet<String>();
		mentions.forEach(mention -> this.mentions.add(mention.toLowerCase()));
	}

	public int mentionsCount() 
	{
		if (this.mentions == null)
			return 0;
		
		return this.mentions.size();
	}

	@SuppressWarnings("unchecked")
	public Collection<Hash> getMedia()
	{
		if (this.media == null)
			return Collections.EMPTY_LIST;
		
		return new ArrayList<Hash>(this.media);
	}
	
	public int mediaCount() 
	{
		if (this.media == null)
			return 0;
		
		return this.media.size();
	}
	
	public Hash getReplyTo() 
	{
		return this.replyTo;
	}

	public Hash getRetweetOf() 
	{
		return this.retweetOf;
	}

	@Override
	public Set<Indexable> getIndexables() 
	{
		Set<Indexable> indexables = super.getIndexables();
		indexables.add(Indexable.from(this.id, getClass()));
		return indexables;
	}

	@Override
	public Set<Identifier> getIdentifiers()
	{
		Set<Identifier> identifiers = super.getIdentifiers();
		identifiers.add(Identifier.from(this.user));
		
		// Concatenated user + owner for faster searches
		identifiers.add(Identifier.from(Identifier.from(this.user), Identifier.from(getOwner().getBytes())));
		
		if (this.hashtags != null)
		{
			for (String hashTag : this.hashtags)
				identifiers.add(Identifier.from(hashTag));
		}

		if (this.mentions != null)
		{
			for (String mention : this.mentions)
				identifiers.add(Identifier.from(mention));
		}
		
		if (this.replyTo != null)
			identifiers.add(Identifier.from(this.replyTo));

		return identifiers;
	}

	private void verifyArguments()
	{
		if (this.text == null)
			throw new NullPointerException("Text is null");
		
		if (this.text.isEmpty() == true)
			throw new IllegalArgumentException("Text is empty");

		if (this.text.length() > TweetParticle.MAX_TEXT_LENGTH)
			throw new IllegalArgumentException("Text length "+this.text.length()+" is greater than MAX_TEXT_LENGTH "+TweetParticle.MAX_TEXT_LENGTH);
		
		if (this.createdAt <= Universe.getDefault().getTimestamp())
			throw new IllegalArgumentException("Created time "+this.createdAt+" is before genesis time "+Universe.getDefault().getTimestamp());
		
		// Hashtags
		if (this.hashtags != null)
		{
			if (this.hashtags.size() > TweetParticle.MAX_HASH_TAGS)
				throw new IllegalStateException("Hashtags exceeds max of "+TweetParticle.MAX_HASH_TAGS);
			
			for (String hashTag : this.hashtags)
			{
				if (hashTag.length() == 0)
					throw new IllegalStateException("Hashtag length is zero");
					
				if (this.text.toLowerCase().contains("#"+hashTag.toLowerCase()) == false)
					throw new IllegalStateException("Tweet doesn't contain hashtag "+hashTag.toLowerCase());
			}
		}
		
		// Mentions
		if (this.mentions != null)
		{
			if (this.mentions.size() > TweetParticle.MAX_MENTIONS)
				throw new IllegalStateException("Mentions exceeds max of "+TweetParticle.MAX_MENTIONS);
	
			for (String mention : this.mentions)
			{
				if (mention.length() == 0)
					throw new IllegalStateException("Mention length is zero");
					
				if (this.text.toLowerCase().contains("@"+mention.toLowerCase()) == false)
					throw new IllegalStateException("Tweet doesn't contain mention "+mention.toLowerCase());
			}
		}
		
		// Media 
		if (this.media != null)
		{
			if (this.media.size() > TweetParticle.MAX_MEDIA)
				throw new IllegalStateException("Maximum media items is "+TweetParticle.MAX_MEDIA);
		}
		
		if (this.replyTo != null && this.replyTo.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Reply hash is ZERO");

		if (this.retweetOf != null && this.retweetOf.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Retweet hash is ZERO");
		
		if (this.replyTo != null && this.retweetOf != null)
			throw new IllegalStateException("Tweets can not be a reply and retweet");

		// User
		if (this.user == null)
			throw new NullPointerException("User is null");

		if (this.user.isEmpty() == true)
			throw new IllegalArgumentException("User is empty");

		if (this.user.length() > TwitterUserRegistration.MAX_SCREENNAME_LENGTH)
			throw new IllegalArgumentException("User "+this.user+" length "+this.user.length()+" is greater than MAX_NAME_LENGTH "+TwitterUserRegistration.MAX_SCREENNAME_LENGTH);

	}
	
	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException 
	{
		try
		{
			verifyArguments();
		}
		catch (NullPointerException | IllegalStateException | IllegalArgumentException ex)
		{
			// Can just throw the causes as they are uncaught exceptions?
			throw new ValidationException(ex);
		}
	}

	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException
	{
		String user = stateMachine.get("user");
		if (user != null && user.equalsIgnoreCase(this.user) == false)
			throw new ValidationException("Expected users "+this.user+" but found "+user);
		else if (user == null)
		{
			Indexable userStateIndexable = Indexable.from(this.user, TwitterUserRegistration.class);
			if (stateMachine.state(userStateIndexable) == false)
				throw new DependencyNotFoundException("User "+this.user+" not found", stateMachine.getAtom().getHash(), userStateIndexable.getHash());
		}
		
		try
		{
			if (this.replyTo != null)
			{
				Indexable replyToIndexable = Indexable.from(this.replyTo, TweetParticle.class);
				Atom replyToAtom = stateMachine.get(replyToIndexable, Atom.class);
				if (replyToAtom != null)
				{
					Fields fields = replyToAtom.getFields();

					Field repliesField = fields.getOrDefault(Indexable.from(this.replyTo, TweetParticle.class), "num_replies", 0);
					fields.set(repliesField.getScope(), repliesField.getName(), ((int) repliesField.getValue()) + 1);
					
					Field favoriteRatioField = fields.getOrDefault(Indexable.from(this.replyTo, TweetParticle.class), "favourite_ratio", 1);
					Field favoritesField = fields.getOrDefault(Indexable.from(this.replyTo, TweetParticle.class), "num_favourites", 0);
					fields.set(favoritesField.getScope(), favoritesField.getName(), ((int) favoritesField.getValue()) + ((int) favoriteRatioField.getValue()));

					stateMachine.set(replyToAtom.getHash(), fields);
				}
				else
					throw new DependencyNotFoundException("REPLY: Atom containing tweet "+this.replyTo+" not found", stateMachine.getAtom().getHash(), replyToIndexable.getHash());
			}
	
			if (this.retweetOf != null)
			{
				Indexable retweetIndexable = Indexable.from(this.retweetOf, TweetParticle.class);
				Atom retweetAtom = stateMachine.get(retweetIndexable, Atom.class);
				if (retweetAtom != null)
				{
					Fields fields = retweetAtom.getFields();
					
					Field repliesField = fields.getOrDefault(Indexable.from(this.retweetOf, TweetParticle.class), "num_retweets", 0);
					fields.set(repliesField.getScope(), repliesField.getName(), ((int) repliesField.getValue()) + 1);
					
					Field favoriteRatioField = fields.getOrDefault(Indexable.from(this.retweetOf, TweetParticle.class), "favourite_ratio", 1);
					Field favoritesField = fields.getOrDefault(Indexable.from(this.retweetOf, TweetParticle.class), "num_favourites", 0);
					fields.set(favoritesField.getScope(), favoritesField.getName(), ((int) favoritesField.getValue()) + ((int) favoriteRatioField.getValue()));

					stateMachine.set(retweetAtom.getHash(), fields);
				}
				else
					throw new DependencyNotFoundException("RETWEET: Atom containing tweet "+this.retweetOf+" not found", stateMachine.getAtom().getHash(), retweetIndexable.getHash());
			}
		}
		catch (IOException ioex)
		{
			// FIXME throwing a ValidationException to wrap an IOException on an atom update isn't right
			//		 but throwing an IOException stalls the ledger update loop ... can fix this when the latter is fixed
			throw new ValidationException(ioex);
		}
	}
	
	@Override
	public void unexecute(StateMachine stateMachine) throws ValidationException, IOException
	{
		try
		{
			if (this.replyTo != null)
			{
				Atom replyToAtom = stateMachine.get(Indexable.from(this.replyTo, TweetParticle.class), Atom.class);
				if (replyToAtom != null)
				{
					Fields fields = replyToAtom.getFields();
					Field field = fields.getOrDefault(Indexable.from(this.replyTo, TweetParticle.class), "num_replies", 0);
					fields.set(field.getScope(), field.getName(), ((int) field.getValue()) - 1);
					stateMachine.set(replyToAtom.getHash(), fields);
				}
				else
				{
					twitterLog.warn("REPLY: Atom containing tweet "+this.replyTo+" not found");
					throw new ValidationException("RETWEET: Atom containing tweet "+this.replyTo+" not found");
				}
			}
	
			if (this.retweetOf != null)
			{
				Atom retweetAtom = stateMachine.get(Indexable.from(this.retweetOf, TweetParticle.class), Atom.class);
				if (retweetAtom != null)
				{
					Fields fields = retweetAtom.getFields();
					Field field = fields.getOrDefault(Indexable.from(this.retweetOf, TweetParticle.class), "num_retweets", 0);
					fields.set(field.getScope(), field.getName(), ((int) field.getValue()) - 1);
					stateMachine.set(retweetAtom.getHash(), fields);
				}
				else
				{
					twitterLog.warn("RETWEET: Atom containing tweet "+this.retweetOf+" not found");
					throw new ValidationException("RETWEET: Atom containing tweet "+this.retweetOf+" not found");
				}
			}
		}
		catch (IOException ioex)
		{
			// FIXME throwing a ValidationException to wrap an IOException on an atom update isn't right
			//		 but throwing an IOException stalls the ledger update loop ... can fix this when the latter is fixed
			throw new ValidationException(ioex);
		}
	}

	@Override
	public boolean isConsumable()
	{
		return false;
	}

	@Override
	public String toString() 
	{
		return super.toString()+" "+this.id+" "+this.user+" "+this.createdAt+" "+this.text;
	}
}
