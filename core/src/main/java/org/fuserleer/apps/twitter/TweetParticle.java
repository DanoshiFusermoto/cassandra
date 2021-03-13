package org.fuserleer.apps.twitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.fuserleer.Universe;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateAddress;
import org.fuserleer.ledger.StateField;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.StateOp;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.SignedParticle;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.Numbers;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("apps.twitter.tweet")
public class TweetParticle extends SignedParticle
{
	private static final Logger twitterLog = Logging.getLogger("twitter");

	public static int MIN_TEXT_LENGTH = 3;
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
		Objects.requireNonNull(this.text, "Text is null");
		
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
		Objects.requireNonNull(this.text, "Text is null");

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

	private void verifyArguments()
	{
		Objects.requireNonNull(this.text, "Text is null");
		Numbers.inRange(this.text.length(), TweetParticle.MIN_TEXT_LENGTH, TweetParticle.MAX_TEXT_LENGTH, "Test length "+this.text.length()+" is not in range "+TweetParticle.MIN_TEXT_LENGTH+" -> "+TweetParticle.MAX_TEXT_LENGTH);
		
		Numbers.lessThan(this.createdAt, Universe.getDefault().getTimestamp(), "Created time "+this.createdAt+" is before genesis time "+Universe.getDefault().getTimestamp());

		// Hashtags
		if (this.hashtags != null)
		{
			Numbers.lessThan(this.hashtags.size(), TweetParticle.MAX_HASH_TAGS, "Hashtags exceeds max of "+TweetParticle.MAX_HASH_TAGS);
			for (String hashTag : this.hashtags)
			{
				Numbers.notZero(hashTag.length(), "Hashtag length is zero");
					
				if (this.text.toLowerCase().contains("#"+hashTag.toLowerCase()) == false)
					throw new IllegalStateException("Tweet doesn't contain hashtag "+hashTag.toLowerCase());
			}
		}
		
		// Mentions
		if (this.mentions != null)
		{
			Numbers.lessThan(this.mentions.size(), TweetParticle.MAX_MENTIONS, "Mentions exceeds max of "+TweetParticle.MAX_MENTIONS);
			for (String mention : this.mentions)
			{
				Numbers.notZero(mention.length(), "Mention length is zero");
				if (this.text.toLowerCase().contains("@"+mention.toLowerCase()) == false)
					throw new IllegalStateException("Tweet doesn't contain mention "+mention.toLowerCase());
			}
		}
		
		// Media 
		if (this.media != null)
			Numbers.lessThan(this.media.size(), TweetParticle.MAX_MEDIA, "Maximum media items is "+TweetParticle.MAX_MEDIA);
		
		if (this.replyTo != null)
			Hash.notZero(this.replyTo, "Reply hash is ZERO");

		if (this.retweetOf != null)
			Hash.notZero(this.retweetOf, "Retweet hash is ZERO");
		
		if (this.replyTo != null && this.retweetOf != null)
			throw new IllegalStateException("Tweets can not be a reply and retweet");

		// User
		Objects.requireNonNull(this.user, "User is null");
		Numbers.inRange(this.user.length(), TwitterUserRegistration.MIN_HANDLE_LENGTH, TwitterUserRegistration.MAX_HANDLE_LENGTH, "User Handle "+this.user+" length "+this.user.length()+" is not in range "+TwitterUserRegistration.MIN_HANDLE_LENGTH+" -> "+TwitterUserRegistration.MAX_HANDLE_LENGTH);
	}
	
	@Override
	public void prepare(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException 
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
		
		stateMachine.sop(new StateOp(new StateAddress(TweetParticle.class, this.id), Instruction.NOT_EXISTS), this);
		stateMachine.sop(new StateOp(new StateField(this.id, "replies"), Instruction.NOT_EXISTS), this);
		stateMachine.sop(new StateOp(new StateField(this.id, "retweets"), Instruction.NOT_EXISTS), this);

		stateMachine.sop(new StateOp(new StateAddress(TwitterUserRegistration.class, Hash.from(this.user.toLowerCase())), Instruction.EXISTS), this);
		
		if (this.replyTo != null)
		{
			stateMachine.sop(new StateOp(new StateAddress(TweetParticle.class, this.replyTo), Instruction.EXISTS), this);
			stateMachine.sop(new StateOp(new StateField(this.replyTo, "replies"), Instruction.GET), this);
		}
			
		if (this.retweetOf != null)
		{
			stateMachine.sop(new StateOp(new StateAddress(TweetParticle.class, this.retweetOf), Instruction.EXISTS), this);
			stateMachine.sop(new StateOp(new StateField(this.retweetOf, "retweets"), Instruction.GET), this);
		}
		
		if (this.media != null)
			this.media.forEach(media -> stateMachine.sop(new StateOp(new StateAddress(Particle.class, media), Instruction.EXISTS), this));

		super.prepare(stateMachine, arguments);
	}
	
	@Override
	public void execute(StateMachine stateMachine, Object... arguments) throws ValidationException, IOException
	{
		stateMachine.sop(new StateOp(new StateAddress(TweetParticle.class, this.id), UInt256.from(this.id.toByteArray()), Instruction.SET), this);
		stateMachine.sop(new StateOp(new StateField(this.id, "replies"), UInt256.ZERO, Instruction.SET), this);
		stateMachine.sop(new StateOp(new StateField(this.id, "retweets"), UInt256.ZERO, Instruction.SET), this);

		if (this.replyTo != null && this.replyTo.equals(Hash.ZERO) == false)
		{
			Optional<UInt256> replies = stateMachine.getInput(new StateField(this.replyTo, "replies"));
			stateMachine.sop(new StateOp(new StateField(this.replyTo, "replies"), replies.get().increment(), Instruction.SET), this);
			stateMachine.associate(this.replyTo, this);
		}
			
		if (this.retweetOf != null && this.retweetOf.equals(Hash.ZERO) == false)
		{
			Optional<UInt256> retweets = stateMachine.getInput(new StateField(this.retweetOf, "retweets"));
			stateMachine.sop(new StateOp(new StateField(this.retweetOf, "retweets"), retweets.get().increment(), Instruction.SET), this);
		}
		
		// ASSOCIATIONS //
		Set<Hash> associations = new HashSet<Hash>();
		associations.add(Hash.from(this.user));

		// Concatenated user + owner for faster searches
		associations.add(Hash.from(Hash.from(this.user), getOwner().asHash()));
		
		if (this.hashtags != null)
		{
			for (String hashTag : this.hashtags)
				associations.add(Hash.from(hashTag));
		}

		if (this.mentions != null)
		{
			for (String mention : this.mentions)
				associations.add(Hash.from(mention));
		}
		
		for (Hash association : associations)
			stateMachine.associate(association, this);
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
