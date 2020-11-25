package org.fuserleer;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.primitives.Longs;

import java.io.IOException;
import java.math.BigInteger;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import org.json.JSONObject;
import org.fuserleer.BasicObject;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignature;
import org.fuserleer.ledger.Block;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Bytes;

@SerializerId2("universe")
public class Universe extends BasicObject
{
	private static Universe instance = null;
	
	public static Universe getDefault()
	{
		if (instance == null)
			throw new RuntimeException("Configuration not set");
		
		return instance;
	}

	public static Universe createAsDefault(JSONObject universe) throws IOException
	{
		if (instance != null)
			throw new RuntimeException("Default configuration already set");
		
		instance = Serialization.getInstance().fromJsonObject(universe, Universe.class);
		
		return instance;
	}

	public static Universe createAsDefault(byte[] universe) throws IOException
	{
		if (instance != null)
			throw new RuntimeException("Default configuration already set");
		
		instance = Serialization.getInstance().fromDson(universe, Universe.class);
		
		return instance;
	}
	
	public static Universe clearDefault()
	{
		Universe universe = getDefault();
		instance = null;
		return universe;
	}


	/**
	 * Universe builder.
	 */
	public static class Builder 
	{
		private Integer port;
		private String name;
		private String description;
		private Type type;
		private Long timestamp;
		private Integer epoch;
		private ECPublicKey creator;
		private Block genesis;
		private LinkedHashSet<ECPublicKey> genodes;

		private Builder() 
		{
			// Nothing to do here
		}

		/**
		 * Sets the TCP/UDP port for the universe.
		 *
		 * @param port The TCP/UDP port for the universe to use, {@code 0 <= port <= 65,535}.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder port(int port) 
		{
			if (port < 0 || port > 65535)
				throw new IllegalArgumentException("Invalid port number: " + port);

			this.port = port;
			return this;
		}

		/**
		 * Sets the name of the universe.
		 * Ideally the universe name is a short identifier for the universe.
		 *
		 * @param name The name of the universe.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder name(String name) 
		{
			this.name = Objects.requireNonNull(name);
			return this;
		}

		/**
		 * Set the description of the universe.
		 * The universe description is a longer description of the universe.
		 *
		 * @param description The description of the universe.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder description(String description) 
		{
			this.description = Objects.requireNonNull(description);
			return this;
		}

		/**
		 * Sets the type of the universe, one of {@link Universe.Type}.
		 *
		 * @param type The type of the universe.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder type(Type type) 
		{
			this.type = Objects.requireNonNull(type);
			return this;
		}

		/**
		 * Sets the creation timestamp of the universe.
		 *
		 * @param timestamp The creation timestamp of the universe.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder timestamp(long timestamp) 
		{
			if (timestamp < 0)
				throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
			
			this.timestamp = timestamp;
			return this;
		}

		/**
		 * Sets the epoch duration in events of the universe.
		 *
		 * @param epoch The event duration for an epoch of the universe.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder epoch(int epoch) 
		{
			if (epoch < 0)
				throw new IllegalArgumentException("Invalid epoch: " + epoch);
			
			this.epoch = epoch;
			return this;
		}

		/**
		 * Sets the universe creators public key.
		 *
		 * @param creator The universe creators public key.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder creator(ECPublicKey creator) 
		{
			this.creator = Objects.requireNonNull(creator);
			return this;
		}

		/**
		 * Sets the genesis.
		 *
		 * @param genesis The genesis.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder setGenesis(Block genesis) 
		{
			Objects.requireNonNull(genesis);
			this.genesis = genesis;
			return this;
		}

		/**
		 * Sets the genesis nodes.
		 *
		 * @param genodes The genesis nodes.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder setGenodes(Set<ECPublicKey> genodes) 
		{
			Objects.requireNonNull(genodes);
			this.genodes = new LinkedHashSet<ECPublicKey>(genodes);
			return this;
		}
		
		/**
		 * Validate and build a universe from the specified data.
		 *
		 * @return The freshly build universe object.
		 */
		public Universe build() 
		{
			require(this.port, "Port number");
			require(this.name, "Name");
			require(this.description, "Description");
			require(this.type, "universeype");
			require(this.timestamp, "Timestamp");
			require(this.creator, "Creator");
			require(this.genesis , "Genesis block");
			require(this.genodes, "Genesis nodes");
			return new Universe(this);
		}

		private void require(Object item, String what) 
		{
			if (item == null)
				throw new IllegalStateException(what + " must be specified");
		}
	}

	/**
	 * Construct a new {@link Builder}.
	 *
	 * @return The freshly constructed builder.
	 */
	public static Builder newBuilder() 
	{
		return new Builder();
	}

	/**
	 * Computes universe magic number from specified parameters.
	 *
	 * @param creator {@link ECPublicKey} of universe creator to use when calculating universe magic
	 * @param timestamp universe timestamp to use when calculating universe magic
	 * @param port universe port to use when calculating universe magic
	 * @param type universe type to use when calculating universe magic
	 * @return The universe magic
	 */
	public static long computeMagic(ECPublicKey creator, long timestamp, int epoch, int port, Type type) 
	{
		return 31l * Longs.fromByteArray(creator.getBytes()) * 19l * timestamp * 13l * epoch * 7l * port + type.ordinal();
	}

	public enum Type
	{
		PRODUCTION,
		TEST,
		DEVELOPMENT;
		
		@JsonValue
		@Override
		public String toString() 
		{
			return this.name();
		}
	}

	@JsonProperty("name")
	@DsonOutput(Output.ALL)
	private String name;

	@JsonProperty("description")
	@DsonOutput(Output.ALL)
	private String description;

	@JsonProperty("timestamp")
	@DsonOutput(Output.ALL)
	private long timestamp;

	@JsonProperty("port")
	@DsonOutput(Output.ALL)
	private int	port;

	@JsonProperty("epoch")
	@DsonOutput(Output.ALL)
	private int	epoch;

	@JsonProperty("type")
	@DsonOutput(Output.ALL)
	private Type type;

	@JsonProperty("genesis")
	@DsonOutput(Output.ALL)
	private Block genesis;

	@JsonProperty("genodes")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=LinkedHashSet.class)
	private Set<ECPublicKey> genodes;

	private ECPublicKey creator;

	private ECSignature signature;
	private transient BigInteger sigR;
	private transient BigInteger sigS;

	Universe() {
		// No-arg constructor for serializer
	}

	private Universe(Builder builder) 
	{
		super();

		this.port = builder.port.intValue();
		this.name = builder.name;
		this.description = builder.description;
		this.type = builder.type;
		this.epoch = builder.epoch;
		this.timestamp = builder.timestamp.longValue();
		this.creator = builder.creator;
		this.genesis = builder.genesis;
		this.genodes = builder.genodes;
	}

	/**
	 * Magic identifier for universe.
	 *
	 * @return
	 */
	@JsonProperty("magic")
	@DsonOutput(value = Output.HASH, include = false)
	public long getMagic() 
	{
		return computeMagic(this.creator, this.timestamp, this.epoch, this.port, this.type);
	}

	/**
	 * The size of an epoch in events.
	 *
	 * @return
	 */
	public int getEpoch()
	{
		return this.epoch;
	}

	public long toEpoch(long height)
	{
		return height / this.epoch;
	}

	/**
	 * The name of the universe.
	 *
	 * @return
	 */
	public String getName()
	{
		return this.name;
	}

	/**
	 * The universe description.
	 *
	 * @return
	 */
	public String getDescription()
	{
		return this.description;
	}

	/**
	 * The default TCP/UDP port for the universe.
	 *
	 * @return
	 */
	public int getPort()
	{
		return this.port;
	}

	/**
	 * The UTC 'BigBang' timestamp for the universe.
	 *
	 * @return
	 */
	public long getTimestamp()
	{
		return this.timestamp;
	}

	/**
	 * Whether this is a production universe.
	 *
	 * @return
	 */
	public boolean isProduction()
	{
		return this.type.equals(Type.PRODUCTION);
	}

	/**
	 * Whether this is a test universe.
	 *
	 * @return
	 */
	public boolean isTest()
	{
		return this.type.equals(Type.TEST);
	}

	/**
	 * Whether this is a development universe.
	 *
	 * @return
	 */
	public boolean isDevelopment()
	{
		return this.type.equals(Type.DEVELOPMENT);
	}

	/**
	 * Gets this universe genesis.
	 *
	 * @return
	 */
	public Block getGenesis()
	{
		return this.genesis;
	}
	
	/**
	 * Gets this universe genesis nodes.
	 *
	 * @return
	 */
	public Set<ECPublicKey> getGenodes()
	{
		return this.genodes;
	}

	/**
	 * Get creator key.
	 *
	 * @return
	 */
	public ECPublicKey getCreator()
	{
		return this.creator;
	}

	public ECSignature getSignature()
	{
		return this.signature;
	}

	public void setSignature(ECSignature signature)
	{
		this.signature = signature;
	}

	public void sign(ECKeyPair key) throws CryptoException
	{
		this.signature = key.sign(getHash());
	}

	public boolean verify(ECPublicKey key)
	{
		return key.verify(getHash(), this.signature);
	}
	
	/**
	 * Check whether a given universe is valid
	 */
	public void validate() 
	{
		// Check signature
		if (this.creator.verify(getHash(), this.signature) == false)
			throw new IllegalStateException("Invalid universe signature");
		
		if (this.genesis == null)
			throw new IllegalStateException("No genesis block defined");
	}

	// Signature - 1 getter, 1 setter.
	// Better option would be to make public keys primitive types as they are
	// very common, or alternatively serialize as an embedded object.
	@JsonProperty("creator")
	@DsonOutput(Output.ALL)
	private byte[] getJsonCreator() 
	{
		return this.creator.getBytes();
	}

	@JsonProperty("creator")
	private void setJsonCreator(byte[] bytes) throws CryptoException 
	{
		this.creator = ECPublicKey.from(bytes);
	}

	// Signature - 2 getters, 2 setters.
	// FIXME: Better option would be to serialize as a ECSignature embedded object
	// rather than the two individual components directly in the object.
	@JsonProperty("signature.r")
	@DsonOutput(value = Output.HASH, include = false)
	private byte[] getJsonSignatureR() 
	{
		return Bytes.trimLeadingZeros(this.signature.getR().toByteArray());
	}

	@JsonProperty("signature.s")
	@DsonOutput(value = Output.HASH, include = false)
	private byte[] getJsonSignatureS() 
	{
		return Bytes.trimLeadingZeros(this.signature.getS().toByteArray());
	}

	@JsonProperty("signature.r")
	private void setJsonSignatureR(byte[] r) 
	{
		// Set sign to positive to stop BigInteger interpreting high bit as sign
		this.sigR = new BigInteger(1, r);
		if (this.sigS != null) 
		{
			this.signature = new ECSignature(this.sigR, this.sigS);
			this.sigS = null;
			this.sigR = null;
		}
	}

	@JsonProperty("signature.s")
	private void setJsonSignatureS(byte[] s) {
		// Set sign to positive to stop BigInteger interpreting high bit as sign
		this.sigS = new BigInteger(1, s);
		if (this.sigR != null) 
		{
			this.signature = new ECSignature(this.sigR, this.sigS);
			this.sigS = null;
			this.sigR = null;
		}
	}
}
