package org.fuserleer.crypto;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.fuserleer.utils.Bytes;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.UInt256;

import com.google.common.primitives.UnsignedBytes;

public final class Hash implements Comparable<Hash> 
{
	public enum Mode
	{
		STANDARD, DOUBLE
	}
	
	private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();
	private static final SecureRandom secureRandom = new SecureRandom();
	private static HashHandler hasher = new SHAHashHandler();

	public static final int BYTES = 32;
    public static final int BITS = BYTES * Byte.SIZE;
    public static final Hash ZERO = new Hash(new byte[BYTES]);

    public static void notZero(final Hash hash)
    {
    	notZero(hash, "Hash is ZERO");
    }

    public static void notZero(final Hash hash, final String message)
    {
    	if (hash.equals(Hash.ZERO) == true)
    		throw new IllegalArgumentException(message);
    }
    
    public static Hash random() 
    {
		byte[] randomBytes = new byte[BYTES];
		secureRandom.nextBytes(randomBytes);
		return new Hash(randomBytes, Mode.STANDARD);
	}
    
	public static Hash from(final Object object)
	{
		Objects.requireNonNull(object, "Object is null");

		if (Hashable.class.isAssignableFrom(object.getClass()) == true)
			return ((Hashable) object).getHash();

		if (Hash.class.isAssignableFrom(object.getClass()) == true)
			return ((Hash) object);

		if (UInt256.class.isAssignableFrom(object.getClass()) == true)
			return new Hash(((UInt256) object).toByteArray());

		if (String.class.isAssignableFrom(object.getClass()) == true)
			return new Hash((String) object, Mode.STANDARD);
		
		if (Number.class.isAssignableFrom(object.getClass()) == true)
			return new Hash(Longs.toByteArray(((Number) object).longValue()), Mode.STANDARD);

		if (byte[].class.isAssignableFrom(object.getClass()) == true)
			return new Hash((byte[]) object, Mode.STANDARD);
		
		throw new IllegalArgumentException("Objects of type "+object.getClass()+" not supported for Hash");
	}
	
	public static Hash from(final Hash ... hashes)
	{
		if (Objects.requireNonNull(hashes, "Hashes is null").length == 0)
			throw new IllegalArgumentException("Hashes is empty");

		return from(Arrays.asList(hashes));
	}
	
	public static Hash from(final List<Hash> hashes)
	{
		if (Objects.requireNonNull(hashes, "Hashes is null").isEmpty() == true)
			throw new IllegalArgumentException("Hashes is empty");

		List<Hash> sortedHashes = new ArrayList<Hash>(hashes);
		Collections.sort(sortedHashes);
		byte[] concatenatedData = new byte[Hash.BYTES * sortedHashes.size()];
		int concatenatedOffset = 0;
		for (Hash hash : sortedHashes)
		{
			System.arraycopy(hash.toByteArray(), 0, concatenatedData, concatenatedOffset, Hash.BYTES);
			concatenatedOffset += Hash.BYTES;
		}

		return new Hash(concatenatedData, Mode.STANDARD);
	}

	private final byte[] 	data;

	// Hashcode caching
	private transient boolean 	hashCodeComputed = false;
	private transient int 		hashCode;
	private transient boolean 	longComputed = false;
	private transient long 		asLong;

	public Hash(final byte[] data, final Mode mode) 
	{
		this(mode.equals(Mode.STANDARD) == true ? Hash.hasher.hash256(data) : Hash.hasher.doubleHash256(data), 0, BYTES);
	}

	public Hash(final Hash hash0, final Hash hash1, final Mode mode) 
	{
		this(mode.equals(Mode.STANDARD) == true ? Hash.hasher.hash256(hash0.toByteArray(), hash1.toByteArray()) : Hash.hasher.doubleHash256(hash0.toByteArray(), hash1.toByteArray()), 0, BYTES);
	}

	public Hash(final byte[] data0, final byte[] data1, final Mode mode) 
	{
		this(mode.equals(Mode.STANDARD) == true ? Hash.hasher.hash256(data0, data1) : Hash.hasher.doubleHash256(data0, data1), 0, BYTES);
	}

	public Hash(final byte[] hash) 
	{
		Objects.requireNonNull(hash, "Hash bytes is null");
		if (hash.length != BYTES)
			throw new IllegalArgumentException("Digest length must be " + BYTES + " bytes for Hash, was " + hash.length);

		this.data = new byte[BYTES];
		System.arraycopy(hash, 0, this.data, 0, BYTES);
	}

	public Hash(final byte[] hash, final int offset, final int length) 
	{
		Objects.requireNonNull(hash, "Hash bytes is null");

		if (length != BYTES)
			throw new IllegalArgumentException("Digest length must be " + BYTES + " bytes for Hash, was " + length);

		if (offset + length > hash.length)
			throw new IllegalArgumentException(String.format("Hash length must be at least %s for offset %s, but was %s", offset + length, offset, hash.length));

		this.data = new byte[BYTES];
		System.arraycopy(hash, offset, this.data, 0, BYTES);
	}

	public Hash(String hex) 
	{
		Objects.requireNonNull(hex, "Hash hex string is null");
		if (hex.length() != (BYTES * 2)) 
			throw new IllegalArgumentException(String.format("Digest length must be %s hex characters for Hash, was %s", BYTES * 2, hex.length()));

		if (hex.startsWith("0x") == true)
			hex = hex.substring(2);
		
		this.data = Bytes.fromHexString(hex);
	}

	public Hash(final String data, final Mode mode) 
	{
		this(mode.equals(Mode.STANDARD) == true ? Hash.hasher.hash256(data.getBytes(StandardCharsets.UTF_8)) : Hash.hasher.doubleHash256(data.getBytes(StandardCharsets.UTF_8)), 0, BYTES);
	}

	/**
	 * Retrieve the hash bytes.
	 * <p>
	 * Note that for performance reasons, the underlying array is returned.
	 * If callers are passing this array to mutating methods, a copy should
	 * be taken.
	 *
	 * @return The hash data
	 */
	public byte[] toByteArray() 
	{
		return this.data;
	}
	
	public Hash invert()
	{
		byte[] inverted = new byte[Hash.BYTES];
		for (int i = 0 ; i < Hash.BYTES ; i++)
			inverted[i] = (byte) (0xFF - (this.data[i] & 0xFF));
		
		return new Hash(inverted);
	}

	public void copyTo(final byte[] array, final int offset) 
	{
		copyTo(array, offset, BYTES);
	}

	public void copyTo(final byte[] array, final int offset, final int length) 
	{
		Objects.requireNonNull(array, "Hash destination array is null");
		
		if (array.length - offset < BYTES) 
			throw new IllegalArgumentException(String.format("Array must be bigger than offset + %d but was %d", BYTES, array.length));

		System.arraycopy(this.data, 0, array, offset, length);
	}

	public byte getFirstByte() 
	{
		return data[0];
	}

	@Override
	public int compareTo(final Hash object) 
	{
		Objects.requireNonNull(object, "Hash comparison is null");
		return COMPARATOR.compare(this.data, object.data);
	}

	@Override
	public String toString () 
	{
		return Bytes.toHexString(this.data);
	}

	@Override
	public boolean equals(Object o) 
	{
		if (o == this)
			return true;

		if (o instanceof Hash) 
		{
			Hash other = (Hash) o;

			if (this.hashCode() == other.hashCode())
				return Arrays.equals(this.data, other.data);
		}

		return false;
	}

	@Override
	public int hashCode() 
	{
		if (this.hashCodeComputed == false) 
		{
			this.hashCode = Arrays.hashCode(this.data);
			this.hashCodeComputed = true;
		}
		
		return this.hashCode;
	}
	
	public long asLong() 
	{
		if (this.longComputed == false) 
		{
			this.asLong = Longs.fromByteArray(this.data);
			this.longComputed = true;
		}
		
		return this.asLong;
	}
}
