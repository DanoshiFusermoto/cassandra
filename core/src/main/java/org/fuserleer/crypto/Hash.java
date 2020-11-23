package org.fuserleer.crypto;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Comparator;

import org.fuserleer.utils.Bytes;
import org.fuserleer.utils.Longs;

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

    public static Hash random() 
    {
		byte[] randomBytes = new byte[BYTES];

		secureRandom.nextBytes(randomBytes);

		return new Hash(randomBytes, Mode.STANDARD);
	}

	private final byte[] 	data;

	// Hashcode caching
	private transient boolean 	hashCodeComputed = false;
	private transient int 		hashCode;
	private transient boolean 	longComputed = false;
	private transient long 		asLong;

	public Hash(byte[] data, Mode mode) 
	{
		this(mode.equals(Mode.STANDARD) == true ? Hash.hasher.hash256(data) : Hash.hasher.doubleHash256(data), 0, BYTES);
	}

	public Hash(Hash hash0, Hash hash1, Mode mode) 
	{
		this(mode.equals(Mode.STANDARD) == true ? Hash.hasher.hash256(hash0.toByteArray(), hash1.toByteArray()) : Hash.hasher.doubleHash256(hash0.toByteArray(), hash1.toByteArray()), 0, BYTES);
	}

	public Hash(byte[] data0, byte[] data1, Mode mode) 
	{
		this(mode.equals(Mode.STANDARD) == true ? Hash.hasher.hash256(data0, data1) : Hash.hasher.doubleHash256(data0, data1), 0, BYTES);
	}

	public Hash(byte[] hash) 
	{
		if (hash.length != BYTES)
			throw new IllegalArgumentException("Digest length must be " + BYTES + " bytes for Hash, was " + hash.length);

		this.data = new byte[BYTES];
		System.arraycopy(hash, 0, this.data, 0, BYTES);
	}

	public Hash(byte[] hash, int offset, int length) 
	{
		if (length != BYTES)
			throw new IllegalArgumentException("Digest length must be " + BYTES + " bytes for Hash, was " + length);

		if (offset + length > hash.length)
			throw new IllegalArgumentException(String.format(
				"Hash length must be at least %s for offset %s, but was %s", offset + length, offset, hash.length));

		this.data = new byte[BYTES];
		System.arraycopy(hash, offset, this.data, 0, BYTES);
	}

	public Hash(String hex) 
	{
		if (hex.startsWith("0x") == true)
			hex = hex.substring(2);
		
		if (hex.length() != (BYTES * 2)) 
			throw new IllegalArgumentException(String.format("Digest length must be %s hex characters for Hash, was %s", BYTES * 2, hex.length()));

		this.data = Bytes.fromHexString(hex);
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

	public void copyTo(byte[] array, int offset) 
	{
		copyTo(array, offset, BYTES);
	}

	public void copyTo(byte[] array, int offset, int length) 
	{
		if (array.length - offset < BYTES) 
			throw new IllegalArgumentException(String.format("Array must be bigger than offset + %d but was %d", BYTES, array.length));

		System.arraycopy(this.data, 0, array, offset, length);
	}

	public byte getFirstByte() 
	{
		return data[0];
	}

	@Override
	public int compareTo(Hash object) 
	{
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
