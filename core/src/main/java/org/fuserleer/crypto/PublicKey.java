package org.fuserleer.crypto;

import java.util.Arrays;
import java.util.Objects;

import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.utils.Bytes;
import org.fuserleer.utils.Longs;

import com.google.common.primitives.SignedBytes;

public abstract class PublicKey implements Comparable<PublicKey>
{
	private Hash hash;
	private Integer hashCode;
	
	public abstract byte[] toByteArray(); 
	
	public final boolean verify(final Hash hash, final Signature signature) throws CryptoException
	{
		return verify(Objects.requireNonNull(hash, "Hash to verify is null").toByteArray(), signature);
	}

	public abstract boolean verify(final byte[] hash, final Signature signature)  throws CryptoException;

	public final int length() 
    {
        return toByteArray().length;
    }
    
    public final synchronized Hash asHash()
	{
    	if (this.hash == null)
    		// Trim off the type prefix and return the raw X coordinate
    		this.hash = new Hash(toByteArray(), Mode.STANDARD);
    	
    	return this.hash;
	}

    public final long asLong()
	{
    	// Create a long from offset 1 as the first byte is usually a type indicator
    	return Longs.fromByteArray(toByteArray(), 1);
	}
    
	@Override
	public final synchronized int hashCode() 
	{
		if (this.hashCode == null)
			this.hashCode = Arrays.hashCode(toByteArray());
		
		return (int) this.hashCode;
	}

	@Override
	public final boolean equals(Object object) 
	{
		if (object == this)
			return true;

		if (object instanceof PublicKey) 
		{
			PublicKey other = (PublicKey) object;
			return Arrays.equals(other.toByteArray(), toByteArray());
		}
		
		return false;
	}

	@Override
	public final String toString() 
	{
		return Bytes.toBase64String(toByteArray());
	}

	@Override
	public final int compareTo(final PublicKey other)
	{
		Objects.requireNonNull(other, "ECPublicKey for compare is null");
		
		return SignedBytes.lexicographicalComparator().compare(this.toByteArray(), other.toByteArray());
	}

}
