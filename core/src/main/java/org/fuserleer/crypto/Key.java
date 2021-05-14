package org.fuserleer.crypto;

import java.util.Arrays;
import java.util.Objects;

import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.utils.Bytes;
import org.fuserleer.utils.Longs;

import com.google.common.primitives.SignedBytes;

public abstract class Key implements Comparable<Key>
{
	private Integer hashCode;
	private Hash hash;
	
	public abstract boolean canSign();

	public abstract boolean canVerify();
	
	public abstract Identity getIdentity();

	public final int length() 
    {
        return toByteArray().length;
    }
	
	public abstract byte[] toByteArray();
	
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
	
    public final synchronized Hash asHash()
	{
    	if (this.hash == null)
    		// Trim off the type prefix and return the raw X coordinate
    		this.hash = new Hash(toByteArray(), Mode.STANDARD);
    	
    	return this.hash;
	}

	@Override
	public final String toString() 
	{
		return Bytes.toBase64String(toByteArray());
	}

	@Override
	public final int compareTo(final Key other)
	{
		Objects.requireNonNull(other, "Key for compare is null");
		
		return SignedBytes.lexicographicalComparator().compare(this.toByteArray(), other.toByteArray());
	}
}
