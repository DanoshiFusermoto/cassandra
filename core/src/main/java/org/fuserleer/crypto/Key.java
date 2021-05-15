package org.fuserleer.crypto;

import java.util.Arrays;
import java.util.Objects;

import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.utils.Base58;
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
	
	@Override
	public final boolean equals(Object object) 
	{
		if (object == this)
			return true;

		if (object instanceof Key) 
		{
			Key other = (Key) object;
			return Arrays.equals(other.toByteArray(), toByteArray());
		}
		
		return false;
	}
	
    public final synchronized Hash asHash()
	{
    	if (this.hash == null)
    		this.hash = computeHash();
    	
    	return this.hash;
	}
    
    Hash computeHash()
    {
		return new Hash(toByteArray(), Mode.STANDARD);
    }

	@Override
	public final String toString() 
	{
		return Base58.toBase58(toByteArray());
	}

	@Override
	public final int compareTo(final Key other)
	{
		Objects.requireNonNull(other, "Key for compare is null");
		
		return SignedBytes.lexicographicalComparator().compare(this.toByteArray(), other.toByteArray());
	}
}
