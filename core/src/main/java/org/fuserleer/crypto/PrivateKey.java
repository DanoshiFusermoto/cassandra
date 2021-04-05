package org.fuserleer.crypto;

import java.util.Arrays;

public abstract class PrivateKey
{
	private Integer hashCode;

	public abstract byte[] toByteArray();
	
	public final int length() 
    {
        return toByteArray().length;
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

		if (object instanceof PrivateKey) 
		{
			PrivateKey other = (PrivateKey) object;
			return Arrays.equals(other.toByteArray(), toByteArray());
		}
		
		return false;
	}

}
