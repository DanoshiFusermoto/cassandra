package org.fuserleer.crypto;

import java.util.Arrays;

public abstract class Signature
{
	private Integer hashCode;

	public abstract byte[] toByteArray(); 

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

		if (object instanceof Signature) 
		{
			Signature other = (Signature) object;
			return Arrays.equals(other.toByteArray(), toByteArray());
		}
		
		return false;
	}
}
