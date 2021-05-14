package org.fuserleer.crypto;

import java.util.Arrays;

public abstract class PrivateKey extends Key
{
	@Override
	public final boolean canSign()
	{
		return true;
	}

	@Override
	public final boolean canVerify()
	{
		return false;
	}

	public final Identity getIdentity()
	{
		throw new UnsupportedOperationException("Private keys do not support identites");
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
