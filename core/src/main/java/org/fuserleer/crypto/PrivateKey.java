package org.fuserleer.crypto;

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
}
