package org.fuserleer.crypto;

import java.util.Objects;

public abstract class PublicKey extends Key
{
	@Override
	public final boolean canSign()
	{
		return false;
	}

	@Override
	public final boolean canVerify()
	{
		return true;
	}

	public final boolean verify(final Hash hash, final Signature signature) throws CryptoException
	{
		return verify(Objects.requireNonNull(hash, "Hash to verify is null").toByteArray(), signature);
	}

	public abstract boolean verify(final byte[] hash, final Signature signature)  throws CryptoException;
}
