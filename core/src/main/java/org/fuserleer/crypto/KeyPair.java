package org.fuserleer.crypto;

import java.util.Objects;

import org.fuserleer.utils.Bytes;

public abstract class KeyPair<P extends PrivateKey, K extends PublicKey, S extends Signature>
{
	public abstract P getPrivateKey(); 

	public abstract K getPublicKey(); 

	public final S sign(final Hash hash) throws CryptoException
	{
		Objects.requireNonNull(hash, "Hash to sign is null");
		return sign(hash.toByteArray());
	}

	public abstract S sign(byte[] hash) throws CryptoException; 

	@Override
	public final boolean equals(Object object) 
	{
		if (this == object)
			return true;

		if (object instanceof KeyPair) 
		{
			KeyPair<?, ?, ?> other = (KeyPair<?, ?, ?>) object;
			// Comparing private keys should be sufficient
			return this.getPrivateKey().equals(other.getPrivateKey());
		}
		
		return false;
	}

	@Override
	public final int hashCode() 
	{
		return this.getPrivateKey().hashCode();
	}

	@Override
	public final String toString() 
	{
		// Not going to print the private key here DUH
		return String.format("%s[%s]", getClass().getSimpleName(), Bytes.toBase64String(getPublicKey().toByteArray()));
	}
}
