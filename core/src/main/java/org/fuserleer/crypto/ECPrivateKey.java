package org.fuserleer.crypto;

import java.util.Objects;

import org.fuserleer.utils.Bytes;

public final class ECPrivateKey extends PrivateKey
{
	public static final int	BYTES = 32;

	private byte[] privateKey;
	
	public static ECPrivateKey from(byte[] key) throws CryptoException 
	{
		return new ECPrivateKey(key);
	}
	
	public static ECPrivateKey from(String key) throws CryptoException 
	{
		return new ECPrivateKey(key);
	}

	ECPrivateKey(String key) throws CryptoException
	{
		this(Bytes.fromBase64String(Objects.requireNonNull(key, "Key string is null")));
	}

	private ECPrivateKey(byte[] key) throws CryptoException 
	{
		Objects.requireNonNull(key, "Key bytes is null");
		
		try {
			validatePrivate(key);
			this.privateKey = trimPrivateKey(key);
		} catch (CryptoException ex) {
			throw ex;
		}
		catch (Exception ex) {
			throw new CryptoException(ex);
		}
	}

	private byte[] trimPrivateKey(byte[] privKey) 
	{
		if (privKey.length > BYTES && privKey[0] == 0) 
		{
			byte[] tmp = new byte[privKey.length - 1];
			System.arraycopy(privKey, 1, tmp, 0, privKey.length - 1);
			return tmp;
		}
		
		if (privKey.length < BYTES) 
		{
			byte[] tmp = new byte[BYTES];
			System.arraycopy(privKey, 0, tmp, BYTES - privKey.length, privKey.length);
		}
		
		return privKey;
	}

	private void validatePrivate(byte[] privateKey) throws CryptoException 
	{
		if (privateKey == null || privateKey.length == 0)
			throw new CryptoException("Private key is null");

		int pklen = privateKey.length;
		if (allZero(privateKey, 0, pklen))
			throw new CryptoException("Private key is zero");

		if (allZero(privateKey, 0, pklen - 1) && privateKey[pklen - 1] == 1)
			throw new CryptoException("Private key is one");
	}
	
	private boolean allZero(byte[] bytes, int offset, int len) 
	{
		for (int i = 0; i < len; ++i) 
		{
			if (bytes[offset + i] != 0)
				return false;
		}
		return true;
	}
	
	public byte[] toByteArray()
	{
		return this.privateKey;
	}
}
