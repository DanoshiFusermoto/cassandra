package org.fuserleer.crypto;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.primitives.SignedBytes;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

import org.bouncycastle.math.ec.ECPoint;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.utils.Bytes;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Asymmetric EC public key provider fixed to curve 'secp256k1'
 */
public final class ECPublicKey implements Comparable<ECPublicKey>
{
	public static final int	BYTES = 32;
	
	@JsonValue
	private final byte[] publicKey;

	private transient long hashCode = Long.MAX_VALUE;

	@JsonCreator
	public static ECPublicKey from(byte[] key) throws CryptoException 
	{
		return new ECPublicKey(key);
	}
	
	public static ECPublicKey from(String key) throws CryptoException 
	{
		return new ECPublicKey(key);
	}

	ECPublicKey(String key) throws CryptoException
	{
		this(Bytes.fromBase64String(key));
	}

	private ECPublicKey(byte[] key) throws CryptoException 
	{
		try {
			validatePublic(key);
			this.publicKey = Arrays.copyOf(key, key.length);
		} catch (CryptoException ex) {
			throw ex;
		}
		catch (Exception ex) {
			throw new CryptoException(ex);
		}
	}

	private void validatePublic(byte[] publicKey) throws CryptoException 
	{
		if (publicKey == null)
			throw new CryptoException("Public key is null");

		int pubkey0 = publicKey[0] & 0xFF;
		if (pubkey0 != 2 && pubkey0 != 3 && pubkey0 != 4)
			throw new CryptoException("Public key is an invalid format");

		if (pubkey0 == 4 && publicKey.length != (BYTES * 2) + 1)
			throw new CryptoException("Public key is an invalid uncompressed size");

		if ((pubkey0 == 2 || pubkey0 == 3) && publicKey.length != BYTES + 1)
			throw new CryptoException("Public key is an invalid compressed size");

		// TODO want to check Y value for compressed pub keys?
		// What are the performance implications?
	}


	public byte[] getBytes() 
	{
		return this.publicKey;
	}
	
    public int length() 
    {
        return this.publicKey.length;
    }
    
    public Hash asHash()
	{
		// Trim off the type prefix and return the raw X coordinate
		return new Hash(this.publicKey, Mode.STANDARD);
	}

	ECPoint getPublicPoint()
	{
		return ECKeyUtils.spec.getCurve().decodePoint(this.publicKey);
	}

	public boolean verify(Hash hash, ECSignature signature) {
		return verify(hash.toByteArray(), signature);
	}

	public boolean verify(byte[] hash, ECSignature signature) 
	{
		if (signature == null)
			return false;

		try 
		{
			return ECKeyUtils.keyHandler.verify(hash, signature, this.publicKey);
		} 
		catch (CryptoException e) 
		{
			return false;
		}
	}

	public byte[] encrypt(byte[] data) throws CryptoException 
	{
        byte[] iv = new byte[16];
        ECKeyUtils.secureRandom.nextBytes(iv);
		return encrypt(data, iv);
	}

	public byte[] encrypt(byte[] data, byte[] iv) throws CryptoException 
	{
		try 
		{
			// 1. The destination is this.getPublicKey()
	        // 2. Generate 16 random bytes using a secure random number generator. Call them IV
			// IV is passed in

	        // 3. Generate a new ephemeral EC key pair
			ECKeyPair ephemeral = new ECKeyPair();

	        // 4. Do an EC point multiply with this.getPublicKey() and ephemeral private key. This gives you a point M.
	        ECPoint m = getPublicPoint().multiply(new BigInteger(1, ephemeral.getPrivateKey())).normalize();

	        // 5. Use the X component of point M and calculate the SHA512 hash H.
	        byte[] h = new Hash(m.getXCoord().getEncoded(), Mode.STANDARD).toByteArray();

	        // 6. The first 32 bytes of H are called key_e and the last 32 bytes are called key_m.
	        byte[] keyE = Arrays.copyOfRange(h, 0, 32);
	        byte[] keyM = Arrays.copyOfRange(h, 32, 64);

	        // 7. Pad the input text to a multiple of 16 bytes, in accordance to PKCS7.
	        // 8. Encrypt the data with AES-256-CBC, using IV as initialization vector, key_e as encryption key and the padded input text as payload. Call the output cipher text.
	        byte[] encrypted = ECKeyUtils.crypt(true, iv, data, keyE);

	        // 9. Calculate a 32 byte MAC with HMACSHA256, using key_m as salt and IV + ephemeral.pub + cipher text as data. Call the output MAC.
	        byte[] mac = ECKeyUtils.calculateMAC(keyM, iv, ephemeral.getPublicKey(), encrypted);

	        // 10. Write out the encryption result IV + ephemeral.pub + encrypted + MAC
	        ByteArrayOutputStream baos = new ByteArrayOutputStream();
	     	DataOutputStream outputStream = new DataOutputStream(baos);
	     	outputStream.write(iv);
	     	outputStream.writeByte(ephemeral.getPublicKey().length());
	     	outputStream.write(ephemeral.getPublicKey().getBytes());
	     	outputStream.writeInt(encrypted.length);
	     	outputStream.write(encrypted);
	     	outputStream.write(mac);

	     	return baos.toByteArray();
		} 
		catch (Exception ex) 
		{
			throw new CryptoException("Failed to encrypt", ex);
		}
	}

	@Override
	public int hashCode() 
	{
		if (this.hashCode == Long.MAX_VALUE)
			this.hashCode = Arrays.hashCode(this.publicKey);
		
		return (int) this.hashCode;
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == this)
			return true;

		if (object instanceof ECPublicKey) 
		{
			ECPublicKey other = (ECPublicKey) object;
			return Arrays.equals(other.publicKey, this.publicKey);
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return Bytes.toBase64String(this.publicKey);
	}

	@Override
	public int compareTo(ECPublicKey other)
	{
		return SignedBytes.lexicographicalComparator().compare(this.publicKey, other.publicKey);
	}
}
