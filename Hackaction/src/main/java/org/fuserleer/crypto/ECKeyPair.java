package org.fuserleer.crypto;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Set;

import org.bouncycastle.crypto.AsymmetricCipherKeyPair;
import org.bouncycastle.crypto.generators.ECKeyPairGenerator;
import org.bouncycastle.crypto.params.ECKeyGenerationParameters;
import org.bouncycastle.crypto.params.ECPrivateKeyParameters;
import org.bouncycastle.crypto.params.ECPublicKeyParameters;
import org.bouncycastle.math.ec.ECPoint;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.utils.Bytes;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;

/**
 * Asymmetric EC key pair provider fixed to curve 'secp256k1'.
 */
public final class ECKeyPair 
{
	public static final int	BYTES = 32;

	/**
	 * Load a private key from file, and compute the public key.
	 *
	 * @param file  The file to load the private key from.
	 * @param create Set to {@code true} if the file should be created if it doesn't exist.
	 * @return An {@link ECKeyPair}
	 * @throws IOException If reading or writing the file fails
	 * @throws CryptoException If the key read from the file is invalid
	 */
	public static final ECKeyPair fromFile(File file, boolean create) throws IOException, CryptoException 
	{
		if (file.exists() == false) 
		{
			if (create == false)
				throw new FileNotFoundException("Keyfile " + file.toString() + " not found");

			File dir = file.getParentFile();
			if (dir != null && dir.exists() == false && dir.mkdirs() == false)
				throw new FileNotFoundException("Failed to create directory: " + dir.toString());

			try (FileOutputStream io = new FileOutputStream(file)) 
			{
				try 
				{
					Set<PosixFilePermission> perms = ImmutableSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
					Files.setPosixFilePermissions(file.toPath(), perms);
				} 
				catch (UnsupportedOperationException ignoredException) 
				{
					// probably windows
				}

				ECKeyPair key = new ECKeyPair();
				io.write(key.getPrivateKey());
				return key;
			}
		} 
		else 
		{
			try (FileInputStream io = new FileInputStream(file)) 
			{
				byte[] deploymentPriv = new byte[BYTES];
				ByteStreams.readFully(io, deploymentPriv);
				return new ECKeyPair(deploymentPriv);
			}
		}
	}

	/**
	 * Write a private key to a file.
	 *
	 * @param file  The file to store the private key to.
	 * @param key   The key to store.
	 * 
	 * @throws IOException If writing the file fails
	 */
	public static final void toFile(File file, ECKeyPair key) throws IOException 
	{
		File dir = file.getParentFile();
		if (dir != null && dir.exists() == false && dir.mkdirs() == false)
			throw new FileNotFoundException("Failed to create directory: " + dir.toString());

		try (FileOutputStream io = new FileOutputStream(file)) 
		{
			try 
			{
				Set<PosixFilePermission> perms = ImmutableSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
				Files.setPosixFilePermissions(file.toPath(), perms);
			} 
			catch (UnsupportedOperationException ignoredException) 
			{
				// probably windows
			}

			io.write(key.getPrivateKey());
		}
	}

	private final byte[] privateKey;
	private final ECPublicKey publicKey;

	public ECKeyPair() throws CryptoException 
	{
		this(ECKeyUtils.secureRandom);
	}

	public ECKeyPair(SecureRandom random) throws CryptoException 
	{
		try 
		{
			ECKeyPairGenerator generator = new ECKeyPairGenerator();
	        ECKeyGenerationParameters keygenParams = new ECKeyGenerationParameters(ECKeyUtils.domain, random);
	        generator.init(keygenParams);
	        AsymmetricCipherKeyPair keypair = generator.generateKeyPair();
	        ECPrivateKeyParameters privParams = (ECPrivateKeyParameters) keypair.getPrivate();
	        ECPublicKeyParameters pubParams = (ECPublicKeyParameters) keypair.getPublic();

	        byte[] privateKeyBytes = privParams.getD().toByteArray();
			validatePrivate(privateKeyBytes);

	        this.privateKey = trimPrivateKey(privateKeyBytes);
	        this.publicKey = ECPublicKey.from(pubParams.getQ().getEncoded(true));
		} 
		catch (Exception ex) 
		{
			throw new CryptoException(ex);
		}
	}
	
	public ECKeyPair(byte[] key) throws CryptoException 
	{
		try 
		{
			validatePrivate(key);
			this.privateKey = Arrays.copyOf(key, key.length);
			this.publicKey = ECPublicKey.from(ECKeyUtils.keyHandler.computePublicKey(key));
		} 
		catch (Exception ex) 
		{
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

	public byte[] getPrivateKey() 
	{
		return this.privateKey;
	}

	public ECPublicKey getPublicKey() 
	{
		return this.publicKey;
	}

	public ECSignature sign(Hash hash) throws CryptoException
	{
		return sign(hash.toByteArray());
	}

	public ECSignature sign(byte[] hash) throws CryptoException 
	{
		return ECKeyUtils.keyHandler.sign(hash, this.privateKey);
	}

	public byte[] decrypt(byte[] data) throws CryptoException 
	{
		try 
		{
			DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(data));
			
			// 1. Read the `IV` (as in `initialization vector`)
			byte[] iv = new byte[16];
			inputStream.readFully(iv);
						
			// 2. Read the ephemeral public key
			int publicKeySize = inputStream.readUnsignedByte();
			byte[] publicKeyRaw = new byte[publicKeySize];
			inputStream.readFully(publicKeyRaw);
			ECPublicKey ephemeral = ECPublicKey.from(publicKeyRaw);

			// 3. Do an EC point multiply with this.getPrivateKey() and ephemeral public key. This gives you a point M.
			ECPoint m = ephemeral.getPublicPoint().multiply(new BigInteger(1, getPrivateKey())).normalize();

			// 4. Use the X component of point M and calculate the SHA512 hash H.
			byte[] h = new Hash(m.getXCoord().getEncoded(), Mode.STANDARD).toByteArray();

			// 5. The first 32 bytes of H are called key_e and the last 32 bytes are called key_m.
			byte[] keyE = Arrays.copyOfRange(h, 0, 32);
			byte[] keyM = Arrays.copyOfRange(h, 32, 64);

			// 6. Read encrypted data
			byte[] encrypted = new byte[inputStream.readInt()];
			inputStream.readFully(encrypted);
						
			// 7. Read MAC
			byte[] mac = new byte[32];
			inputStream.readFully(mac);
			
			// 8. Compare MAC with MAC'. If not equal, decryption will fail.
			if (Arrays.equals(mac, ECKeyUtils.calculateMAC(keyM, iv, ephemeral, encrypted)) == false)
				throw new CryptoException("MAC mismatch when decrypting");

			// 9. Decrypt the cipher text with AES-256-CBC, using IV as initialization vector, key_e as decryption key
			// and the cipher text as payload. The output is the padded input text.
			return ECKeyUtils.crypt(false, iv, encrypted, keyE);
		} 
		catch (CryptoException e) 
		{
			throw e;
		} 
		catch (Exception e) 
		{
			throw new CryptoException("Failed to decrypt", e);
		}
	}

	@Override
	public boolean equals(Object object) 
	{
		if (this == object)
			return true;

		if (object instanceof ECKeyPair) 
		{
			ECKeyPair other = (ECKeyPair) object;
			// Comparing private keys should be sufficient
			return Arrays.equals(other.getPrivateKey(), this.getPrivateKey());
		}
		
		return false;
	}

	@Override
	public int hashCode() 
	{
		return Arrays.hashCode(this.getPrivateKey());
	}

	@Override
	public String toString() 
	{
		// Not going to print the private key here DUH
		return String.format("%s[%s]", getClass().getSimpleName(), Bytes.toBase64String(getPublicKey().getBytes()));
	}
}
