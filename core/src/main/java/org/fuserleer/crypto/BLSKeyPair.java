package org.fuserleer.crypto;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Objects;
import java.util.Set;

import org.apache.milagro.amcl.RAND;
import org.apache.milagro.amcl.BLS381.BIG;
import org.fuserleer.crypto.bls.group.G2Point;
import org.fuserleer.crypto.bls.group.Scalar;
import org.fuserleer.utils.Numbers;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
public final class BLSKeyPair extends KeyPair<BLSPrivateKey, BLSPublicKey, BLSSignature>
{
	/**
	 * Load a private key from file, and compute the public key.
	 *
	 * @param file  The file to load the private key from.
	 * @param create Set to {@code true} if the file should be created if it doesn't exist.
	 * @return An {@link BLSKeyPair}
	 * @throws IOException If reading or writing the file fails
	 * @throws CryptoException If the key read from the file is invalid
	 */
	public static final BLSKeyPair fromFile(final File file, final boolean create) throws IOException, CryptoException 
	{
		Objects.requireNonNull(file, "Key file is null");
		
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

				BLSKeyPair key = new BLSKeyPair();
				DataOutputStream dos = new DataOutputStream(io);
				dos.writeInt(key.getPrivateKey().toByteArray().length);
				dos.write(key.getPrivateKey().toByteArray());
				dos.flush();
				return key;
			}
		} 
		else 
		{
			try (FileInputStream io = new FileInputStream(file)) 
			{
				DataInputStream dis = new DataInputStream(io);
				byte[] privBytes = new byte[dis.readInt()];
				dis.readFully(privBytes);
				return new BLSKeyPair(privBytes);
			}
		}
	}

	private final BLSPrivateKey privateKey;
	private final BLSPublicKey publicKey;
	
	private final static RAND random;
	
	static
	{
		random = new RAND();
		
		// FIXME crappy seed
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write(Longs.toByteArray(System.currentTimeMillis()));
			baos.write(Longs.toByteArray(System.nanoTime()));
			baos.write(Longs.toByteArray(Runtime.getRuntime().freeMemory()));
			baos.write(Longs.toByteArray(Runtime.getRuntime().maxMemory()));
			baos.write(Longs.toByteArray(Runtime.getRuntime().totalMemory()));
			random.seed(baos.size(), baos.toByteArray());
		}
		catch (Throwable t)
		{
			throw new RuntimeException(t);
		}
	}

	public BLSKeyPair() 
	{
	    Scalar secret = new Scalar(BIG.randomnum(BLSCurveParameters.curveOrder(), random));
	
	    G2Point g2Generator = BLSCurveParameters.g2Generator();
	    BLSPrivateKey privateKey = new BLSPrivateKey(secret);
	    G2Point g2Point = g2Generator.mul(secret);
	    BLSPublicKey publicKey = new BLSPublicKey(g2Point);
	
	    this.privateKey = privateKey;
	    this.publicKey = publicKey;
  	}

	public BLSKeyPair(final byte[] bytes) 
	{
		Objects.requireNonNull(bytes, "BLSKeyPair bytes is null");
		Numbers.isZero(bytes.length, "BLSKeyPair bytes length is zero");
		
	    G2Point g2Generator = BLSCurveParameters.g2Generator();
	    Scalar secret = Scalar.from(bytes);
	
	    BLSPrivateKey privateKey = new BLSPrivateKey(secret);
	    G2Point g2Point = g2Generator.mul(secret);
	    BLSPublicKey publicKey = new BLSPublicKey(g2Point);

	    this.privateKey = privateKey;
	    this.publicKey = publicKey;
  	}

	@Override
	public BLSPrivateKey getPrivateKey()
	{
		return this.privateKey;
	}

	@Override
	public BLSPublicKey getPublicKey()
	{
		return this.publicKey;
	}

	@Override
	public BLSSignature sign(final byte[] hash) throws CryptoException
	{
		return BLS12381.sign(this, hash).getSignature();
	}
}
