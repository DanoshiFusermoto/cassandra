package org.fuserleer.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

class SHAHashHandler implements HashHandler {
	// Note that default provide around 20-25% faster than Bouncy Castle.
	// See jmh/org.radix.benchmark.HashBenchmark
	private final ThreadLocal<MessageDigest> hash256DigesterInner = ThreadLocal.withInitial(() -> getDigester("SHA-256", null));
	private final ThreadLocal<MessageDigest> hash256DigesterOuter = ThreadLocal.withInitial(() -> getDigester("SHA-256", null));
	private final ThreadLocal<MessageDigest> hash512DigesterInner = ThreadLocal.withInitial(() -> getDigester("SHA-512", null));
	private final ThreadLocal<MessageDigest> hash512DigesterOuter = ThreadLocal.withInitial(() -> getDigester("SHA-512", null));

	SHAHashHandler() {
	}

	@Override
	public byte[] hash256(byte[] data) 
	{
		return hash256(data, 0, data.length);
	}

	@Override
	public byte[] hash256(byte[] data, int offset, int length) 
	{
		final MessageDigest hash256DigesterInnerLocal = hash256DigesterInner.get();
		hash256DigesterInnerLocal.reset();
		hash256DigesterInnerLocal.update(data, offset, length);
		
		byte[] digest = hash256DigesterInnerLocal.digest();
		byte[] truncated = new byte[32];
		System.arraycopy(digest, 0, truncated, 0, truncated.length);
		return truncated;
	}

	@Override
	public byte[] hash256(byte[] data0, byte[] data1) 
	{
		final MessageDigest hash256DigesterInnerLocal = hash256DigesterInner.get();
		hash256DigesterInnerLocal.reset();
		hash256DigesterInnerLocal.update(data0);
		
		byte[] digest = hash256DigesterInnerLocal.digest(data1);
		byte[] truncated = new byte[32];
		System.arraycopy(digest, 0, truncated, 0, truncated.length);
		return truncated;
	}

	@Override
	public byte[] hash512(byte[] data, int offset, int length) 
	{
		final MessageDigest hash512DigesterInnerLocal = hash512DigesterInner.get();
		hash512DigesterInnerLocal.reset();
		hash512DigesterInnerLocal.update(data, offset, length);
		return hash512DigesterInnerLocal.digest();
	}

	@Override
	public byte[] doubleHash256(byte[] data) 
	{
		return doubleHash256(data, 0, data.length);
	}

	@Override
	public byte[] doubleHash256(byte[] data, int offset, int length) 
	{
		// Here we use SHA-256(SHA-512(data)) to avoid length-extension attack
		final MessageDigest hash256DigesterOuterLocal = hash256DigesterOuter.get();
		final MessageDigest hash256DigesterInnerLocal = hash256DigesterInner.get();
		hash256DigesterOuterLocal.reset();
		hash256DigesterInnerLocal.reset();
		hash256DigesterInnerLocal.update(data, offset, length);
		
		byte[] digest = hash256DigesterOuterLocal.digest(hash256DigesterInnerLocal.digest());
		byte[] truncated = new byte[32];
		System.arraycopy(digest, 0, truncated, 0, truncated.length);
		return truncated;
	}

	@Override
	public byte[] doubleHash256(byte[] data0, byte[] data1) {
		// Here we use SHA-256(SHA-512(data0 || data1)) to avoid length-extension attack
		final MessageDigest hash256DigesterOuterLocal = hash256DigesterOuter.get();
		final MessageDigest hash256DigesterInnerLocal = hash256DigesterInner.get();
		hash256DigesterInnerLocal.reset();
		hash256DigesterOuterLocal.reset();
		hash256DigesterInnerLocal.update(data0);
		byte[] digest = hash256DigesterOuterLocal.digest(hash256DigesterInnerLocal.digest(data1));
		byte[] truncated = new byte[32];
		System.arraycopy(digest, 0, truncated, 0, truncated.length);
		return truncated;
	}

	@Override
	public byte[] doubleHash512(byte[] data, int offset, int length) 
	{
		// Here we use SHA-512(SHA-512(data0 || data1)) to avoid length-extension attack
		final MessageDigest hash512DigesterInnerLocal = hash512DigesterInner.get();
		final MessageDigest hash512DigesterOuterLocal = hash512DigesterOuter.get();
		hash512DigesterInnerLocal.reset();
		hash512DigesterOuterLocal.reset();
		hash512DigesterInnerLocal.update(data, offset, length);
		return hash512DigesterOuterLocal.digest(hash512DigesterInnerLocal.digest());
	}

	private static MessageDigest getDigester(String algorithm, String provider) 
	{
		try 
		{
			return (provider == null) ? MessageDigest.getInstance(algorithm) : MessageDigest.getInstance(algorithm, provider);
		} catch (NoSuchProviderException e) {
			throw new IllegalArgumentException("No such provider for: " + algorithm, e);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("No such algorithm: " + algorithm, e);
		}
	}
}
