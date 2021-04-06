package org.fuserleer.crypto;

import java.util.concurrent.atomic.AtomicLong;

public final class CryptoUtils
{
	// ECC Key profiling
	public final static AtomicLong ECCSigned = new AtomicLong(0);
	public final static AtomicLong ECCVerified = new AtomicLong(0);

	// BLS Key profiling
	public final static AtomicLong BLSSigned = new AtomicLong(0);
	public final static AtomicLong BLSVerified = new AtomicLong(0);

	// Hash profiling
	public final static AtomicLong hashed = new AtomicLong(0);
	public final static AtomicLong singles = new AtomicLong(0);
	public final static AtomicLong doubles = new AtomicLong(0);

}
