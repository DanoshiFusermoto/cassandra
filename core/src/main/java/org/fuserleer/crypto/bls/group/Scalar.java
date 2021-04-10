package org.fuserleer.crypto.bls.group;

import java.util.Objects;

import org.apache.milagro.amcl.BLS381.BIG;
import org.fuserleer.utils.Numbers;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
public class Scalar 
{
	public final static Scalar from(final byte[] bytes)
	{
		Objects.requireNonNull(bytes, "Bytes is null");
		Numbers.isZero(bytes.length, "Bytes length is zero");
		return new Scalar(BIG.fromBytes(bytes));
	}
	
	private final BIG value;

	public Scalar(final BIG value) 
	{
		Objects.requireNonNull(value, "Value is null");
		this.value = value;
	}

	BIG value() 
	{
		return new BIG(value);
	}
	
	public byte[] toByteArray()
	{
		byte[] bytes = new byte[BIG.MODBYTES];
		this.value.toBytes(bytes);
		return bytes;
	}
}
