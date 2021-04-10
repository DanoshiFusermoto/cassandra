package org.fuserleer.crypto;

import java.util.Objects;

import org.apache.milagro.amcl.BLS381.BIG;
import org.fuserleer.crypto.bls.group.G1Point;
import org.fuserleer.crypto.bls.group.Scalar;
import org.fuserleer.utils.Numbers;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
public final class BLSPrivateKey extends PrivateKey
{
	public final static BLSPrivateKey from(final byte[] bytes)
	{
		Objects.requireNonNull(bytes, "Bytes is null");
		Numbers.isZero(bytes.length, "Bytes length is zero");
		return new BLSPrivateKey(new Scalar(BIG.fromBytes(bytes)));
	}

	private final Scalar scalarValue;

	BLSPrivateKey(final Scalar value) 
	{
		Objects.requireNonNull(value, "PrivateKey was not properly initialized");
		this.scalarValue = value;
	}

	protected G1Point sign(final G1Point message) 
	{
		Objects.requireNonNull(message, "Message to sign is null");
		return message.mul(this.scalarValue);
	}
	
	public byte[] toByteArray()
	{
		return this.scalarValue.toByteArray();
	}
}
