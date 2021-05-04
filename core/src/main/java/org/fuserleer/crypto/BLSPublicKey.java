package org.fuserleer.crypto;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bouncycastle.util.Arrays;
import org.fuserleer.crypto.bls.group.G2Point;
import org.fuserleer.utils.Bytes;
import org.fuserleer.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
public final class BLSPublicKey extends PublicKey
{
	@JsonCreator
	public static BLSPublicKey from(final byte[] bytes) 
	{
	    return new BLSPublicKey(bytes);
	}
	
	public static BLSPublicKey from(final String key) throws CryptoException 
	{
		Objects.requireNonNull(key, "Key string is null");
		Numbers.isZero(key.length(), "Key string is empty");
		return BLSPublicKey.from(Bytes.fromBase64String(Objects.requireNonNull(key, "Key string is null")));
	}
	
	// Placeholder for the serializer ID
/*	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("version")
	@DsonOutput(Output.ALL)
	private short version = 100;*/

	private byte[] bytes;

	private transient G2Point point;

	@SuppressWarnings("unused")
	private BLSPublicKey()
	{
		// FOR SERIALIZER
	}

	BLSPublicKey(final byte[] bytes) 
	{
		Objects.requireNonNull(bytes, "Bytes for public key is null");
		Numbers.isZero(bytes.length, "Bytes length is zero");
		
		this.point = G2Point.fromBytes(bytes);
		this.bytes = Arrays.clone(bytes);
	}

	BLSPublicKey(final G2Point point) 
	{
		Objects.requireNonNull(point, "Public key point is null");
		this.point = point;
		this.bytes = this.point.toBytes();
	}

	public BLSPublicKey combine(final Collection<BLSPublicKey> publicKeys) 
	{
		Objects.requireNonNull(publicKeys, "Public keys to combine is null");
		return new BLSPublicKey(g2Point().add(publicKeys.stream().map(pk -> pk.point).collect(Collectors.toList())));
	}

	public BLSPublicKey combine(final BLSPublicKey publicKey) 
	{
		Objects.requireNonNull(publicKey, "Public key for combine is null");
		return new BLSPublicKey(g2Point().add(publicKey.point));
	}

  	public synchronized G2Point g2Point() 
  	{
  		if (this.point == null)
  			this.point = G2Point.fromBytes(this.bytes);
  		
  		return new G2Point(this.point);
  	}
  	
	@JsonValue
	@Override
	public byte[] toByteArray()
	{
		return Arrays.clone(this.bytes);
	}

	@Override
	public boolean verify(final byte[] hash, final Signature signature)
	{
		Objects.requireNonNull(hash, "Hash to verify is null");
		Numbers.isZero(hash.length, "Hash length is zero");
		Objects.requireNonNull(signature, "Signature to verify is null");
		return BLS12381.verify(this, (BLSSignature) signature, hash);
	}
}
