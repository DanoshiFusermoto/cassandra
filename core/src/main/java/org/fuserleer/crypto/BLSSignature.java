package org.fuserleer.crypto;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import org.bouncycastle.util.Arrays;
import org.fuserleer.crypto.bls.group.G1Point;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
@SerializerId2("crypto.bls_signature")
public final class BLSSignature extends Signature
{
	public static BLSSignature from(final byte[] bytes) 
	{
		Objects.requireNonNull(bytes, "Bytes is null for BLS point");
		Numbers.isZero(bytes.length, "Bytes length is zero");
		return new BLSSignature(bytes);
	}

	  // Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("version")
	@DsonOutput(Output.ALL)
	private short version = 100;

	private byte[] bytes;

	private transient G1Point point;

	@SuppressWarnings("unused")
	private BLSSignature()
	{
		// FOR SERIALIZER
	}
	
	BLSSignature(final byte[] bytes) 
	{
		Objects.requireNonNull(bytes, "Bytes for signature is null");
		Numbers.isZero(bytes.length, "Bytes length is zero");
		
		this.point = G1Point.fromBytes(bytes);
		this.bytes = Arrays.clone(bytes);
	}

	BLSSignature(final G1Point point) 
	{
		Objects.requireNonNull(point, "Point for signature is null");
		this.point = point;
		this.bytes = this.point.toBytes();
	}

	@Override
	public String toString() 
	{
		return "Signature [ecpPoint="+this.point+"]";
	}

	public BLSSignature combine(final Collection<BLSSignature> signatures) 
	{
		Objects.requireNonNull(signatures, "Signature to combine is null");
		return new BLSSignature(g1Point().add(signatures.stream().map(s -> s.point).collect(Collectors.toList())));
	}

	public BLSSignature combine(final BLSSignature signature) 
	{
		Objects.requireNonNull(signature, "Signature to combine is null");
		return new BLSSignature(g1Point().add(signature.g1Point()));
	}

	public BLSSignature reduce(final BLSSignature signature) 
	{
		Objects.requireNonNull(signature, "Signature to reduce is null");
		return new BLSSignature(g1Point().sub(signature.g1Point()));
	}

  	synchronized G1Point g1Point() 
  	{
  		if (this.point == null)
  			this.point = G1Point.fromBytes(this.bytes);
  		
  		return new G1Point(this.point);
  	}
  
	@JsonProperty("point")
	@DsonOutput(Output.ALL)
	private byte[] getJsonPoint() 
	{
		return this.bytes;
	}

	@JsonProperty("point")
	private void setJsonPoint(byte[] point) 
	{
		this.bytes = Arrays.clone(point);
	}
	
	public byte[] toByteArray() 
	{
		return Arrays.clone(this.bytes);
	}
}
