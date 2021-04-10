package org.fuserleer.crypto;

import java.util.Objects;

import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
@SerializerId2("crypto.bls_sigpub")
public final class BLSSignatureAndPublicKey 
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("version")
	@DsonOutput(Output.ALL)
	private short version = 100;

	@JsonProperty("signature")
	@DsonOutput(Output.ALL)
	private BLSSignature signature;
	
	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private BLSPublicKey publicKey;

	@SuppressWarnings("unused")
	private BLSSignatureAndPublicKey()
	{
		// FOR SERIALIZER
	}

	public BLSSignatureAndPublicKey(final BLSSignature signature, final BLSPublicKey pubKey) 
	{
		Objects.requireNonNull(pubKey, "Public key is null");
		Objects.requireNonNull(signature, "Signature is null");
		
		this.signature = signature;
		this.publicKey = pubKey;
	}

	public BLSPublicKey getPublicKey() 
	{
		return this.publicKey;
	}

	public BLSSignature getSignature() 
	{
		return this.signature;
	}

	public BLSSignatureAndPublicKey combine(final BLSSignatureAndPublicKey sigAndPubKey) 
	{
		Objects.requireNonNull(sigAndPubKey, "SignatureAndPublicKey to combine is null");
		
		BLSSignature newSignature = this.signature.combine(sigAndPubKey.signature);
		BLSPublicKey newPubKey = this.publicKey.combine(sigAndPubKey.publicKey);
		return new BLSSignatureAndPublicKey(newSignature, newPubKey);
	}
	
  	@Override
	public int hashCode() 
  	{
  		return Objects.hash(this.signature, this.publicKey);
	}
	
	@Override
	public boolean equals(Object obj) 
	{
	    if (this == obj)
	    	return true;
	    if (obj == null)
	    	return false;
	    if (getClass() != obj.getClass())
	    	return false;
	    
	    BLSSignatureAndPublicKey other = (BLSSignatureAndPublicKey) obj;
	    if (this.signature == null) 
	    {
	    	if (other.signature != null)
	    		return false;
	    } else if (this.signature.equals(other.signature) == false)
	    	return false;
	    
	    if (this.publicKey == null) 
	    {
	    	if (other.publicKey != null)
	    		return false;
	    } else if (this.publicKey.equals(other.publicKey) == false)
	    	return false;

	    return true;
	}
}
