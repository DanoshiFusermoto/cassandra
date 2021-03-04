package org.fuserleer.crypto;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("crypto.ecsignature_bag")
public final class ECSignatureBag
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("signatures")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	@JsonDeserialize(as=LinkedHashMap.class)
	private Map<ECPublicKey, ECSignature> signatures;
	
	public ECSignatureBag()
	{
		this.signatures = new LinkedHashMap<ECPublicKey, ECSignature>();
	}
	
	public ECSignatureBag(final Map<ECPublicKey, ECSignature> signatures)
	{
		this();
		
		Objects.requireNonNull(signatures, "Signatures is null");
		if (signatures.isEmpty() == true)
			throw new IllegalArgumentException("Signatures is empty");

		this.signatures.putAll(signatures);
	}
	
	public final Set<ECPublicKey> getSigners()
	{
		synchronized(this.signatures)
		{
			return Collections.unmodifiableSet(this.signatures.keySet());
		}
	}
	
	public final boolean verify(final Hash hash, final ECPublicKey identity)
	{
		Objects.requireNonNull(hash, "Hash to verify is null");
		Hash.notZero(hash, "Hash to veryify is ZERO");
		Objects.requireNonNull(identity, "Verification identity is null");
		
		synchronized(this.signatures)
		{
			if (this.signatures.containsKey(identity) == false)
				return false;
			
			return identity.verify(hash, this.signatures.get(identity));
		}
	}

	public final boolean add(final ECPublicKey identity, final ECSignature signature)
	{
		Objects.requireNonNull(identity, "Identity to add is null");
		Objects.requireNonNull(signature, "Signature to add is null");

		synchronized(this.signatures)
		{
			if (this.signatures.containsKey(identity) == true)
				return false;
			
			this.signatures.put(identity, signature);
			return true;
		}
	}

	public boolean isEmpty() 
	{
		synchronized(this.signatures)
		{
			return this.signatures.isEmpty();
		}
	}

	public int size() 
	{
		synchronized(this.signatures)
		{
			return this.signatures.size();
		}
	}
}
