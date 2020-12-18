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
	
	private ECSignatureBag()
	{
		// For serializer
	}
	
	public ECSignatureBag(Map<ECPublicKey, ECSignature> signatures)
	{
		this.signatures = new LinkedHashMap<ECPublicKey, ECSignature>(signatures);
	}
	
	public Set<ECPublicKey> getSigners()
	{
		return Collections.unmodifiableSet(this.signatures.keySet());
	}
	
	public boolean verify(Hash hash, ECPublicKey signer)
	{
		Objects.requireNonNull(hash, "Hash to verify is null");
		Objects.requireNonNull(signer, "Signer is null");
		
		if (this.signatures.containsKey(signer) == false)
			return false;
		
		return signer.verify(hash, this.signatures.get(signer));
	}
}
