package org.fuserleer.crypto;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("crypto.certificate")
public final class Certificate extends BasicObject implements Primitive
{
	@JsonProperty("object")
	@DsonOutput(Output.ALL)
	private Hash object;
	
	@JsonProperty("signatures")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=LinkedHashMap.class)
	private ECSignatureBag	signatures;

	private Certificate()
	{
		// For serializer
	}
	
	public Certificate(final Hash object, final Map<ECPublicKey, ECSignature> signatures)
	{
		Objects.requireNonNull(object, "Object is null");
		if (object.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Object is ZERO");
		
		Objects.requireNonNull(signatures, "Signatures is null");
		if (signatures.isEmpty() == true)
			throw new IllegalArgumentException("Signatures is empty");

		this.object = object;
		this.signatures = new ECSignatureBag(signatures);
	}

	public Hash getObject()
	{
		return object;
	}

	public ECSignatureBag getSignatures()
	{
		return signatures;
	}
	
	public boolean verify(ECPublicKey signer)
	{
		return this.signatures.verify(this.object, signer);
	}
}
