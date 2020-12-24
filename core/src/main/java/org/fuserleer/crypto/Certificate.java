package org.fuserleer.crypto;

import java.util.Map;
import java.util.Objects;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.database.IndexablePrimitive;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("crypto.certificate")
public abstract class Certificate extends BasicObject implements IndexablePrimitive
{
	@JsonProperty("decision")
	@DsonOutput(Output.ALL)
	private boolean decision;

	@JsonProperty("signatures")
	@DsonOutput(Output.ALL)
	private ECSignatureBag	signatures;

	protected Certificate()
	{
		// For serializer
	}
	
	protected Certificate(final boolean decision)
	{
		this.signatures = new ECSignatureBag();
		this.decision = decision;
	}

	protected Certificate(final boolean decision, final Map<ECPublicKey, ECSignature> signatures)
	{
		Objects.requireNonNull(signatures, "Signatures is null");
		if (signatures.isEmpty() == true)
			throw new IllegalArgumentException("Signatures is empty");

		this.signatures = new ECSignatureBag(signatures);
		this.decision = decision;
	}
	
	protected final boolean add(final ECPublicKey identity, final ECSignature signature) throws CryptoException
	{
		if (verify(identity, signature) == false)
			throw new CryptoException("Signature from "+identity+" does not sign certificate");
		
		return this.signatures.add(identity, signature);
	}

	public final boolean getDecision()
	{
		return this.decision;
	}

	public final ECSignatureBag getSignatures()
	{
		return this.signatures;
	}
	
	public abstract <T> T getObject();
	
	public abstract boolean verify(final ECPublicKey identity, final ECSignature signature);

	final boolean verify(Hash hash, ECPublicKey identity)
	{
		return this.signatures.verify(hash, identity);
	}
}
