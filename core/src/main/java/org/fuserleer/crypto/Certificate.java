package org.fuserleer.crypto;

import java.util.Map;
import java.util.Objects;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.ledger.StateDecision;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("crypto.certificate")
public abstract class Certificate extends BasicObject implements Primitive
{
	@JsonProperty("decision")
	@DsonOutput(Output.ALL)
	private StateDecision decision;

	@JsonProperty("signatures")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private ECSignatureBag	signatures;

	protected Certificate()
	{
		// For serializer
	}
	
	protected Certificate(final StateDecision decision)
	{
		this.signatures = new ECSignatureBag();
		this.decision = decision;
	}

	protected Certificate(final StateDecision decision, final Map<ECPublicKey, ECSignature> signatures)
	{
		this(decision, new ECSignatureBag(signatures));
	}

	protected Certificate(final StateDecision decision, final ECSignatureBag signatures)
	{
		Objects.requireNonNull(signatures, "Signatures is null");
		if (signatures.isEmpty() == true)
			throw new IllegalArgumentException("Signatures is empty");

		this.decision = Objects.requireNonNull(decision, "Decision is null");
		if (decision.equals(StateDecision.ERROR) == true || decision.equals(StateDecision.UNKNOWN) == true)
			throw new IllegalArgumentException("Decision "+decision+" is unsupported at present");

		this.signatures = signatures;
	}
	
	protected final boolean add(final ECPublicKey identity, final ECSignature signature) throws CryptoException
	{
		Objects.requireNonNull(identity, "Identity is null");
		Objects.requireNonNull(signature, "Signature is null");
		
		if (verify(identity, signature) == false)
			throw new CryptoException("Signature from "+identity+" does not sign certificate");
		
		return this.signatures.add(identity, signature);
	}

	public final StateDecision getDecision()
	{
		return this.decision;
	}

	public final ECSignatureBag getSignatures()
	{
		return this.signatures;
	}
	
	public abstract <T> T getObject();
	
	public abstract boolean verify(final ECPublicKey identity, final ECSignature signature);

	final boolean verify(final Hash hash, final ECPublicKey identity)
	{
		Objects.requireNonNull(identity, "Identity is null");
		Objects.requireNonNull(hash, "Hash is null");
		Hash.notZero(hash, "Hash is ZERO");

		return this.signatures.verify(hash, identity);
	}
}
