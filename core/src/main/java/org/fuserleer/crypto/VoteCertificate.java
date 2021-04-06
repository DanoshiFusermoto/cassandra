package org.fuserleer.crypto;

import java.util.List;
import java.util.Objects;

import org.fuserleer.collections.Bloom;
import org.fuserleer.ledger.StateDecision;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class VoteCertificate extends Certificate
{
	@JsonProperty("signers")
	@DsonOutput(Output.ALL)
	private Bloom signers;

	@JsonProperty("signature")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private BLSSignature signature;

	@SuppressWarnings("unused")
	protected VoteCertificate()
	{
		super();
	}

	protected VoteCertificate(final StateDecision decision, final Bloom signers, final BLSSignature signature) throws CryptoException
	{
		super(decision);
		
		Objects.requireNonNull(signature, "Signature is null");
		Objects.requireNonNull(signers, "Identities is null");
		Numbers.isZero(signers.count(), "Signers is empty");
		
		this.signers = signers;
		this.signature = signature;
	}

	public final Bloom getSigners()
	{
		return this.signers;
	}

	public final BLSSignature getSignature()
	{
		return this.signature;
	}
	
	final boolean verify(final Hash hash, final List<BLSPublicKey> identities)
	{
		Objects.requireNonNull(identities, "Identity is null");
		Numbers.isZero(identities.size(), "Identities is empty");
		Objects.requireNonNull(hash, "Hash is null");
		Hash.notZero(hash, "Hash is ZERO");

		BLSPublicKey aggregated = BLS12381.aggregatePublicKey(identities);
		return BLS12381.verify(aggregated, this.signature, hash.toByteArray());
	}
}
