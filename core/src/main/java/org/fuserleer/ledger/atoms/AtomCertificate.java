package org.fuserleer.ledger.atoms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.fuserleer.crypto.Certificate;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignature;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateCertificate;
import org.fuserleer.ledger.StateDecision;
import org.fuserleer.ledger.StateKey;
import org.fuserleer.ledger.VotePowerBloom;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.atom.certificate")
public final class AtomCertificate extends Certificate
{
	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;
	
	@JsonProperty("certificates")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	@JsonDeserialize(as=LinkedHashMap.class)
	private Map<Hash, StateCertificate> certificates;

	// FIXME need to implement some way to have agreement on powers as weakly-subjective and may cause differing certificate hashes between validators
	@JsonProperty("powers")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
//	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=LinkedHashMap.class)
	private Map<Long, VotePowerBloom> powers;

	@SuppressWarnings("unused")
	private AtomCertificate()
	{
		super();
		
		// FOR SERIALIZER
	}
	
	public AtomCertificate(final Hash atom, final Collection<StateCertificate> certificates, final Collection<VotePowerBloom> powers) throws CryptoException, ValidationException
	{
		super(Objects.requireNonNull(certificates, "Certificates is null").stream().anyMatch(c -> c.getDecision().equals(StateDecision.NEGATIVE)) ? StateDecision.NEGATIVE : StateDecision.POSITIVE);
		
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		this.atom = atom;
		
		if (certificates.isEmpty() == true)
			throw new IllegalArgumentException("Certificates is empty");
		
		List<StateCertificate> sortedCertificates = new ArrayList<StateCertificate>(certificates);
		sortedCertificates.sort(new Comparator<StateCertificate>() 
		{
			@Override
			public int compare(StateCertificate o1, StateCertificate o2)
			{
				return o1.getState().get().compareTo(o2.getState().get());
			}
		});
		
		this.certificates = new LinkedHashMap<Hash, StateCertificate>();
		for (StateCertificate certificate : sortedCertificates)
		{
			if (certificate.getAtom().equals(this.atom) == false)
				throw new ValidationException("State certificate for "+certificate.getState()+" does not reference atom "+this.atom);
			
			// Create a "copy" instance of the StateCertificate dropping the bulky bloom which will 
			// have been duplicated if there are multiple states within the same shard group
			this.certificates.put(certificate.getState().get(), new StateCertificate(certificate.getState(), certificate.getAtom(), certificate.getBlock(), 
																					 certificate.getInput(), certificate.getOutput(), certificate.getExecution(), 
																					 certificate.getMerkle(), certificate.getAudit(),
																					 certificate.getPowers(), certificate.getSignatures()));
		}

		// TODO need to validate that we have a VotePowerBloom for all the shard groups referenced by the StateCertificates
		this.powers = new LinkedHashMap<Long, VotePowerBloom>();
		for (StateCertificate certificate : certificates)
			this.powers.put(certificate.getPowerBloom().getShardGroup(), certificate.getPowerBloom());
	}

	public Hash getAtom()
	{
		return this.atom;
	}

	public Collection<VotePowerBloom> getVotePowers()
	{
		return new ArrayList<VotePowerBloom>(this.powers.values());
	}

	@Override
	public <T> T getObject()
	{
		return (T) this.atom;
	}
	
	public StateCertificate get(StateKey<?, ?> state)
	{
		return this.certificates.get(state.get());
	}

	@Override
	public boolean verify(final ECPublicKey signer, final ECSignature signature)
	{
		for (StateCertificate certificate : this.certificates.values())
			if (certificate.getSignatures().getSigners().contains(signer) == true)
				return certificate.verify(signer, signature);

		return false;
	}

	public Collection<StateCertificate> getAll()
	{
		return new ArrayList<StateCertificate>(this.certificates.values());
	}
}
