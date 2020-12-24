package org.fuserleer.ledger.atoms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.fuserleer.crypto.Certificate;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.ECSignature;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.atoms.certificate")
public final class AtomCertificate extends Certificate
{
	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;
	
	@JsonProperty("certificates")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=LinkedHashMap.class)
	private Map<Hash, ParticleCertificate> certificates;

	private AtomCertificate()
	{
		super();
		
		// FOR SERIALIZER
	}
	
	public AtomCertificate(final Hash atom, final Collection<ParticleCertificate> certificates) throws CryptoException, ValidationException
	{
		super(!Objects.requireNonNull(certificates, "Certificates is null").stream().anyMatch(c -> c.getDecision() == false));
		
		if (Objects.requireNonNull(atom, "Atom is null").equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Atom is ZERO");
		this.atom = atom;
		
		if (certificates.isEmpty() == true)
			throw new IllegalArgumentException("Certificates is empty");
		
		this.certificates = new LinkedHashMap<Hash, ParticleCertificate>();
		for (ParticleCertificate certificate : certificates)
		{
			if (certificate.getAtom().equals(this.atom) == false)
				throw new ValidationException("Certificate for particle "+certificate.getParticle()+" does not reference atom "+this.atom);
			
			this.certificates.put(certificate.getParticle(), certificate);
		}
	}

	public Hash getAtom()
	{
		return this.atom;
	}

	@Override
	public <T> T getObject()
	{
		return (T) this.atom;
	}
	
	public ParticleCertificate get(Hash particle)
	{
		return this.certificates.get(particle);
	}

	@Override
	public boolean verify(final ECPublicKey signer, final ECSignature signature)
	{
		for (ParticleCertificate certificate : this.certificates.values())
			if (certificate.getSignatures().getSigners().contains(signer) == true)
				return certificate.verify(signer, signature);

		return false;
	}

	public Collection<ParticleCertificate> getAll()
	{
		return new ArrayList<ParticleCertificate>(this.certificates.values());
	}
}
