package org.fuserleer.ledger.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.atoms")
public class AtomsMessage extends Message
{
	public final static int MAX_ATOMS = 16;

	@JsonProperty("nonce")
	@DsonOutput(Output.ALL)
	private long nonce;
	
	@JsonProperty("atoms")
	@DsonOutput(Output.ALL)
	private List<Atom> atoms;

	AtomsMessage()
	{
		// Serializer only
	}

	public AtomsMessage(long nonce, Collection<Atom> atoms)
	{
		super();

		this.nonce = nonce;
		if (Objects.requireNonNull(atoms, "Atoms is null").isEmpty() == true)
			throw new IllegalArgumentException("Atoms is empty");

		if (atoms.size() > AtomsMessage.MAX_ATOMS)
			throw new IllegalArgumentException("Atoms is greater than allowed max of "+AtomsMessage.MAX_ATOMS);

		this.atoms = new ArrayList<Atom>(atoms);
	}
	
	public List<Atom> getAtoms()
	{
		return this.atoms;
	}
	
	public long getNonce()
	{
		return this.nonce;
	}
}
