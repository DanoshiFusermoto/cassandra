package org.fuserleer.ledger.messages;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.atom.broadcast")
public final class AtomBroadcastMessage extends Message
{
	public final static int MAX_ATOMS = 64;
	
	@JsonProperty("atoms")
	@DsonOutput(Output.ALL)
	private List<Hash> atoms;

	AtomBroadcastMessage()
	{
		// Serializer only
	}

	public AtomBroadcastMessage(List<Hash> atoms)
	{
		super();

		Objects.requireNonNull(atoms, "Atoms is null");
		if (atoms.isEmpty() == true)
			throw new IllegalArgumentException("Atoms is empty");
		
		if (atoms.size() > MAX_ATOMS)
			throw new IllegalArgumentException("Atoms is greater than allowed max of "+MAX_ATOMS);

		this.atoms = new ArrayList<Hash>(atoms);
	}

	public List<Hash> getAtoms()
	{
		return this.atoms;
	}
}
