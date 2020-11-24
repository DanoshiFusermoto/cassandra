package org.fuserleer.ledger.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import org.fuserleer.crypto.Hash;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.atoms.get")
public final class GetAtomsMessage extends Message
{
	public final static int MAX_ATOMS = 1024;
	
	@JsonProperty("nonce")
	@DsonOutput(Output.ALL)
	private long nonce;

	@JsonProperty("atoms")
	@DsonOutput(Output.ALL)
	private List<Hash> atoms;
	
	private GetAtomsMessage()
	{
		super();
		
		this.nonce = ThreadLocalRandom.current().nextLong();
	}
	
	public GetAtomsMessage(Collection<Hash> atoms)
	{
		this();
		
		Objects.requireNonNull(atoms, "Atoms is null");
		if (atoms.isEmpty() == true)
			throw new IllegalArgumentException("Atoms is empty");
		
		if (atoms.size() > MAX_ATOMS)
			throw new IllegalArgumentException("Atoms is greater than allowed max of "+MAX_ATOMS);

		this.atoms = new ArrayList<Hash>(atoms);
	}
	
	public List<Hash> getAtoms()
	{
		return Collections.unmodifiableList(this.atoms);
	}
	
	public long getNonce()
	{
		return this.nonce;
	}
}

