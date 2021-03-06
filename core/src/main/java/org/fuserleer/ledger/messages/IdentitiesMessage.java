package org.fuserleer.ledger.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.message.identities")
public class IdentitiesMessage extends Message
{
	@JsonProperty("identities")
	@DsonOutput(Output.ALL)
	private List<BLSPublicKey> identities;

	@SuppressWarnings("unused")
	private IdentitiesMessage()
	{
		super();
	}
	
	public IdentitiesMessage(final Collection<BLSPublicKey> identities)
	{
		super();
		
		Objects.requireNonNull(identities, "Identities is null");
		if (identities.isEmpty() == true)
			throw new IllegalArgumentException("Identities is empty");
		
		this.identities = new ArrayList<BLSPublicKey>(identities);
	}

	public List<BLSPublicKey> getIdentities() 
	{ 
		return this.identities; 
	}
}
