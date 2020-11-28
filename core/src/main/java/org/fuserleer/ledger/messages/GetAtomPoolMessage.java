package org.fuserleer.ledger.messages;

import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.messages.atom.get.pool")
public final class GetAtomPoolMessage extends Message
{
	public GetAtomPoolMessage()
	{
		super();
	}
}