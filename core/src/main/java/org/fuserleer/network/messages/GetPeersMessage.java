package org.fuserleer.network.messages;

import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("network.message.getpeers")
public final class GetPeersMessage extends Message
{
	public GetPeersMessage()
	{
		super();
	}
}
