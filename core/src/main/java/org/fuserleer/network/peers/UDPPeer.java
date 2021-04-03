package org.fuserleer.network.peers;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;

import org.fuserleer.Context;
import org.fuserleer.common.Direction;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.Polymorphic;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("network.peer")
public final class UDPPeer extends ConnectedPeer implements Polymorphic
{
	private static final Logger networkLog = Logging.getLogger("network");
	private static final Logger messagingLog = Logging.getLogger("messaging");

	private DatagramChannel		socket = null;

	public UDPPeer(Context context, DatagramChannel socket, URI host, Direction direction, Peer peer) throws IOException
	{
		super(context, host, direction, peer);

		this.socket = socket;
		addProtocol(Protocol.UDP);
	}

	@Override
	public Protocol getProtocol()
	{
		return Protocol.UDP;
	}

	@Override
	public synchronized void send(Message message) throws IOException
	{
		try
		{
			byte[] bytes = Message.prepare(message, getContext().getNode().getIdentityKey());
			if (bytes.length > 65535)
				throw new IOException("Datagram packet to "+this+" of size "+bytes.length+" is too large");
	
			ByteBuffer buf = ByteBuffer.wrap(bytes);
			InetSocketAddress addr = new InetSocketAddress(InetAddress.getByName(getURI().getHost()), getURI().getPort());
			int sent = this.socket.send(buf, addr);
			if (sent != bytes.length)
				throw new IOException("Message was not completely sent "+sent+"/"+bytes.length+" bytes");
		}
		catch (CryptoException cex)
		{
			throw new IOException("Message signing failed", cex);
		}
	}
	
	@Override
	public void receive(Message message) throws IOException
	{
		// DO NOTHING //
	}

	@Override
	public void connect() throws IOException
	{
		onConnecting();
	}
}
