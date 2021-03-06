package org.fuserleer.network.peers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.fuserleer.Context;
import org.fuserleer.common.Direction;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.BanException;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.Polymorphic;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("network.peer")
public final class TCPPeer extends ConnectedPeer implements Polymorphic
{
	private static final Logger networkLog = Logging.getLogger("network");
	private static final Logger messagingLog = Logging.getLogger("messaging");

	private class TCPProcessor implements Runnable
	{
		@Override
		public void run ()
		{
			Message message = null;

			try
			{
				internalConnect();
	
				while (TCPPeer.this.socket.isClosed() == false && 
					   TCPPeer.this.getState().equals(PeerState.DISCONNECTING) == false && 
					   TCPPeer.this.getState().equals(PeerState.DISCONNECTED) == false)
				{
					try
					{
						message = Message.parse(socket.getInputStream());
					}
					catch(IOException ioex)
					{
						disconnect(ioex.getMessage(), ioex);
						return;
					}
					catch(BanException bex)
					{
						ban(bex.getMessage(), 60);  // TODO increase this!
						return;
					}
					catch(InterruptedException iex)
					{
						continue;
					}
					catch ( Exception ex )
					{
						disconnect("Exception in message parsing", ex);
						return;
					}
	
					try
					{
						TCPPeer.this.getContext().getNetwork().getMessaging().received(message, TCPPeer.this);
					}
					catch(Exception ex)
					{
						messagingLog.error(TCPPeer.this+" receive error", ex);
					}
				}
				
				networkLog.warn("TCPProcessor thread "+TCPThread.getName()+" is quitting");
			}
			catch (IOException ioex)
			{
				networkLog.error("TCPProcessor thread "+TCPThread.getName()+" threw "+ioex);
			}
		}
	}

	private transient Socket	socket = null;
	private transient Thread 	TCPThread = null;
	
	public TCPPeer(Context context, Socket socket, URI host, Direction direction, Peer peer) throws IOException, CryptoException
	{
		super(context, host, direction, peer);
		
		this.socket = socket;
		addProtocol(Protocol.TCP);
	}
	
	@Override
	public void connect() throws IOException
	{
		try
		{
			onConnecting();
	
			if (getDirection().equals(Direction.OUTBOUND) == true)
				listen();
		}
		catch (IOException ioex)
		{
			onDisconnected();
			throw ioex;
		}
	}

	private IOException internalConnectException = null;
	private synchronized void internalConnect() throws IOException
	{
		if (this.internalConnectException != null)
			throw this.internalConnectException;
		
		try
		{
			if (this.socket.isConnected() == false)
				this.socket.connect(new InetSocketAddress(getURI().getHost(), getURI().getPort()), (int) TimeUnit.SECONDS.toMillis(getContext().getConfiguration().get("network.peer.connect.timeout", 5)));
		}
		catch (IOException ioex)
		{
			this.internalConnectException = ioex;
			throw ioex;
		}
		catch(Throwable throwable)
		{
			this.internalConnectException = new IOException(throwable);
			throw this.internalConnectException;
		}
	}
	
	@Override
	public final synchronized void onConnected()
	{
		super.onConnected();

		if (getDirection().equals(Direction.INBOUND) == true)
			listen();
	}
	
	private final void listen()
	{
		this.TCPThread = new Thread(new TCPProcessor());
		this.TCPThread.setDaemon(true);
		this.TCPThread.setName(getContext().getName()+" Peer-"+socket.getInetAddress()+"-TCP");
		this.TCPThread.start();
	}
	
	@Override
	public final synchronized void onDisconnected()
	{
		if (this.socket.isClosed() == false)
			try { socket.close(); } catch (Exception ex) { networkLog.error(this.socket.getInetAddress()+" close threw exception", ex); }

		super.onDisconnected();
	}

	@Override
	public Protocol getProtocol()
	{
		return Protocol.TCP;
	}
	
	@Override
	public synchronized void send(Message message) throws IOException
	{
		internalConnect();
		
		try
		{
			byte[] bytes = Message.prepare(message, getEphemeralLocalKeyPair());
			if (bytes.length > Message.MAX_MESSAGE_SIZE)
				throw new IOException("Message to "+this+" of size "+bytes.length+" is too large");
	
			this.socket.getOutputStream().write(bytes);
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
}
