package org.fuserleer.network.peers;

import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.messages.HandshakeMessage;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.Context;
import org.fuserleer.common.Direction;
import org.fuserleer.crypto.BLSSignature;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.network.peers.events.PeerBannedEvent;
import org.fuserleer.network.peers.events.PeerConnectedEvent;
import org.fuserleer.network.peers.events.PeerConnectingEvent;
import org.fuserleer.network.peers.events.PeerDisconnectedEvent;
import org.fuserleer.time.Time;

public abstract class ConnectedPeer extends Peer
{
	private static final Logger networkLog = Logging.getLogger("network");

	private static final int DEFAULT_BANTIME = 60 * 60;

	private transient Direction direction;
	private transient Semaphore	handshake = new Semaphore(1);
	private transient PeerState	state = PeerState.DISCONNECTED;

	private transient final ECKeyPair ephemeralLocalKeyPair;
	private transient final BLSSignature ephemeralKeyBinding;
	private transient volatile ECPublicKey ephemeralRemotePublicKey;

	private transient final Context context;

	ConnectedPeer(Context context, URI host, Direction direction, Peer peer) throws CryptoException 
	{
		super(host, peer);
		
		this.context = Objects.requireNonNull(context);
		this.direction = Objects.requireNonNull(direction);
		
		this.ephemeralLocalKeyPair = new ECKeyPair();
		this.ephemeralKeyBinding = context.getNode().getKeyPair().sign(this.ephemeralLocalKeyPair.getPublicKey().toByteArray());
	}

	Context getContext()
	{
		return this.context;
	}
	
	public final Direction getDirection()
	{
		return this.direction;
	}
	
	public abstract Protocol getProtocol();
	
	// CONNECTIVITY //
	public abstract void connect() throws IOException, SocketException;

	synchronized final void onConnecting() throws IOException
	{
		networkLog.info(this.context+": Connectioned opened on "+toString());

		setState(PeerState.CONNECTING);
		setActiveAt(0l);
		setConnectingAt(Time.getSystemTime());
		
		if (this.direction.equals(Direction.OUTBOUND) == true)
		{
			setAttempts(getAttempts()+1);
			setAttemptedAt(Time.getSystemTime());
			// NOTE if reattempt delay is too high, the power applied will make it eons in length!
			setAttemptAt(Time.getSystemTime() + TimeUnit.SECONDS.toMillis((long) Math.pow(getContext().getConfiguration().get("network.peer.reattempt", 10), getAttempts())));
		}
		
		this.context.getEvents().post(new PeerConnectingEvent(this));
		
		send(new HandshakeMessage(this.context.getNode(), this.ephemeralLocalKeyPair.getPublicKey(), this.ephemeralKeyBinding));
	}

	synchronized void onConnected()
	{
		if (hasProtocol() == false)
		{
			disconnect("Peer can not be connected without a protocol");
			throw new IllegalStateException("Peer can not be connected without a protocol");
		}
		
		setState(PeerState.CONNECTED);
		setAttempts(0);
		setAttemptAt(0);
		setConnectedAt(Time.getSystemTime());
		this.context.getEvents().post(new PeerConnectedEvent(this));
	}

	public final boolean isHandshaked()
	{
		return this.handshake.availablePermits() == 0;
	}

	public final boolean handshake(final ECPublicKey ephemeralRemotePublicKey)
	{
		Objects.requireNonNull(ephemeralRemotePublicKey, "Ephemeral remote public key is null");
		
		if (this.handshake.tryAcquire() == false)
			throw new IllegalStateException("Handshake already performed!");

		// Handshake achieved
		if (this.handshake.availablePermits() == 0)
		{
			this.ephemeralRemotePublicKey = ephemeralRemotePublicKey;
			onConnected();
			return true;
		}
		
		return false;
	}
	
	public final void ban(String reason)
	{
		ban(reason, DEFAULT_BANTIME);
	}

	public final void ban(String reason, int duration)
	{
		networkLog.info(getContext().getName()+": "+toString()+" - Banned for "+duration+" seconds due to "+reason);

		setBanReason(reason);
		setBannedUntil(Time.getSystemTime()+(duration*1000l));

		disconnect(reason);

		this.context.getEvents().post(new PeerBannedEvent(this));
	}
	
	public final synchronized void disconnect(String reason)
	{
		disconnect(reason, null);
	}

	public final synchronized void disconnect(String reason, Throwable throwable)
	{
		if (getState().equals(PeerState.DISCONNECTING) || getState().equals(PeerState.DISCONNECTED))
			return;

		try
		{
			setState(PeerState.DISCONNECTING);

			if (reason != null)
			{
				if (throwable != null)
					networkLog.error(getContext().getName()+": "+toString()+" - Disconnected - "+reason, throwable);
				else
					networkLog.error(getContext().getName()+": "+toString()+" - Disconnected - "+reason);
			}
			else
			{
				if (throwable != null)
					networkLog.error(getContext().getName()+": "+toString()+" - Disconnected - ", throwable);
				else
					networkLog.info(getContext().getName()+": "+toString()+" - Disconnected");
			}
		}
		catch(Exception e)
		{
			networkLog.error("Exception in disconnect of "+getContext().getName()+": "+toString(), e);
		}
		finally
		{
			onDisconnected();
		}
	}

	synchronized void onDisconnected()
	{
		setState(PeerState.DISCONNECTED);
		setDisconnectedAt(Time.getSystemTime());
		this.context.getEvents().post(new PeerDisconnectedEvent(this));
	}

	final ECKeyPair getEphemeralLocalKeyPair()
	{
		return this.ephemeralLocalKeyPair;
	}
	
	final BLSSignature getEphemeralKeyBinding()
	{
		return this.ephemeralKeyBinding;
	}

	public final ECPublicKey getEphemeralRemotePublicKey()
	{
		return this.ephemeralRemotePublicKey;
	}

	public abstract void send(Message message) throws IOException;

	public abstract void receive(Message message) throws IOException; 

	// STATE //
	public final PeerState getState()
	{
		return this.state;
	}

	public final void setState(PeerState state)
	{
		this.state = state;
	}
	
	@Override
	public String toString()
	{
		return getProtocol()+" "+(getNode().isSynced() == true ? "synced" : "unsynced")+" "+super.toString()+" "+getState()+" "+getDirection();
	}
}
