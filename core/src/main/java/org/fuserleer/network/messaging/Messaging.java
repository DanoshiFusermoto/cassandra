package org.fuserleer.network.messaging;

import java.io.IOException;
import java.net.SocketException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.exceptions.QueueFullException;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.ledger.messages.SyncAcquiredMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.messages.HandshakeMessage;
import org.fuserleer.network.messages.NodeMessage;
import org.fuserleer.common.Agent;
import org.fuserleer.common.Direction;
import org.fuserleer.crypto.BLSSignature;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.node.Node;
import org.fuserleer.time.Time;

public class Messaging
{
	private static final Logger log = Logging.getLogger();
	private static final Logger messagingLog = Logging.getLogger("messaging");

	private final class QueuedMessage
	{
		private final Message		message;
		private final ConnectedPeer	peer;
		
		public QueuedMessage(final Message message, final ConnectedPeer peer)
		{
			super();
			
			this.message = message;
			this.peer = peer;
		}

		public Message getMessage()
		{
			return this.message;
		}

		public ConnectedPeer getPeer()
		{
			return this.peer;
		}
	}

	private final Map<Class<? extends Message>, Map<Class<?>, MessageProcessor>> listeners = new HashMap<Class<? extends Message>, Map<Class<?>, MessageProcessor>>();

	private final BlockingQueue<QueuedMessage>	inboundQueue;
	private final BlockingQueue<QueuedMessage>	outboundQueue;
	
	private final Executable inboundExecutable = new Executable()
	{
		@Override
		public void execute()
		{
			while (isTerminated() == false)
			{
				QueuedMessage inboundMessage = null;

				try 
				{
					inboundMessage = Messaging.this.inboundQueue.poll(1, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// Exit if we are interrupted
					Thread.currentThread().interrupt();
					break;
				}

				if (inboundMessage == null)
					continue;

				try
				{
					Messaging.this.bytesIn.addAndGet(inboundMessage.getMessage().getSize());

					if (inboundMessage.getPeer().getState().equals(PeerState.DISCONNECTING) == true ||
						inboundMessage.getPeer().getState().equals(PeerState.DISCONNECTED) == true)
						continue;
					
					if (messagingLog.hasLevel(Logging.DEBUG) == true)
						messagingLog.debug(Messaging.this.context+": Received "+inboundMessage.message.getCommand()+" from "+inboundMessage.getPeer()+" "+inboundMessage.message);

					if (inboundMessage.getMessage().getSender() == null)
					{
						inboundMessage.getPeer().disconnect("Sent orphan message "+inboundMessage.message.getCommand());
						continue;
					}
					
					if (inboundMessage.getMessage().getSignature() == null)
					{
						inboundMessage.getPeer().disconnect("Sent unsigned message "+inboundMessage.message.getCommand());
						continue;
					}

					if (Time.getSystemTime() - inboundMessage.getMessage().getTimestamp() > (Messaging.this.context.getConfiguration().get("messaging.time_to_live", 30)*1000l))
					{
//						SystemMetaData.getDefault().increment("messages.inbound.discarded");
						continue;
					}
					
					Node node = null;
					// MUST send a HandshakeMessage first to establish handshake //
					if (inboundMessage.getPeer().isHandshaked() == false)
					{
						if(inboundMessage.getMessage() instanceof HandshakeMessage == false)
						{
							inboundMessage.getPeer().disconnect("Did not send HandshakeMessage");
							continue;
						}
						
						node = ((HandshakeMessage)inboundMessage.getMessage()).getNode();
						if (node == null)
						{
							inboundMessage.getPeer().disconnect(inboundMessage.getMessage()+": Didn't send node object in handshake");
							continue;
						}

						ECPublicKey ephemeralRemotePublicKey = ((HandshakeMessage)inboundMessage.getMessage()).getEphemeralKey();
						if (ephemeralRemotePublicKey == null)
						{
							inboundMessage.getPeer().disconnect(inboundMessage.getMessage()+": Didn't send ephemeral public key in handshake");
							continue;
						}

						BLSSignature ephemeralBindingSignature = ((HandshakeMessage)inboundMessage.getMessage()).getBinding();
						if (ephemeralBindingSignature == null)
						{
							inboundMessage.getPeer().disconnect(inboundMessage.getMessage()+": Didn't send BLS signature binding ephemeral public key to identity in handshake");
							continue;
						}

						if (node.getIdentity().verify(ephemeralRemotePublicKey.toByteArray(), ephemeralBindingSignature) == false)
						{
							inboundMessage.getPeer().disconnect(inboundMessage.getMessage()+": BLS signature binding ephemeral public key to identity failed");
							continue;
						}
						
						inboundMessage.getPeer().handshake(ephemeralRemotePublicKey);
					}

					if (inboundMessage.getMessage() instanceof NodeMessage)
						node = ((NodeMessage)inboundMessage.getMessage()).getNode();
					
					// Remote node update?
					if (node != null)
					{
						if (node.getIdentity() == null)
						{
							inboundMessage.getPeer().disconnect(inboundMessage.getMessage()+": Gave null identity");
							continue;
						}

						if (node.getAgentVersion() <= Agent.REFUSE_AGENT_VERSION)
						{
							inboundMessage.getPeer().disconnect("Old peer "+inboundMessage.getPeer().getURI()+" "+node.getAgent()+":"+node.getProtocolVersion());
							continue;
						}

						if (node.getIdentity().equals(Messaging.this.context.getNode().getIdentity()))
						{
							inboundMessage.getPeer().ban("Message from self");
							continue;
						}

						if (inboundMessage.getPeer().getState().equals(PeerState.CONNECTED) == false)
						{
							Peer knownPeer = Messaging.this.context.getNetwork().getPeerStore().get(node.getIdentity());

							if (knownPeer != null && knownPeer.getBannedUntil() > System.currentTimeMillis())
							{
								inboundMessage.getPeer().ban("Banned peer "+node.getIdentity()+" at "+inboundMessage.getPeer().toString());
								continue;
							}
							
							inboundMessage.getPeer().setNode(node);
						}
					}
					
					// Handle this here directly to ensure that any processors that might reference 
					// the node object know the sync status is updated
					if (inboundMessage.getMessage() instanceof SyncAcquiredMessage)
					{
						inboundMessage.getPeer().getNode().setHead(((SyncAcquiredMessage)inboundMessage.getMessage()).getHead());
						inboundMessage.getPeer().getNode().setSynced(true);
						messagingLog.info(Messaging.this.context.getName()+": Received SyncAcquiredMessage with block header "+((SyncAcquiredMessage)inboundMessage.getMessage()).getHead()+" for "+inboundMessage.getPeer());
					}
					
					// FIXME why is this failing??
					if (inboundMessage.getMessage().verify(inboundMessage.getPeer().getEphemeralRemotePublicKey()) == false)
					{
						inboundMessage.getPeer().disconnect(inboundMessage.getMessage()+": Did not validate against ephemeral key "+inboundMessage.getPeer().getEphemeralRemotePublicKey()+" "+inboundMessage.getPeer().getURI()+":"+inboundMessage.getPeer().getNode().getIdentity());
						continue;
					}
					
					Messaging.this.receivedTotal.incrementAndGet();
					Messaging.this.received.compute(inboundMessage.getMessage().getClass(), (c, ai) -> {
						if (ai == null)
							return new AtomicLong(1);
						
						ai.incrementAndGet();
						return ai;
					});
					inboundMessage.getPeer().receive(inboundMessage.getMessage());
				}
				catch (Exception ex)
				{
					messagingLog.error(inboundMessage.getMessage()+": Pre-processing from "+inboundMessage.getPeer().getURI()+" failed", ex);
					continue;
				}

				// MESSAGING PROCESSING //
				synchronized (Messaging.this.listeners)
				{
					Map<Class<?>, MessageProcessor> listeners = Messaging.this.listeners.get(inboundMessage.getMessage().getClass());

					if (listeners != null)
					{
						for (MessageProcessor listener : listeners.values())
						{
							try
							{
								listener.process(inboundMessage.getMessage(), inboundMessage.getPeer());
							}
							catch (Exception ex)
							{
								messagingLog.error(inboundMessage.getMessage()+" from "+inboundMessage.getPeer().getURI()+" failed", ex);
							}
						}
					}
				}
			}
		}
	};

	private final Executable outboundExecutable = new Executable()
	{
		@Override
		public void execute()
		{
			while (isTerminated() == false)
			{
				QueuedMessage outboundMessage = null;

				try 
				{
					outboundMessage = Messaging.this.outboundQueue.poll(1, TimeUnit.SECONDS);
				} 
				catch (InterruptedException e) 
				{
					// Exit if we are interrupted
					Thread.currentThread().interrupt();
					break;
				}

				if (outboundMessage == null)
					continue;

				if (messagingLog.hasLevel(Logging.DEBUG) == true)
					messagingLog.debug(Messaging.this.context+": Sending "+outboundMessage.message.getCommand()+" to "+outboundMessage.getPeer()+" "+outboundMessage.message);

				if (Time.getSystemTime() - outboundMessage.getMessage().getTimestamp() > (Messaging.this.context.getConfiguration().get("messaging.time_to_live", 30)*1000l))
				{
					messagingLog.warn(outboundMessage.getMessage()+": TTL to "+outboundMessage.getPeer()+" has expired");
					continue;
				}

				try
				{
					Messaging.this.sentTotal.incrementAndGet();
					Messaging.this.sent.compute(outboundMessage.getMessage().getClass(), (c, ai) -> {
						if (ai == null)
							return new AtomicLong(1);
						
						ai.incrementAndGet();
						return ai;
					});
					outboundMessage.getPeer().send(outboundMessage.getMessage());
					Messaging.this.bytesOut.addAndGet(outboundMessage.getMessage().getSize());
				}
				catch (Exception ex)
				{
					messagingLog.error(outboundMessage.getMessage()+" to "+outboundMessage.getPeer()+" failed", ex);
				}
			}
		}
	};

	private final Context context;
	
	private Thread inboundThread = null;
	private Thread outboundThread = null;
	
	private AtomicLong bytesIn = new AtomicLong(0l);
	private AtomicLong bytesOut = new AtomicLong(0l);
	private AtomicLong sentTotal = new AtomicLong(0l);
	private AtomicLong receivedTotal = new AtomicLong(0l);
	private final Map<Class<?>, AtomicLong> received;
	private final Map<Class<?>, AtomicLong> sent;

	public Messaging(final Context context)
	{ 
		this.context = Objects.requireNonNull(context);
		
		this.inboundQueue = new LinkedBlockingQueue<QueuedMessage>(this.context.getConfiguration().get("messaging.inbound.queue_max", 16384));
		this.outboundQueue = new LinkedBlockingQueue<QueuedMessage>(this.context.getConfiguration().get("messaging.outbound.queue_max", 32768));
		this.received = Collections.synchronizedMap(new HashMap<Class<?>, AtomicLong>());
		this.sent = Collections.synchronizedMap(new HashMap<Class<?>, AtomicLong>());

		// GOT IT!
		messagingLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
	}

	public void start() throws StartupException
	{
		this.inboundThread = new Thread(this.inboundExecutable);
		this.inboundThread.setDaemon (true);
		this.inboundThread.setName(this.context.getName()+" Messaging-Inbound");
		this.inboundThread.start();

		this.outboundThread = new Thread(this.outboundExecutable);
		this.outboundThread.setDaemon (true);
		this.outboundThread.setName(this.context.getName()+" Messaging-Outbound");
		this.outboundThread.start();
	}

	public void stop() throws TerminationException
	{
		this.inboundExecutable.terminate(true);
		this.outboundExecutable.terminate(true);
	}

	public void register(final Class<? extends Message> type, final Class<?> owner, final MessageProcessor listener)
	{
		Objects.requireNonNull(type, "Type class for registration is null");
		Objects.requireNonNull(owner, "Owner class for registration is null");
		Objects.requireNonNull(listener, "Listener for registration is null");
		
		synchronized(this.listeners)
		{
			if (this.listeners.containsKey(type) == false)
				this.listeners.put(type, new HashMap<Class<?>, MessageProcessor>());

			if (this.listeners.get(type).containsKey(owner) == false)
				listeners.get(type).put(owner, listener);
		}
	}

	public void deregister(final MessageProcessor<? extends Message> listener)
	{
		Objects.requireNonNull(listener, "Listener for deregistration is null");

		synchronized(this.listeners)
		{
			for (Class<? extends Message> type : this.listeners.keySet())
			{
				Iterator<MessageProcessor> listenerIterator = this.listeners.get(type).values().iterator();
				while (listenerIterator.hasNext())
				{
					if (listenerIterator.next() == listener)
					{
						listenerIterator.remove();
						break;
					}
				}
			}
		}
	}

	public void deregisterAll(final Class<?> owner)
	{
		Objects.requireNonNull(owner, "Owner for blanket deregistration is null");

		synchronized(this.listeners)
		{
			for (Class<? extends Message> type : this.listeners.keySet())
			{
				Iterator<Class<?>> listenerOwnerIterator = this.listeners.get(type).keySet().iterator();
				while (listenerOwnerIterator.hasNext())
				{
					if (listenerOwnerIterator.next() == owner)
					{
						listenerOwnerIterator.remove();
						break;
					}
				}
			}
		}
	}

	public void received(final Message message, final ConnectedPeer peer) throws IOException
	{
		Objects.requireNonNull(message, "Message received is null");
		Objects.requireNonNull(peer, "Peer received from is null");

		if (this.inboundQueue.offer(new QueuedMessage(message, peer)) == false)
		{
			messagingLog.error(message+": Inbound queue is full "+peer);
			throw new QueueFullException(message+": Inbound queue is full "+peer);
		}
	}

	public void send(final Message message, final ConnectedPeer peer) throws IOException
	{
		try
		{
			Objects.requireNonNull(message, "Message to send is null");
			Objects.requireNonNull(peer, "Peer to send to is null");

			if (peer.getState().equals(PeerState.DISCONNECTED) || peer.getState().equals(PeerState.DISCONNECTING))
				throw new SocketException(peer+" is "+peer.getState());

			message.setDirection(Direction.OUTBOUND);

			if (this.outboundQueue.offer(new QueuedMessage(message, peer), 1, TimeUnit.SECONDS) == false)
			{
				messagingLog.error(message+": Outbound queue is full "+peer);
				throw new QueueFullException(message+": Outbound queue is full "+peer);
			}
		} 
		catch (InterruptedException ex) 
		{
			messagingLog.error(message+": Sending to "+peer+" failed", ex);
			// Not going to handle it here.
			Thread.currentThread().interrupt();
			throw new IOException("While sending message", ex);
		} 
		catch (IOException ex) 
		{
			messagingLog.error(message+": Sending to "+peer+" failed", ex);
			throw ex;
		}			
		catch (Exception ex) 
		{
			messagingLog.error(message+": Sending to "+peer+" failed", ex);
			throw new IOException(ex);
		}
	}

	public long getBytesIn()
	{
		return this.bytesIn.get();
	}

	public long getBytesOut()
	{
		return this.bytesOut.get();
	}
	
	public long getTotalSent()
	{
		return this.sentTotal.get();
	}

	public long getTotalReceived()
	{
		return this.receivedTotal.get();
	}
	
	public Collection<Entry<Class<?>, Long>> getReceivedByType()
	{
		synchronized(this.received)
		{
			return this.received.entrySet().stream().map(e -> new AbstractMap.SimpleEntry<Class<?>, Long>(e.getKey(), e.getValue().get())).collect(Collectors.toList());
		}
	}

	public Collection<Entry<Class<?>, Long>> getSentByType()
	{
		synchronized(this.sent)
		{
			return this.sent.entrySet().stream().map(e -> new AbstractMap.SimpleEntry<Class<?>, Long>(e.getKey(), e.getValue().get())).collect(Collectors.toList());
		}
	}

	public void resetBytesInOut()
	{
		this.bytesIn.set(0);
		this.bytesOut.set(0);
	}
}
