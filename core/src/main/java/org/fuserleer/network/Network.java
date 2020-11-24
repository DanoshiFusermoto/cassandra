package org.fuserleer.network;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.StandardProtocolFamily;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.Universe;
import org.fuserleer.common.Agent;
import org.fuserleer.common.Direction;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.executors.ScheduledExecutable;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.discovery.RemoteLedgerDiscovery;
import org.fuserleer.network.discovery.Whitelist;
import org.fuserleer.network.messages.NodeMessage;
import org.fuserleer.network.messaging.Message;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.messaging.Messaging;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.Peer;
import org.fuserleer.network.peers.PeerHandler;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.PeerStore;
import org.fuserleer.network.peers.RUDPPeer;
import org.fuserleer.network.peers.TCPPeer;
import org.fuserleer.network.peers.events.PeerAvailableEvent;
import org.fuserleer.network.peers.events.PeerConnectedEvent;
import org.fuserleer.network.peers.events.PeerConnectingEvent;
import org.fuserleer.network.peers.events.PeerDisconnectedEvent;
import org.fuserleer.network.peers.filters.TCPPeerFilter;
import org.fuserleer.node.Node;
import org.fuserleer.time.Time;

import com.google.common.eventbus.Subscribe;

public final class Network implements Service
{
	private static final Logger networkLog = Logging.getLogger("network");
	
	public static final int DEFAULT_PEER_INACTIVITY = 60; 
	public static final int DEFAULT_CONNECT_TIMEOUT = 60; 
	public static final int DEFAULT_TCP_CONNECTIONS_OUT = 4; 
	public static final int DEFAULT_TCP_CONNECTIONS_IN = 4; 

	private final Context		context;
	private final Messaging 	messaging;
	private final PeerStore 	peerStore;
	private final PeerHandler	peerHandler;

    private final Set<ConnectedPeer>	peers = new HashSet<ConnectedPeer>();
    private final ReentrantLock 		connecting = new ReentrantLock();
	private final Whitelist 			whitelist;

	private DatagramChannel	datagramChannel = null;
	private	final BlockingQueue<DatagramPacket> datagramQueue = new LinkedBlockingQueue<DatagramPacket>();

    private Runnable UDPListener = new Runnable()
    {
		@Override
		public void run()
		{
			ByteBuffer buffer = ByteBuffer.allocate(65536);
		    try
		    {
		    	while (Network.this.datagramChannel.isOpen() == true)
		    	{
		    		SocketAddress socketAddress;
		    		try
		    		{
		    			socketAddress = Network.this.datagramChannel.receive(buffer);
		    		}
		    		catch (SocketTimeoutException stex)
		    		{
		    			continue;
		    		}

		    		buffer.flip();
					byte[] data = new byte[buffer.limit()];
					buffer.get(data);
					DatagramPacket datagramPacket = new DatagramPacket(data, data.length, socketAddress);
		    		if (Network.this.datagramQueue.offer(datagramPacket) == false)
		    			networkLog.error("UDP datagram could not be queued");

		    		datagramPacket = null;
		    		buffer.clear();
		    	}
		    }
		    catch (Exception ex)
		    {
		    	networkLog.fatal("UDP receiver thread quit on: ", ex);
		    }
		}
	};

    private Executable UDPProcessor = new Executable()
    {
		@Override
		public void execute()
		{
		    try
		    {
		    	while (Network.this.datagramChannel.isOpen() == true && isTerminated() == false)
		    	{
		    		DatagramPacket datagramPacket = null;

					try 
					{
						datagramPacket = Network.this.datagramQueue.poll(1, TimeUnit.SECONDS);
					} 
					catch (InterruptedException e) 
					{
						// Give up and exit if interrupted
						Thread.currentThread().interrupt();
						break;
					}

					if (datagramPacket == null)
						continue;

					Network.this.connecting.lock();

					try
					{
						byte[] data = datagramPacket.getData();
						Message message = Message.parse(new ByteArrayInputStream(data, datagramPacket.getOffset(), datagramPacket.getLength()));
						
						ConnectedPeer connectedPeer = Network.this.get(message.getSender(), Protocol.UDP, PeerState.CONNECTING, PeerState.CONNECTED);
						if (connectedPeer == null)
						{
							if ((message instanceof NodeMessage) == false)
							{
								networkLog.error(datagramPacket.getAddress().toString()+" did not send NodeMessage on connect");
								continue;
							}

							NodeMessage nodeMessage = (NodeMessage)message;
							URI uri = Agent.getURI(datagramPacket.getAddress().getHostAddress(), nodeMessage.getNode().getPort());
							Peer persistedPeer = Network.this.peerStore.get(nodeMessage.getNode().getIdentity());
							connectedPeer = new RUDPPeer(Network.this.context, Network.this.datagramChannel, uri, Direction.INBOUND, persistedPeer);
						}
						
	    				Network.this.messaging.received(message, connectedPeer);
					}
					catch (Exception ex)
					{
						networkLog.error("UDP "+datagramPacket.getAddress().toString()+" error", ex);
						continue;
		    		}
		    		finally
		    		{
		    			Network.this.connecting.unlock();
		    		}
		    	}
		    }
		    catch (Exception ex)
		    {
		    	networkLog.fatal("UDP worker thread quit on: ", ex);
		    }
		}
	};
	
    private ServerSocket 	TCPServerSocket = null;

    private Executable TCPListener = new Executable()
    {
        @SuppressWarnings("unchecked")
    	private <T extends ConnectedPeer> T connect(Socket socket, Node node) throws IOException
        {
    		try
    		{
    			Network.this.connecting.lock();

    			URI uri = Agent.getURI(socket.getInetAddress().getHostAddress(), node.getPort());
				if (Network.this.has(node.getIdentity(), Protocol.TCP) == true)
				{
					networkLog.error(node.getIdentity()+" already has a socked assigned");
					return null;
				}

				Peer persistedPeer = Network.this.peerStore.get(node.getIdentity());
				ConnectedPeer connectedPeer = new TCPPeer(Network.this.context, socket, uri, Direction.INBOUND, persistedPeer);
    			return (T) connectedPeer;
    		}
    		finally
    		{
    			Network.this.connecting.unlock();
    		}
        }

        @Override
		public void execute()
		{
	    	while (Network.this.TCPServerSocket.isClosed() == false && isTerminated() == false)
	    	{
	    		try
	    		{
	    			Socket socket = null;

	    			try
	    			{
	    				socket = Network.this.TCPServerSocket.accept();
	    			}
	    			catch (SocketTimeoutException socktimeex)
	    			{
	    				continue;
	    			}

					Network.this.connecting.lock();
					try
					{
						Message message = Message.parse(socket.getInputStream());
						if ((message instanceof NodeMessage) == false)
						{
							networkLog.error(socket.toString()+" did not send NodeMessage on connect");
							socket.close();
							continue;
						}
						
						NodeMessage nodeMessage = (NodeMessage)message;
						if (nodeMessage.verify(nodeMessage.getNode().getIdentity()) == false)
						{
							networkLog.error(socket.toString()+" NodeMessage failed verification");
							socket.close();
							continue;
						}
						
						URI uri = Agent.getURI(socket.getInetAddress().getHostAddress(), nodeMessage.getNode().getPort());
						
						// Store the node in the peer store even if can not connect due to whitelists or available connections as it may be a preferred peer
						Network.this.context.getNetwork().getPeerStore().store(new Peer(uri, nodeMessage.getNode(), Protocol.TCP));
						
						if (Network.this.isWhitelisted(uri) == false)
					    {
							networkLog.debug(uri.getHost()+" is not whitelisted");
							socket.close();
							continue;
					    }
						
						List<ConnectedPeer> connected = Network.this.get(Protocol.TCP, PeerState.CONNECTING, PeerState.CONNECTED).stream().filter(cp -> cp.getDirection().equals(Direction.INBOUND)).collect(Collectors.toList());
						if (connected.size() >= Network.this.context.getConfiguration().get("network.connections.in", 8))
						{
							networkLog.debug(socket.toString()+" all inbound slots occupied");
							socket.close();
							continue;
						}
						
						ConnectedPeer connectedPeer = connect(socket, nodeMessage.getNode());
						if (connectedPeer == null)
							socket.close();
						else
							Network.this.context.getNetwork().getMessaging().received(nodeMessage, connectedPeer);
					}
					catch (Exception ex)
					{
						networkLog.error("TCP "+socket.getInetAddress()+" error", ex);
						continue;
		    		}
		    		finally
		    		{
		    			Network.this.connecting.unlock();
		    		}
	    		}
	    		catch (IOException ioex)
	    		{
	    			if (TCPServerSocket.isClosed() && isTerminated() == false)
	    			{
	    				try
	    				{
	    					networkLog.error("TCPServerSocket died "+Network.this.TCPServerSocket);
		    				Network.this.TCPServerSocket = new ServerSocket(Network.this.TCPServerSocket.getLocalPort(), 16, Network.this.TCPServerSocket.getInetAddress());
		    				Network.this.TCPServerSocket.setReceiveBufferSize(65536);
		    				Network.this.TCPServerSocket.setSoTimeout(1000);
		    				networkLog.info("Recreated TCPServerSocket on "+Network.this.TCPServerSocket);
	    				}
	    				catch (Exception ex)
	    				{
	    					networkLog.fatal("TCPServerSocket death is fatal", ex);
	    				}
	    			}
	    		}
	    		catch (Exception ex)
	    		{
	    			networkLog.fatal("TCPServerSocket exception ", ex);
	    		}
	    	}
		}
	};
	
	private Future<?> houseKeepingTaskFuture = null;

    public Network(Context context)
    {
    	super();
    	
    	this.context = Objects.requireNonNull(context);
    	this.peerStore = new PeerStore(context);
    	this.peerHandler = new PeerHandler(context);
    	this.messaging = new Messaging(context);
    	
    	this.whitelist = new Whitelist(this.context.getConfiguration().get("network.whitelist", ""));
    	
		// GOT IT!
		networkLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN | Logging.WARN);
    }
    
    public Messaging getMessaging()
    {
    	return this.messaging;
    }

    public PeerStore getPeerStore()
    {
    	return this.peerStore;
    }

    @Override
	public void start() throws StartupException
	{
    	try
    	{
	    	if (this.context.getConfiguration().has("network.address"))
	    	{
	    		this.datagramChannel = DatagramChannel.open(StandardProtocolFamily.INET);
	    		this.datagramChannel.bind(new InetSocketAddress(InetAddress.getByName(this.context.getConfiguration().get("network.address", "0.0.0.0")),
	    																			  this.context.getConfiguration().get("network.udp", Universe.getDefault().getPort())));
	    		
    			this.TCPServerSocket = new ServerSocket(this.context.getConfiguration().get("network.udp", Universe.getDefault().getPort()), 
				 	   	   								16,
				 	   	   								InetAddress.getByName(this.context.getConfiguration().get("network.address", "0.0.0.0")));
    		}
	    	else
	    	{
	    		this.datagramChannel = DatagramChannel.open(StandardProtocolFamily.INET);
	    		this.datagramChannel.bind(new InetSocketAddress(InetAddress.getByName(this.context.getConfiguration().get("network.address", "0.0.0.0")),
	    														this.context.getConfiguration().get("network.port", Universe.getDefault().getPort())));
	    		
    			this.TCPServerSocket = new ServerSocket(this.context.getConfiguration().get("network.port", Universe.getDefault().getPort()), 16);
	    	}

	    	this.datagramChannel.configureBlocking(true);
	    	this.datagramChannel.setOption(StandardSocketOptions.SO_RCVBUF, this.context.getConfiguration().get("network.udp.buffer", 1<<18));
	    	this.datagramChannel.setOption(StandardSocketOptions.SO_SNDBUF, this.context.getConfiguration().get("network.udp.buffer", 1<<18));
	    	networkLog.debug("UDP server socket "+this.datagramChannel.getLocalAddress()+" RCV_BUF: "+this.datagramChannel.getOption(StandardSocketOptions.SO_RCVBUF));
	    	networkLog.debug("UDP server socket "+this.datagramChannel.getLocalAddress()+" SND_BUF: "+this.datagramChannel.getOption(StandardSocketOptions.SO_SNDBUF));

	        Thread UDPServerThread = new Thread(this.UDPListener);
	        UDPServerThread.setDaemon(false);
	        UDPServerThread.setName(this.context.getName()+" UDP Server");
	        UDPServerThread.start();

	        Thread UDPworkerThread = new Thread(this.UDPProcessor);
	        UDPworkerThread.setDaemon(false);
	        UDPworkerThread.setName(this.context.getName()+" UDP Worker");
	        UDPworkerThread.start();
	        
	        this.TCPServerSocket.setSoTimeout(1000);
	    	this.TCPServerSocket.setReceiveBufferSize(this.context.getConfiguration().get("network.tcp.buffer", 1<<18));
	    	networkLog.debug("TCP server socket "+this.TCPServerSocket.getLocalSocketAddress()+" RCV_BUF: "+this.TCPServerSocket.getReceiveBufferSize());

			Thread TCPServerThread = new Thread(this.TCPListener); 		
			TCPServerThread.setDaemon(false);
			TCPServerThread.setName(this.context.getName()+" TCP Server");
			TCPServerThread.start();

			this.messaging.start();

			this.context.getEvents().register(this.peerListener);
			this.peerStore.start();
			this.peerHandler.start();
			
			this.messaging.register(NodeMessage.class, this.getClass(), new MessageProcessor<NodeMessage>() 
			{
				@Override
				public void process(NodeMessage message, ConnectedPeer peer)
				{
					peer.setNode(message.getNode());
					Network.this.context.getEvents().post(new PeerAvailableEvent(peer));
				}
			});
			
			// HOUSE KEEPING / DISCOVERY //
			this.houseKeepingTaskFuture = Executor.getInstance().scheduleAtFixedRate(new ScheduledExecutable(10, 10, TimeUnit.SECONDS)
			{
				@Override
				public void execute()
				{
					// Housekeeping //
					try
					{
						Set<ConnectedPeer> toDisconnect = new HashSet<ConnectedPeer>();

						synchronized(Network.this.peers)
						{
							for (ConnectedPeer peer : Network.this.peers)
							{
								if ((peer.getState().equals(PeerState.CONNECTING) == true && Time.getSystemTime() - peer.getAttemptedAt() > TimeUnit.SECONDS.toMillis(Network.this.context.getConfiguration().get("network.peer.connect.timeout", DEFAULT_CONNECT_TIMEOUT))) ||
									(peer.getState().equals(PeerState.CONNECTED) == true && 
									 Time.getSystemTime() - peer.getActiveAt() > TimeUnit.SECONDS.toMillis(Network.this.context.getConfiguration().get("network.peer.inactivity", DEFAULT_PEER_INACTIVITY)) &&
									 Time.getSystemTime() - peer.getConnectedAt() > TimeUnit.SECONDS.toMillis(Network.this.context.getConfiguration().get("network.peer.inactivity", DEFAULT_PEER_INACTIVITY))))
								{
									toDisconnect.add(peer);
								}
							}
						}

						for (ConnectedPeer peer : toDisconnect)
							peer.disconnect("Peer is silent for "+TimeUnit.MILLISECONDS.toSeconds(Time.getSystemTime() - peer.getActiveAt())+" seconds");
					}
					catch (Exception ex)
					{
						networkLog.error(ex);
					}
					
					// Discovery //
					// Discovery //
					try
					{
						List<ConnectedPeer> connected = Network.this.get(Protocol.TCP, PeerState.CONNECTING, PeerState.CONNECTED).stream().filter(cp -> cp.getDirection().equals(Direction.OUTBOUND)).collect(Collectors.toList());
						if (connected.size() < Network.this.context.getConfiguration().get("network.connections.out", 8))
						{
							Collection<Peer> preferredPeers = new RemoteLedgerDiscovery(Network.this.context).discover(new TCPPeerFilter(Network.this.context));
							for (Peer preferredPeer : preferredPeers)
							{
								if (Network.this.get(preferredPeer.getNode().getIdentity(), Protocol.TCP) != null)
									continue;
								
								Network.this.connecting.lock();
	
								try
								{
									Network.this.connect(preferredPeer.getURI(), Direction.OUTBOUND, Protocol.TCP);
									break;
								}
								catch (Exception ex)
								{
									networkLog.error("TCP "+preferredPeer.toString()+" connect error", ex);
									continue;
					    		}
					    		finally
					    		{
					    			Network.this.connecting.unlock();
					    		}
							}
						}
						else
							networkLog.debug("All outbound slots occupied");
					}
					catch (Exception ex)
					{
						networkLog.error(ex);
					}

					// System Heartbeat //
					// TODO still need this? or even node objects / messages?
					synchronized(Network.this.peers)
					{
						for (ConnectedPeer peer : Network.this.peers)
						{
							if(peer.getState().equals(PeerState.CONNECTED))
							{
								try
								{
									Network.this.getMessaging().send(new NodeMessage(Network.this.context.getNode()), peer);
								}
								catch (IOException ioex)
								{
									networkLog.error("Could not send System heartbeat to "+peer, ioex);
								}
							}
						}
	    			}
				}
			});
    	}
    	catch (Exception ex)
    	{
    		throw new StartupException(ex, this.getClass());
    	}
	}

    @Override
	public void stop() throws TerminationException
	{
    	try
		{
    		if (this.houseKeepingTaskFuture != null)
    			this.houseKeepingTaskFuture.cancel(false);
    		
			this.context.getEvents().unregister(this.peerListener);
    		
    		synchronized(this.peers)
			{
				for (ConnectedPeer peer : this.peers)
					peer.disconnect("Stopping network");
			}

			if (this.datagramChannel != null)
			{
				this.UDPProcessor.terminate(false);
				this.datagramChannel.close();
			}

			if (this.TCPServerSocket != null)
			{
				this.TCPListener.terminate(false);
				this.TCPServerSocket.close();
			}
			
			this.messaging.stop();
			this.peerHandler.stop();
			this.peerStore.stop();
		}
		catch (Exception ex)
		{
			throw new TerminationException(ex, this.getClass());
		}
	}
    
	public void clean() throws IOException
	{
		this.peerStore.clean();
	}

    @SuppressWarnings("unchecked")
	public <T extends ConnectedPeer> T connect(URI uri, Direction direction, Protocol protocol) throws IOException
    {
		try
		{
			this.connecting.lock();

			ConnectedPeer connectedPeer = get(uri, protocol);
			if (connectedPeer == null)
			{
		    	if (protocol.toString().equalsIgnoreCase(Protocol.UDP.toString()) == true)
		    	{
					Peer persistedPeer = this.peerStore.get(uri);
					connectedPeer = new RUDPPeer(this.context, this.datagramChannel, uri, direction, persistedPeer);
		    	}
		    	else if (protocol.toString().equalsIgnoreCase(Protocol.TCP.toString()) == true)
		    	{
		    		if (direction.equals(Direction.OUTBOUND) == false)
		    			throw new IllegalArgumentException("Can only specify OUTBOUND for TCP connections");
		    		
					Peer persistedPeer = this.peerStore.get(uri);

					Socket socket = new Socket();
					socket.setReceiveBufferSize(this.context.getConfiguration().get("network.tcp.buffer", 1<<18));
					socket.setSendBufferSize(this.context.getConfiguration().get("network.tcp.buffer", 1<<18));
					socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort()), (int) TimeUnit.SECONDS.toMillis(this.context.getConfiguration().get("network.peer.connect.timeout", 5)));
			
					connectedPeer = new TCPPeer(this.context, socket, uri, Direction.OUTBOUND, persistedPeer);
		    	}
			}

			return (T) connectedPeer;
		}
		finally
		{
			this.connecting.unlock();
		}
    }
    
	public List<ConnectedPeer> get(PeerState ... states)
	{
		return get(null, false, states);
	}

	public List<ConnectedPeer> get(Protocol protocol, PeerState ... states)
	{
		return get(protocol, false, states);
	}

	public List<ConnectedPeer> get(Protocol protocol, boolean shuffle, PeerState ... states)
	{
		synchronized(this.peers)
		{
			List<ConnectedPeer>	peers = new ArrayList<ConnectedPeer>();

			for (ConnectedPeer peer : this.peers)
			{
				if ((protocol == null || peer.getProtocol().equals(protocol) == true) &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(peer.getState())))
					peers.add(peer);
			}

			if (shuffle == false)
				return peers;
			else
			{
				Collections.shuffle(peers);
				return peers;
			}
		}
	}

	public int count(PeerState ... states)
	{
		synchronized(this.peers)
		{
			int count = 0;

			for (ConnectedPeer peer : this.peers)
			{
				if (states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(peer.getState()))
					count++;
			}
			
			return count;
		}
	}

	public int count(Protocol protocol, PeerState ... states)
	{
		synchronized(this.peers)
		{
			int count = 0;

			for (ConnectedPeer peer : this.peers)
			{
				if (peer.getProtocol().equals(protocol) == true &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(peer.getState())))
					count++;
			}
			
			return count;
		}
	}

	@SuppressWarnings("unchecked")
	private <T extends ConnectedPeer> T get(URI host, Protocol protocol, PeerState ... states)
	{
		synchronized(this.peers)
		{
			for (ConnectedPeer peer : this.peers)
			{
				if (peer.getProtocol().equals(protocol) == true &&
					peer.getURI().equals(host) == true &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(peer.getState())))
					return (T) peer;
			}

			return null;
		}
	}

/*	public boolean has(URI host)
	{
		synchronized(this.peers)
		{
			for (ConnectedPeer peer : this.peers)
			{
				if (peer.getURI().equals(host))
					return true;
			}

			return false;
		}
	}

	public boolean has(URI host, Protocol protocol)
	{
		synchronized(this.peers)
		{
			for (ConnectedPeer peer : this.peers)
			{
				if (peer.getProtocol().equals(protocol) == true &&
					peer.getURI().equals(host) == true)
					return true;
			}

			return false;
		}
	}*/

	@SuppressWarnings("unchecked")
	public <T extends ConnectedPeer> T get(ECPublicKey identity, Protocol protocol, PeerState ... states)
	{
		synchronized(this.peers)
		{
			for (ConnectedPeer peer : this.peers)
			{
				if (peer.getProtocol().equals(protocol) == true &&
					peer.getNode().getIdentity().equals(identity) &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(peer.getState())))
					return (T) peer;
			}
		}

		return null;
	}

	public boolean has(ECPublicKey identity, PeerState ... states)
	{
		synchronized(this.peers)
		{
			for (ConnectedPeer peer : this.peers)
			{
				if (peer.getNode().getIdentity().equals(identity) == true &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(peer.getState())))
					return true;
			}

			return false;
		}
	}

	public boolean has(ECPublicKey identity, Protocol protocol, PeerState ... states)
	{
		synchronized(this.peers)
		{
			for (ConnectedPeer peer : this.peers)
			{
				if (peer.getProtocol().equals(protocol) == true &&
					peer.getNode().getIdentity().equals(identity) == true &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(peer.getState())))
					return true;
			}

			return false;
		}
	}
	
	public boolean isWhitelisted(URI host)
	{
		return whitelist.accept(host);
	}

    // PEER LISTENER //
    private SynchronousEventListener peerListener = new SynchronousEventListener()
    {
    	@Subscribe
		public void on(PeerConnectingEvent event)
		{
			synchronized(Network.this.peers)
			{
				boolean add = true;
				// Want to check on reference not equality as we can have multiple instances
				// that may satisfy the equality check that we want to keep track of.
				for (ConnectedPeer peer : Network.this.peers)
				{
					if (peer == event.getPeer())
					{
						add = false;
						break;
					}
				}

				if (add)
					Network.this.peers.add(event.getPeer());
			}
		}

    	@Subscribe
		public void on(PeerConnectedEvent event)
		{
			synchronized(Network.this.peers)
			{
				// Check for multiple identities connecting on different hosts/host:port endpoints
				for (ConnectedPeer peer : Network.this.peers)
				{
					if (peer == event.getPeer())
						continue;
					
					if (peer.getNode().getIdentity().equals(event.getPeer().getNode().getIdentity()) == true && 
						peer.getProtocol().equals(event.getPeer().getProtocol()) == true)
					{
						event.getPeer().disconnect("Already connected at endpoint "+peer);
						break;
					}	
				}
			}
		}

    	@Subscribe
		public void on(PeerDisconnectedEvent event)
		{
			synchronized(Network.this.peers)
			{
				// Want to check on reference not equality as we can have multiple instances
				// that may satisfy the equality check that we want to keep track of.
				Iterator<ConnectedPeer> peersIterator = Network.this.peers.iterator();
				while (peersIterator.hasNext())
				{
					Peer peer = peersIterator.next();
					if (peer == event.getPeer())
					{
						peersIterator.remove();
						break;
					}
				}
			}
		}
    };
}
