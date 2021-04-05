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
import org.fuserleer.WebSocketService;
import org.fuserleer.common.Agent;
import org.fuserleer.common.Direction;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.executors.ScheduledExecutable;
import org.fuserleer.ledger.ShardMapper;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.discovery.OutboundShardDiscoveryFilter;
import org.fuserleer.network.discovery.OutboundSyncDiscoveryFilter;
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
import org.fuserleer.network.peers.filters.PeerFilter;
import org.fuserleer.network.peers.filters.StandardPeerFilter;
import org.fuserleer.node.Node;
import org.fuserleer.time.Time;

import com.google.common.eventbus.Subscribe;

public final class Network implements Service
{
	private static final Logger networkLog = Logging.getLogger("network");
	
	public static final int DEFAULT_PEER_INACTIVITY = 60; 
	public static final int DEFAULT_CONNECT_TIMEOUT = 60; 
	public static final int DEFAULT_TCP_CONNECTIONS_OUT_SYNC = 4; 
	public static final int DEFAULT_TCP_CONNECTIONS_OUT_SHARD = 1; 
//	public static final int DEFAULT_TCP_CONNECTIONS_IN = 4; 

	private final Context		context;
	private final Messaging 	messaging;
	private final PeerStore 	peerStore;
	private final PeerHandler	peerHandler;
	private final GossipHandler	gossipHandler;
	private final WebSocketService websocketService;

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
		    			networkLog.error(Network.this.context.getName()+": UDP datagram could not be queued");

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
								networkLog.error(Network.this.context.getName()+": "+datagramPacket.getSocketAddress().toString()+" did not send NodeMessage on connect");
								continue;
							}

							NodeMessage nodeMessage = (NodeMessage)message;
							URI uri = Agent.getURI(datagramPacket.getAddress().getHostAddress(), nodeMessage.getNode().getNetworkPort());
							
							Peer persistedPeer = Network.this.peerStore.get(nodeMessage.getNode().getIdentity().getECPublicKey());
							if (persistedPeer == null)
								persistedPeer = new Peer(uri, nodeMessage.getNode(), Protocol.UDP);
							
							connectedPeer = new RUDPPeer(Network.this.context, Network.this.datagramChannel, uri, Direction.INBOUND, persistedPeer);
							connectedPeer.connect();
						}
						
	    				Network.this.messaging.received(message, connectedPeer);
					}
					catch (Exception ex)
					{
						networkLog.error(Network.this.context.getName()+": UDP "+datagramPacket.getAddress().toString()+" error", ex);
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

    			URI uri = Agent.getURI(socket.getInetAddress().getHostAddress(), node.getNetworkPort());
				if (Network.this.has(node.getIdentity().getECPublicKey(), Protocol.TCP) == true)
				{
					networkLog.error(Network.this.context.getName()+": "+node.getIdentity()+" already has a socked assigned");
					return null;
				}

				Peer persistedPeer = Network.this.peerStore.get(node.getIdentity().getECPublicKey());
				ConnectedPeer connectedPeer = new TCPPeer(Network.this.context, socket, uri, Direction.INBOUND, persistedPeer);
				connectedPeer.connect();
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
							networkLog.error(Network.this.context.getName()+": "+socket.toString()+" did not send NodeMessage on connect");
							socket.close();
							continue;
						}
						
						NodeMessage nodeMessage = (NodeMessage)message;
						if (nodeMessage.verify(nodeMessage.getNode().getIdentity().getECPublicKey()) == false)
						{
							networkLog.error(Network.this.context.getName()+": "+socket.toString()+" NodeMessage failed verification");
							socket.close();
							continue;
						}
						
						URI uri = Agent.getURI(socket.getInetAddress().getHostAddress(), nodeMessage.getNode().getNetworkPort());
						
						// Store the node in the peer store even if can not connect due to whitelists or available connections as it may be a preferred peer
						Peer connectingPeer = new Peer(uri, nodeMessage.getNode(), Protocol.TCP);
						Network.this.context.getNetwork().getPeerStore().store(connectingPeer);
						
						if (Network.this.isWhitelisted(uri) == false)
					    {
							networkLog.debug(Network.this.context.getName()+": "+uri.getHost()+" is not whitelisted");
							socket.close();
							continue;
					    }
						
						// TODO omitted for TCP based shard connectivity, re-enable when connection less
/*						List<ConnectedPeer> connected = Network.this.get(Protocol.TCP, PeerState.CONNECTING, PeerState.CONNECTED).stream().filter(cp -> cp.getDirection().equals(Direction.INBOUND)).collect(Collectors.toList());
						if (connected.size() >= Network.this.context.getConfiguration().get("network.connections.in", 8))
						{
							networkLog.debug(Network.this.context.getName()+": "+socket.toString()+" all inbound slots occupied");
							socket.close();
							continue;
						}*/
						
						ConnectedPeer connectedPeer = connect(socket, nodeMessage.getNode());
						if (connectedPeer == null)
							socket.close();
						else
							Network.this.context.getNetwork().getMessaging().received(nodeMessage, connectedPeer);
					}
					catch (Exception ex)
					{
						networkLog.error(Network.this.context.getName()+": TCP "+socket.getInetAddress()+" error", ex);
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
	    					networkLog.error(Network.this.context.getName()+": TCPServerSocket died "+Network.this.TCPServerSocket);
		    				Network.this.TCPServerSocket = new ServerSocket(Network.this.TCPServerSocket.getLocalPort(), 16, Network.this.TCPServerSocket.getInetAddress());
		    				Network.this.TCPServerSocket.setReceiveBufferSize(65536);
		    				Network.this.TCPServerSocket.setSoTimeout(1000);
		    				networkLog.info(Network.this.context.getName()+": Recreated TCPServerSocket on "+Network.this.TCPServerSocket);
	    				}
	    				catch (Exception ex)
	    				{
	    					networkLog.fatal(Network.this.context.getName()+": TCPServerSocket death is fatal", ex);
	    				}
	    			}
	    		}
	    		catch (Exception ex)
	    		{
	    			networkLog.fatal(Network.this.context.getName()+": TCPServerSocket exception ", ex);
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
    	this.gossipHandler = new GossipHandler(context);
    	this.websocketService = new WebSocketService(context);
    	
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

    public GossipHandler getGossipHandler()
    {
    	return this.gossipHandler;
    }

    @Override
	public void start() throws StartupException
	{
    	try
    	{
    		this.datagramChannel = DatagramChannel.open(StandardProtocolFamily.INET);
    		this.datagramChannel.bind(new InetSocketAddress(InetAddress.getByName(this.context.getConfiguration().get("network.address", "0.0.0.0")),
    																			  this.context.getConfiguration().get("network.udp", Universe.getDefault().getPort())));
	    		
   			this.TCPServerSocket = new ServerSocket(this.context.getConfiguration().get("network.udp", Universe.getDefault().getPort()), 
			 	   	   								16,
			 	   	   								InetAddress.getByName(this.context.getConfiguration().get("network.address", "0.0.0.0")));

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
			this.gossipHandler.start();
			this.websocketService.start();
			
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
								if ((peer.getState().equals(PeerState.CONNECTING) == true && Time.getSystemTime() - peer.getConnectingAt() > TimeUnit.SECONDS.toMillis(Network.this.context.getConfiguration().get("network.peer.connect.timeout", DEFAULT_CONNECT_TIMEOUT))) ||
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
					
					// Discovery / Rotation //
					try
					{
						RemoteLedgerDiscovery discoverer = new RemoteLedgerDiscovery(Network.this.context);
						Collection<Peer> preferred = new HashSet<Peer>();

						// Sync //
						long syncShardGroup = ShardMapper.toShardGroup(Network.this.context.getNode().getIdentity(), Network.this.context.getLedger().numShardGroups());
						{
							OutboundSyncDiscoveryFilter outboundSyncDiscoveryFilter = new OutboundSyncDiscoveryFilter(Network.this.context, Collections.singleton(syncShardGroup));
							Collection<Peer> syncPreferred = discoverer.discover(outboundSyncDiscoveryFilter, Network.this.context.getConfiguration().get("network.connections.out.sync", Network.DEFAULT_TCP_CONNECTIONS_OUT_SYNC));
							syncPreferred.removeAll(Network.this.get(StandardPeerFilter.build(Network.this.context).setProtocol(Protocol.TCP).setStates(PeerState.CONNECTING, PeerState.CONNECTED).setDirection(Direction.INBOUND)));
	
							List<ConnectedPeer> syncConnected = Network.this.get(StandardPeerFilter.build(Network.this.context).setProtocol(Protocol.TCP).setStates(PeerState.CONNECTING, PeerState.CONNECTED).setDirection(Direction.OUTBOUND).setShardGroup(syncShardGroup));
							if (syncConnected.size() > Network.this.context.getConfiguration().get("network.connections.out.sync", Network.DEFAULT_TCP_CONNECTIONS_OUT_SYNC))
							{
								for (ConnectedPeer peer : syncConnected)
								{
									if (syncPreferred.contains(peer) == false)
									{
										peer.disconnect("Discovered better sync peer");
										break;
									}
								}
							}
							syncPreferred.removeAll(syncConnected);
							preferred.addAll(syncPreferred);
						}
						
						// Shard //
						// TODO temporary connectivity over TCP for shard groups ... eventually should be connectionless
						for (long sg = 0 ; sg < Network.this.context.getLedger().numShardGroups(Network.this.context.getLedger().getHead().getHeight()) ; sg++)
						{
							long shardGroup = sg;
							if (shardGroup == syncShardGroup)
								continue;

							OutboundShardDiscoveryFilter outboundShardDiscoveryFilter = new OutboundShardDiscoveryFilter(Network.this.context, Collections.singleton(shardGroup));
							Collection<Peer> shardPreferred = discoverer.discover(outboundShardDiscoveryFilter, Network.this.context.getConfiguration().get("network.connections.out.shard", Network.DEFAULT_TCP_CONNECTIONS_OUT_SHARD));
							shardPreferred.removeAll(Network.this.get(StandardPeerFilter.build(Network.this.context).setProtocol(Protocol.TCP).setStates(PeerState.CONNECTING, PeerState.CONNECTED).setDirection(Direction.INBOUND)));

							List<ConnectedPeer> shardConnected = Network.this.get(StandardPeerFilter.build(Network.this.context).setProtocol(Protocol.TCP).setStates(PeerState.CONNECTING, PeerState.CONNECTED).setDirection(Direction.OUTBOUND).setShardGroup(shardGroup));
							if (shardConnected.size() > Network.this.context.getConfiguration().get("network.connections.out.shard", Network.DEFAULT_TCP_CONNECTIONS_OUT_SHARD))
							{
								for (ConnectedPeer peer : shardConnected)
								{
									if (shardPreferred.contains(peer) == false)
									{
										peer.disconnect("Discovered better shard peer");
										break;
									}
								}
							}
							shardPreferred.removeAll(shardConnected);
							preferred.addAll(shardPreferred);
						}
						
						if (preferred.isEmpty() == false)
						{
							for (Peer preferredPeer : preferred)
							{
								if (Network.this.get(preferredPeer.getNode().getIdentity().getECPublicKey(), Protocol.TCP) != null)
									continue;
								
								Network.this.connecting.lock();
		
								try
								{
									Network.this.connect(preferredPeer.getURI(), Direction.OUTBOUND, Protocol.TCP);
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
						else if (networkLog.hasLevel(Logging.DEBUG) == true)
							networkLog.debug("Preferred outbound peers already connected");
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
			
			this.websocketService.stop();
			this.gossipHandler.stop();
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
				Peer persistedPeer = this.peerStore.get(uri);
				if (protocol.toString().equalsIgnoreCase(Protocol.UDP.toString()) == true)
		    	{
					connectedPeer = new RUDPPeer(this.context, this.datagramChannel, uri, direction, persistedPeer);
					connectedPeer.connect();
		    	}
		    	else if (protocol.toString().equalsIgnoreCase(Protocol.TCP.toString()) == true)
		    	{
		    		if (direction.equals(Direction.OUTBOUND) == false)
		    			throw new IllegalArgumentException("Can only specify OUTBOUND for TCP connections");
		    		
					Socket socket = new Socket();
					socket.setReceiveBufferSize(this.context.getConfiguration().get("network.tcp.buffer", 1<<18));
					socket.setSendBufferSize(this.context.getConfiguration().get("network.tcp.buffer", 1<<18));
					connectedPeer = new TCPPeer(this.context, socket, uri, Direction.OUTBOUND, persistedPeer);
					connectedPeer.connect();
		    	}
			}

			return (T) connectedPeer;
		}
		finally
		{
			this.connecting.unlock();
		}
    }
    
	public List<ConnectedPeer> get(PeerFilter<ConnectedPeer> filter)
	{
		synchronized(this.peers)
		{
			List<ConnectedPeer>	peers = new ArrayList<ConnectedPeer>();

			for (ConnectedPeer peer : this.peers)
			{
				try
				{
					if (filter.filter(peer) == true)
						peers.add(peer);
				}
				catch (IOException ex)
				{
					networkLog.error(Network.this.context.getName()+": Filter "+filter.getClass()+" for "+peer+" failed", ex);
				}
			}

			Collections.shuffle(peers);
			return peers;
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

	@SuppressWarnings("unchecked")
	private <T extends ConnectedPeer> T get(ECPublicKey identity, Protocol protocol, PeerState ... states)
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
					if (peer.equals(event.getPeer()) == true)
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
					if (peer.getURI().equals(event.getPeer().getURI()) == true)
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
					if (peer.equals(event.getPeer()) == true)
					{
						peersIterator.remove();
						break;
					}
				}
			}
		}
    };
}
