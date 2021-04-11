package org.fuserleer.network.peers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.crypto.PublicKey;
import org.fuserleer.events.EventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executor;
import org.fuserleer.executors.ScheduledExecutable;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.discovery.BootstrapService;
import org.fuserleer.network.messages.GetPeersMessage;
import org.fuserleer.network.messages.NodeMessage;
import org.fuserleer.network.messages.PeerPingMessage;
import org.fuserleer.network.messages.PeerPongMessage;
import org.fuserleer.network.messages.PeersMessage;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.events.PeerAvailableEvent;
import org.fuserleer.network.peers.events.PeerConnectedEvent;
import org.fuserleer.network.peers.events.PeerDisconnectedEvent;
import org.fuserleer.network.peers.filters.AllPeersFilter;
import org.fuserleer.network.peers.filters.NotLocalPeersFilter;
import org.fuserleer.network.peers.filters.PeerFilter;
import org.fuserleer.network.peers.filters.StandardPeerFilter;
import org.fuserleer.time.Time;
import org.fuserleer.utils.UInt128;

import com.google.common.eventbus.Subscribe;

public class PeerHandler implements Service
{
//	private static final Logger log = Logging.getLogger();
	private static final Logger networklog = Logging.getLogger("network");

	public static final Comparator<Peer> SHUFFLER = new Comparator<Peer>()
	{
		Random random = new Random(System.currentTimeMillis());

		@Override
		public int compare(Peer p1, Peer p2)
		{
			int rn = random.nextInt();

			if (rn < 0)
				return -1;

			if (rn > 1)
				return 1;

			return 0;
		}
	};

	public static final class PeerDistanceComparator implements Comparator<Peer>
	{
		private final PublicKey origin;

		public PeerDistanceComparator(PublicKey origin)
		{
			this.origin = Objects.requireNonNull(origin);
		}

		@Override
		public int compare(Peer p1, Peer p2)
		{
			return compareXorDistances(p2.getNode().getIdentity(), p1.getNode().getIdentity());
		}
		
		public int compareXorDistances(PublicKey id1, PublicKey id2) 
		{
			UInt128 d1 = UInt128.from(this.origin.toByteArray()).xor(UInt128.from(id1.toByteArray()));
			UInt128 d2 = UInt128.from(this.origin.toByteArray()).xor(UInt128.from(id2.toByteArray()));

			int cmp = Integer.compare(d1.getLowestSetBit(), d2.getLowestSetBit());
			return (cmp == 0) ? d1.compareTo(d2) : cmp;
		}
	}

	private final Context	context;
	private BootstrapService bootstraper;
	private Map<Peer, Long> pings = new HashMap<Peer, Long>();
	
	private Future<?> houseKeepingTaskFuture = null;

	public PeerHandler(Context context)
	{
		super();
		
		this.context = Objects.requireNonNull(context);
	}

	@Override
	public void start() throws StartupException
	{
		try
		{
			this.context.getNetwork().getMessaging().register(PeersMessage.class, this.getClass(), new MessageProcessor<PeersMessage> ()
			{
				@Override
				public void process (PeersMessage peersMessage, ConnectedPeer peer)
				{
					NotLocalPeersFilter filter = new NotLocalPeersFilter(PeerHandler.this.context.getNode());
					for ( Peer p : peersMessage.getPeers())
					{
						if (p.getNode() == null)
							continue;
						try
						{
							if (filter.filter(p) == false)
								continue;

							PeerHandler.this.context.getNetwork().getPeerStore().store(p);
						}
						catch(IOException ioex)
						{
							networklog.error(PeerHandler.this.context.getName()+": Failed to store peer "+p, ioex);
						}
					}
				}
			});
	
			this.context.getNetwork().getMessaging().register(GetPeersMessage.class, this.getClass(), new MessageProcessor<GetPeersMessage> ()
			{
				@Override
				public void process(GetPeersMessage getPeersMessage, ConnectedPeer peer)
				{
					try
					{
						// Deliver known Peers in its entirety, filtered on whitelist and activity
						// Chunk the sending of Peers so that UDP can handle it
						// TODO make this better!
						List<Peer> peersList = PeerHandler.this.context.getNetwork().getPeerStore().get(new NotLocalPeersFilter(peer.getNode()));
						List<Peer> peersSublist = new ArrayList<Peer>();
						for (Peer p : peersList)
						{
							peersSublist.add(p);
	
							if (peersSublist.size() == 64)
							{
								PeerHandler.this.context.getNetwork().getMessaging().send(new PeersMessage(peersSublist), peer);
								peersSublist = new ArrayList<Peer>();
							}
						}
	
						if (peersSublist.isEmpty() == false)
							PeerHandler.this.context.getNetwork().getMessaging().send(new PeersMessage(peersSublist), peer);
					}
					catch (Exception ex)
					{
						networklog.error(PeerHandler.this.context.getName()+": peers.get "+peer, ex);
					}
				}
			});
	
			this.context.getNetwork().getMessaging().register(PeerPingMessage.class, this.getClass(), new MessageProcessor<PeerPingMessage> ()
			{
				@Override
				public void process (PeerPingMessage message, ConnectedPeer peer)
				{
					try
					{
						networklog.debug("peer.ping from "+peer+" with nonce '"+message.getNonce()+"'");
						peer.setActiveAt(Time.getSystemTime());
						PeerHandler.this.context.getNetwork().getMessaging().send(new PeerPongMessage(PeerHandler.this.context.getNode(), message.getNonce()), peer);
					}
					catch (Exception ex)
					{
						networklog.error(PeerHandler.this.context.getName()+": peer.ping "+peer, ex);
					}
				}
			});
	
			this.context.getNetwork().getMessaging().register(PeerPongMessage.class, this.getClass(), new MessageProcessor<PeerPongMessage> ()
			{
				@Override
				public void process (PeerPongMessage message, ConnectedPeer peer)
				{
					try
					{
						synchronized(PeerHandler.this.pings)
						{
							if (PeerHandler.this.pings.remove(peer) == message.getNonce())
							{
								networklog.debug("Got peer.pong from "+peer+" with nonce "+message.getNonce());
								PeerHandler.this.context.getEvents().post(new PeerAvailableEvent(peer));
							}
							else
								peer.disconnect("Got peer.pong with unexpected nonce "+message.getNonce());
						}
					}
					catch (Exception ex)
					{
						networklog.error(PeerHandler.this.context.getName()+": peer.pong "+peer, ex);
					}
				}
			});
			
			this.context.getNetwork().getMessaging().register(NodeMessage.class, this.getClass(), new MessageProcessor<NodeMessage>() 
			{
				@Override
				public void process(NodeMessage message, ConnectedPeer peer)
				{
					peer.setNode(message.getNode());
					peer.setActiveAt(Time.getSystemTime());
					try
					{
						PeerHandler.this.context.getNetwork().getPeerStore().store(peer);
					}
					catch (IOException e)
					{
						networklog.error(PeerHandler.this.context.getName()+": Failed to update peer on NodeMessage "+peer, e);
					}
				}
			});
	
	        // PEERS HOUSEKEEPING //
			this.houseKeepingTaskFuture = Executor.getInstance().scheduleWithFixedDelay(new ScheduledExecutable(60, this.context.getConfiguration().get("network.peers.broadcast.interval", 30), TimeUnit.SECONDS)
			{
				@Override
				public void execute()
				{
 					try
					{
						// Clean out aged peers with no activity
						for (Peer peer : PeerHandler.this.context.getNetwork().getPeerStore().get(new AllPeersFilter())) 
						{
							if (PeerHandler.this.context.getNetwork().has(peer.getNode().getIdentity(), PeerState.CONNECTING, PeerState.CONNECTED) == false && 
								(peer.getConnectingAt() > 0 && TimeUnit.MILLISECONDS.toSeconds(Time.getSystemTime() - peer.getConnectingAt()) >= PeerHandler.this.context.getConfiguration().get("network.peers.aged", 3600)) &&
								(peer.getActiveAt() > 0 && TimeUnit.MILLISECONDS.toSeconds(Time.getSystemTime() - peer.getActiveAt()) >= PeerHandler.this.context.getConfiguration().get("network.peers.aged", 3600)))
							{
								if (peer.getNode() == null) 
									PeerHandler.this.context.getNetwork().getPeerStore().delete(peer.getURI());
								else
									PeerHandler.this.context.getNetwork().getPeerStore().delete(peer.getNode().getIdentity());
							}
						}
	
						// Ping / pongs //
						for (ConnectedPeer peer : PeerHandler.this.context.getNetwork().get(StandardPeerFilter.build(PeerHandler.this.context).setStates(PeerState.CONNECTED)))
						{
							synchronized(PeerHandler.this.pings)
							{
								if (PeerHandler.this.pings.remove(peer) != null)
								{
									peer.disconnect("Did not respond to ping");
									continue;
								}
								
								PeerPingMessage ping = new PeerPingMessage(PeerHandler.this.context.getNode(), System.nanoTime()+peer.getURI().hashCode());
								PeerHandler.this.pings.put(peer, ping.getNonce());
								networklog.debug("Pinging "+peer+" with nonce '"+ping.getNonce()+"'");

								PeerHandler.this.context.getNetwork().getMessaging().send(ping, peer);
							}		
						}

						// Peer refresh
						for (ConnectedPeer connectedPeer : PeerHandler.this.context.getNetwork().get(StandardPeerFilter.build(PeerHandler.this.context).setStates(PeerState.CONNECTED)))
							PeerHandler.this.context.getNetwork().getMessaging().send(new GetPeersMessage(), connectedPeer);
					}
					catch (Throwable t)
					{
						networklog.error("Peers update failed", t);
					}
				}
			});
	
			this.context.getEvents().register(this.peerListener);
	
			// Clean out any existing non-whitelisted peers from the store (whitelist may have changed since last execution) //
			for (Peer peer : this.context.getNetwork().getPeerStore().get(new AllPeersFilter()))
			{
				if (this.context.getNetwork().isWhitelisted(peer.getURI()) == false)
				{
					networklog.debug("Deleting "+peer.getURI()+" as not whitelisted");
					this.context.getNetwork().getPeerStore().delete(peer.getURI());
				}
			}
			
			this.bootstraper = new BootstrapService(this.context);
			Thread bootstrapperThread = new Thread(this.bootstraper, this.context.getName()+" - Bootstrapper");
			bootstrapperThread.setDaemon(true);
			bootstrapperThread.start();
		}
		catch (Exception ex)
		{
			throw new StartupException(ex);
		}
	}

	@Override
	public void stop() throws TerminationException
	{
		if (this.houseKeepingTaskFuture != null)
			this.houseKeepingTaskFuture.cancel(false);

		this.context.getEvents().unregister(this.peerListener);
	}

	public Peer getPeer(PublicKey identity) throws IOException
	{
		return this.context.getNetwork().getPeerStore().get(identity);
	}

	public List<Peer> getPeers(PeerFilter<Peer> filter) throws IOException
	{
		return getPeers(filter, null);
	}

	public List<Peer> getPeers(PeerFilter<Peer> filter, Comparator<Peer> sorter) throws IOException
	{
		List<Peer> peers = new ArrayList<Peer>();
		peers = this.context.getNetwork().getPeerStore().get(filter);

		if (sorter != null)
			peers.sort(sorter);

		return Collections.unmodifiableList(peers);
	}

	public List<Peer> getPeers(Collection<PublicKey> identities) throws IOException
	{
		return getPeers(identities, null, null);
	}

	public List<Peer> getPeers(Collection<PublicKey> identities, PeerFilter<Peer> filter) throws IOException
	{
		return getPeers(identities, filter, null);
	}

	public List<Peer> getPeers(Collection<PublicKey> identities, PeerFilter<Peer> filter, Comparator<Peer> sorter) throws IOException
	{
		List<Peer> peers = new ArrayList<Peer>();

		for (PublicKey identity : identities)
		{
			Peer peer = this.context.getNetwork().getPeerStore().get(identity);

			if (peer == null)
				continue;
			peers.add(peer);
		}

		if (sorter != null)
			peers.sort(sorter);

		return Collections.unmodifiableList(peers);
	}

	// PEER LISTENER //
	private EventListener peerListener = new EventListener()
	{
    	@Subscribe
		public void on(PeerConnectedEvent event)
		{
			try
			{
				event.getPeer().setConnectedAt(Time.getSystemTime());
				PeerHandler.this.context.getNetwork().getPeerStore().store(event.getPeer());
				
				if (event.getPeer().getProtocol().equals(Protocol.TCP) == true) // TODO what if the UDP connection is the ONLY connection we have to the node?
					PeerHandler.this.context.getNetwork().getMessaging().send(new GetPeersMessage(), event.getPeer());
			}
			catch (IOException ioex)
			{
				networklog.debug("Failed to request known peer information from "+event.getPeer(), ioex);
			}
		}

    	@Subscribe
		public void on(PeerDisconnectedEvent event)
		{
    		try
    		{
				if (PeerHandler.this.context.getNetwork().isWhitelisted(event.getPeer().getURI()) == false)
				{
					networklog.debug("Store aborted, "+event.getPeer().getURI()+" is not whitelisted");
					return;
				}
	
				if (event.getPeer().getNode() != null && 
					PeerHandler.this.context.getNetwork().has(event.getPeer().getNode().getIdentity(), Protocol.UDP))
					return;
	
				PeerHandler.this.context.getNetwork().getPeerStore().store(event.getPeer());
			}
			catch (IOException ioex)
			{
				networklog.debug("Failed to store known peer information for "+event.getPeer(), ioex);
			}
		}
	};
}
