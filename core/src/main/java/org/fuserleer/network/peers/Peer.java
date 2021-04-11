package org.fuserleer.network.peers;

import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Agent;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.node.Node;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.time.Time;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;

@SerializerId2("network.peer")
public class Peer extends BasicObject
{
	private static final Logger networkLog = Logging.getLogger("network");

	private URI	host = null;

	@JsonProperty("protocols")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=LinkedHashSet.class)
	private Set<Protocol> protocols;

	@JsonProperty("node")
	@DsonOutput(Output.ALL)
	private Node node = null;
	
	@JsonProperty("attempts")
	@DsonOutput(Output.PERSIST)
	private int attempts;

	@JsonProperty("attempt_at")
	@DsonOutput(Output.PERSIST)
	private long attemptAt;

	@JsonProperty("attempted_at")
	@DsonOutput(Output.PERSIST)
	private long attemptedAt;

	@JsonProperty("active_at")
	@DsonOutput(Output.PERSIST)
	private long activeAt;

	@JsonProperty("connected_at")
	@DsonOutput(Output.PERSIST)
	private long connectedAt;
	
	@JsonProperty("connecting_at")
	@DsonOutput(Output.PERSIST)
	private long connectingAt;

	@JsonProperty("disconnected_at")
	@DsonOutput(Output.PERSIST)
	private long disconnectedAt;

	@JsonProperty("banned_until")
	@DsonOutput(Output.PERSIST)
	private long bannedUntil;

	private String 		banReason = null;

	private transient long 	trafficIn = 0;
	private transient long 	trafficOut = 0;
	private transient int 	latency = 0;
	private transient Integer hashcode = 0;
	
	private Peer()
	{
		super();
	}
	
	private Peer(URI host)
	{
		this();
		
		this.host = Objects.requireNonNull(host);
	}

	public Peer(URI host, Node node, Protocol ... protocols)
	{
		this(host);
		
		this.node = Objects.requireNonNull(node);
		
		this.protocols = new LinkedHashSet<Protocol>();
		for (Protocol protocol : protocols)
			this.protocols.add(protocol);
	}

	protected Peer(URI host, Peer peer) 
	{
		this(host);
	
		if (peer != null) 
		{
			this.node = peer.node == null ? null : new Node(peer.node);
			this.activeAt = peer.activeAt;
			this.attempts = peer.attempts;
			this.attemptAt = peer.attemptAt;
			this.attemptedAt = peer.attemptedAt;
			this.connectingAt = peer.connectingAt;
			this.connectedAt = peer.connectedAt;
			this.disconnectedAt = peer.disconnectedAt;
			this.trafficIn = peer.trafficIn;
			this.trafficOut = peer.trafficOut;
			this.bannedUntil = peer.bannedUntil;
			this.banReason = peer.banReason;
			this.protocols = peer.protocols == null ? null : new LinkedHashSet<>(peer.protocols);
		}
	}

	@Override
	public final boolean equals(Object object)
	{
		if (object == null) 
			return false;
		
		if (object == this) 
			return true;

		if (object instanceof Peer)
		{
			if (((Peer)object).getURI().equals(getURI()) == false)
				return false;

			if (((Peer)object).getNode().equals(getNode()) == false)
				return false;
			
			return true;
		}

		return false;
	}

	@Override
	public final int hashCode()
	{
		if (this.hashcode == null)
			this.hashcode = Objects.hash(getURI().toString().toLowerCase(), getNode().getIdentity());
		
		return this.hashcode;
	}

	@Override
	public String toString()
	{
		return this.host.toString()+" "+(this.node == null ? "unknown" : this.node.toString())+" "+ (this.banReason == null ? "" : (" BANNED "+this.bannedUntil));
	}

	public URI getURI()
	{
		return this.host;
	}

	private void setURI(URI host)
	{
		this.host = host;
	}

	public String getBanReason()
	{
		return this.banReason;
	}

	public void setBanReason(String banReason)
	{
		this.banReason = banReason;
	}

	public boolean hasProtocol()
	{
		return this.protocols.isEmpty() == false;
	}

	public boolean hasProtocol(Protocol protocol)
	{
		return this.protocols.contains(protocol);
	}

	public void addProtocol(Protocol protocol)
	{
		this.protocols.add(protocol);
	}

	public Node getNode()
	{
		return this.node;
	}

	public void setNode(Node node)
	{
		this.node = node;
		setURI(Agent.getURI(getURI().getHost(), node.getNetworkPort()));
	}

	public boolean isBanned()
	{
		return getBannedUntil() > Time.getSystemTime();
	}
	
	public long getBannedUntil()
	{
		return this.bannedUntil;
	}
	
	void setBannedUntil(long bannedUntil)
	{
		this.bannedUntil = bannedUntil;
	}

	public long getActiveAt()
	{
		return this.activeAt;
	}

	void setActiveAt(long timestamp)
	{
		this.activeAt = timestamp;
	}

	public int getAttempts()
	{
		return this.attempts;
	}

	void setAttempts(int attempts)
	{
		this.attempts = attempts;
	}

	public long getAttemptAt()
	{
		return this.attemptAt;
	}

	void setAttemptAt(long timestamp)
	{
		this.attemptAt = timestamp;
	}

	public long getAttemptedAt()
	{
		return this.attemptedAt;
	}

	void setAttemptedAt(long timestamp)
	{
		this.attemptedAt = timestamp;
	}

	public long getConnectedAt()
	{
		return this.connectedAt;
	}

	void setConnectedAt(long timestamp)
	{
		this.connectedAt = timestamp;
	}
	
	public long getConnectingAt()
	{
		return this.connectingAt;
	}

	void setConnectingAt(long timestamp)
	{
		this.connectingAt = timestamp;
	}

	public long getDisconnectedAt()
	{
		return this.disconnectedAt;
	}

	void setDisconnectedAt(long timestamp)
	{
		this.disconnectedAt = timestamp;
	}

	// Property "host" - 1 getter, 1 setter
	// Could potentially just serialize the URI as a string
	@JsonProperty("host")
	@DsonOutput(Output.ALL)
	private Map<String, Object> serializeHost() 
	{
		return ImmutableMap.of("ip", this.host.getHost().toLowerCase(), "port", this.host.getPort());
	}

	@JsonProperty("host")
	private void deserializeHost(Map<String, Object> props) 
	{
		String hostip = (String) props.get("ip");
		Integer port = ((Number) props.get("port")).intValue();

		this.host = Agent.getURI(hostip, port);
	}

	// Property "ban_reason" - 1 getter, 1 setter
	// FIXME: Should just serialize if non-null
	@JsonProperty("ban_reason")
	@DsonOutput(Output.PERSIST)
	private String serializeBanReason() 
	{
		return (getBannedUntil() > 0) ? banReason : null;
	}

	@JsonProperty("ban_reason")
	private void deserializeBanReason(String banReason) 
	{
		this.banReason = banReason;
	}

	Peer merge(Peer other)
	{
		Objects.requireNonNull(other, "Peer to merge is null");
		if (this.node.getIdentity().equals(other.getNode().getIdentity()) == false)
			throw new IllegalStateException("Peers to merge do not have same identities "+this.getNode().getIdentity()+" / "+other.getNode().getIdentity());
		
		Peer peer = new Peer();
		peer.host = this.host;
		
		if (other.getNode().getHead().getHeight() > this.getNode().getHead().getHeight())
			peer.node = other.node;
		else
			peer.node = this.node;
		
		peer.activeAt = other.activeAt > this.activeAt ? other.activeAt : this.activeAt;

		peer.attemptAt = other.attemptAt > this.attemptAt ? other.attemptAt : this.attemptAt;
		peer.attempts = other.attempts > this.attempts ? other.attempts : this.attempts;
		
		if (other.bannedUntil > this.bannedUntil)
		{
			peer.bannedUntil = other.bannedUntil;
			peer.banReason = other.banReason;
		}
		else
		{
			peer.bannedUntil = this.bannedUntil;
			peer.banReason = this.banReason;
		}
			
		if (other.connectedAt > this.connectedAt)
		{
			peer.connectedAt = other.connectedAt;
			peer.disconnectedAt = other.disconnectedAt;
		}
		else
		{
			peer.connectedAt = this.connectedAt;
			peer.disconnectedAt = this.disconnectedAt;
		}
		
		peer.latency = other.latency > this.latency ? other.latency : this.latency;
		peer.trafficIn = other.trafficIn > this.trafficIn ? other.trafficIn : this.trafficIn;
		peer.trafficOut = other.trafficOut > this.trafficOut ? other.trafficOut : this.trafficOut;
		peer.protocols = new LinkedHashSet<Protocol>(this.protocols);
		return peer;
	}
}
