package org.fuserleer.node;

import java.util.Map;
import java.util.Objects;

import org.fuserleer.BasicObject;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@SerializerId2("node")
public class Node extends BasicObject
{
	public static final int OOS_TRIGGER_LIMIT = 60; // TODO ~5 minutes of latency if block target is ~5 seconds.  Sufficient for alpha testing, but need smarter sync trigger.
	public static final int OOS_RESOLVED_LIMIT = 6; // TODO ~1 minutes of latency if block target is ~5 seconds.  Sufficient for alpha testing, but need smarter sync trigger.

	private String 	agent;

	private int  	agentVersion;
	
	private int  	protocolVersion;

	@JsonProperty("network_port")
	@DsonOutput(Output.ALL)
	private int 	networkPort;

	@JsonProperty("api_port")
	@DsonOutput(Output.ALL)
	private int	apiPort;

	@JsonProperty("websocket_port")
	@DsonOutput(Output.ALL)
	private int	websocketPort;

	@JsonProperty("head")
	@DsonOutput(Output.ALL)
	private BlockHeader	head;

	/**
	 * Whether the node is signalling it is synced with respect to its shard group
	 */
	@JsonProperty("synced")
	@DsonOutput(value = {Output.API, Output.WIRE})
	private boolean	synced;

	private ECPublicKey		identity;
	
	public Node()
	{
		super();

		this.agent = "unknown";
		this.agentVersion = 0;
		this.head = null;
		this.protocolVersion = 0;
		this.networkPort = 0;
		this.websocketPort = 0;
		this.apiPort = 0;
		this.identity = null;
		this.synced = false;
	}

	public Node(final Node node)
	{
		super();

		Objects.requireNonNull(node, "Node is null");
		
 		this.agent = node.getAgent();
 		this.agentVersion = node.getAgentVersion();
 		this.head = node.getHead();
		this.protocolVersion = node.getProtocolVersion();
		this.networkPort = node.getNetworkPort();
		this.apiPort = node.getWebsocketPort();
		this.websocketPort = node.getAPIPort();
		this.identity = node.getIdentity();
		this.synced = node.isSynced();
	}

	public Node(final ECPublicKey identity, final BlockHeader head, final String agent, final int agentVersion, final int protocolVersion, 
				final int networkPort, final int apiPort, final int websocketPort, final boolean synced)
	{
		this();

		this.identity = Objects.requireNonNull(identity, "Identity is null");
		this.agent = Objects.requireNonNull(agent, "Agent is null");
		this.head = Objects.requireNonNull(head, "BlockHeader is null");
		
		Numbers.isNegative(agentVersion, "Agent version is negative");
		Numbers.isNegative(protocolVersion, "Protocol version is negative");
		
		Numbers.inRange(networkPort, 1, 65535, "Network port is invalid");
		Numbers.inRange(apiPort, 1, 65535, "API port is invalid");
		Numbers.inRange(websocketPort, 1, 65535, "Websocket port is invalid");

		this.agentVersion = agentVersion;
		this.protocolVersion = protocolVersion;
		this.networkPort = networkPort;
		this.apiPort = apiPort;
		this.websocketPort = websocketPort;
		this.synced = synced;
	}

	public String getAgent()
	{
		return this.agent;
	}

	public int getAgentVersion()
	{
		return this.agentVersion;
	}

	public int getProtocolVersion()
	{
		return this.protocolVersion;
	}

	public int getNetworkPort()
	{
		return this.networkPort;
	}

	void setNetworkPort(final int port)
	{
		Numbers.inRange(port, 1, 65535, "Network port is invalid");
		this.networkPort = port;
	}

	public int getAPIPort()
	{
		return this.apiPort;
	}

	void setAPIPort(final int port)
	{
		Numbers.inRange(port, 1, 65535, "API port is invalid");
		this.apiPort = port;
	}

	public int getWebsocketPort()
	{
		return this.websocketPort;
	}

	void setWebsocketPort(final int port)
	{
		Numbers.inRange(port, 1, 65535, "Websocket port is invalid");
		this.websocketPort = port;
	}

	public BlockHeader getHead()
	{
		return this.head;
	}

	public void setHead(BlockHeader head)
	{
		Objects.requireNonNull(head, "Block header is null");
		
		this.head = head;
	}
	
	// SYNC //
	public final boolean isSynced()
	{
		return this.synced;
	}
	
	public final void setSynced(boolean synced)
	{
		this.synced = synced;
	}

	public final boolean isInSyncWith(Node other, int limit)
	{
		Objects.requireNonNull(other, "Other node is null");
		
		// Don't broadcast if not in sync with the remote node
		// TODO likely needs to be a lot more intelligent
		long thisHeight = getHead().getHeight();
		long otherHeight = other.getHead().getHeight();
		long heightDelta = Math.abs(thisHeight - otherHeight);
		if (heightDelta > limit)
			return false;
		
		return true;
	}

	public final boolean isAheadOf(Node other, int limit)
	{
		Objects.requireNonNull(other, "Other node is null");

		long thisHeight = getHead().getHeight();
		long otherHeight = other.getHead().getHeight();
		long heightDelta = thisHeight - otherHeight;
		if (heightDelta >= limit)
			return true;
		
		return false;
	}

	public ECPublicKey getIdentity()
	{
		return this.identity;
	}

	// Property "agent" - 1 getter, 1 setter
	// FIXME: Should be included in a serializable class
	@JsonProperty("agent")
	@DsonOutput(Output.ALL)
	Map<String, Object> getJsonAgent() 
	{
		return ImmutableMap.of(
				"name", this.agent,
				"version", this.agentVersion,
				"protocol", this.protocolVersion);
	}

	@JsonProperty("agent")
	void setJsonAgent(Map<String, Object> props) 
	{
		this.agent = (String) props.get("name");
		this.agentVersion = ((Number) props.get("version")).intValue();
		this.protocolVersion = ((Number) props.get("protocol")).intValue();
	}

	// Property "key" - 1 getter, 1 setter
	// FIXME: Should serialize ECKeyPair directly
	@JsonProperty("identity")
	@DsonOutput(Output.ALL)
	byte[] getJsonIdentity() 
	{
		return (this.identity == null) ? null : this.identity.getBytes();
	}

	@JsonProperty("identity")
	void setJsonIdentity(byte[] identity) throws SerializationException {
		try 
		{
			this.identity = ECPublicKey.from(identity);
		} 
		catch (CryptoException cex) 
		{
			throw new SerializationException("Invalid identity key", cex);
		}
	}

	@Override
	public String toString()
	{
		return this.getIdentity().toString()+"@"+this.head;
	}
}
