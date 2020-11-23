package org.fuserleer.node;

import java.util.Map;
import java.util.Objects;

import org.fuserleer.BasicObject;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.Universe;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

@SerializerId2("node")
public class Node extends BasicObject
{
	private String 	agent;

	private int  	agentVersion;
	
	private int  	protocolVersion;

	@JsonProperty("port")
	@DsonOutput(Output.ALL)
	private int 	port;

	@JsonProperty("head")
	@DsonOutput(Output.ALL)
	private BlockHeader	head;

	private ECPublicKey		identity;
	
	public Node()
	{
		super();

		this.agent = "unknown";
		this.agentVersion = 0;
		this.head = null;
		this.protocolVersion = 0;
		this.port = 0;
		this.identity = null;
	}

	public Node(Node node)
	{
		super();

		Objects.requireNonNull(node, "Node is null");
		
 		this.agent = node.getAgent();
 		this.agentVersion = node.getAgentVersion();
 		this.head = node.getHead();
		this.protocolVersion = node.getProtocolVersion();
		this.port = node.getPort();
		this.identity = node.getIdentity();
	}

	public Node(ECPublicKey identity, BlockHeader head, String agent, int agentVersion, int protocolVersion, int port)
	{
		this();

		this.identity = Objects.requireNonNull(identity, "Identity is null");
		this.agent = Objects.requireNonNull(agent, "Agent is null");
		this.head = Objects.requireNonNull(head, "BlockHeader is null");
		
		if (agentVersion < 0)
			throw new IllegalArgumentException("Agent version is negative");
		
		if (protocolVersion < 0)
			throw new IllegalArgumentException("Protocol version is negative");
		
		if (port <= 0 || port > 65535)
			throw new IllegalArgumentException("Port is invalid");
		
		this.agentVersion = agentVersion;
		this.protocolVersion = protocolVersion;
		this.port = port;
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

	public int getPort()
	{
		return port;
	}

	void setPort(int port)
	{
		if (port <= 0 || port > 65535)
			throw new IllegalArgumentException("Port is invalid");

		this.port = port;
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
	public final boolean isInSyncWith(Node other)
	{
		Objects.requireNonNull(other, "Other node is null");
		
		// Don't broadcast if not in sync with the remote node
		// TODO likely needs to be a lot more intelligent
		long thisHeight = getHead().getHeight();
		long otherHeight = other.getHead().getHeight();
		long heightDelta = Math.abs(thisHeight - otherHeight);
		if (heightDelta > Universe.getDefault().getEpoch())
			return false;
		
		return true;
	}

	public final boolean isAheadOf(Node other)
	{
		Objects.requireNonNull(other, "Other node is null");

		long thisHeight = getHead().getHeight();
		long otherHeight = other.getHead().getHeight();
		long heightDelta = thisHeight - otherHeight;
		if (heightDelta >= Universe.getDefault().getEpoch())
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
