package org.fuserleer.node;

import java.util.Map;

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
	private BlockHeader	block;

	private ECPublicKey		identity;
	
	public Node()
	{
		super();

		this.agent = "unknown";
		this.agentVersion = 0;
		this.block = null;
		this.protocolVersion = 0;
		this.port = 0;
		this.identity = null;
	}

	public Node(Node node)
	{
		super();

 		this.agent = node.getAgent();
 		this.agentVersion = node.getAgentVersion();
 		this.block = node.getBlock();
		this.protocolVersion = node.getProtocolVersion();
		this.port = node.getPort();
		this.identity = node.getIdentity();
	}

	public Node(ECPublicKey identity, BlockHeader block, String agent, int agentVersion, int protocolVersion, int port)
	{
		this();

		this.identity = identity;
		this.agent = agent;
		this.block = block;
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
		this.port = port;
	}

	public BlockHeader getBlock()
	{
		return this.block;
	}

	public void setBlock(BlockHeader block)
	{
		this.block = block;
	}
	
	// SYNC //
	public final boolean isInSyncWith(Node other)
	{
		// Don't broadcast if not in sync with the remote node
		// TODO likely needs to be a lot more intelligent
		long thisHeight = getBlock().getHeight();
		long otherHeight = other.getBlock().getHeight();
		long heightDelta = Math.abs(thisHeight - otherHeight);
		if (heightDelta > Universe.getDefault().getEpoch())
			return false;
		
		return true;
	}

	public final boolean isAheadOf(Node other)
	{
		long thisHeight = getBlock().getHeight();
		long otherHeight = other.getBlock().getHeight();
		long heightDelta = thisHeight - otherHeight;
		if (heightDelta >= Universe.getDefault().getEpoch())
			return true;
		
		return false;
	}

	public ECPublicKey getIdentity()
	{
		return this.identity;
	}

	void setIdentity(ECPublicKey identity)
	{
		this.identity = identity;
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
		return this.getIdentity().toString()+"@"+this.block;
	}
}
