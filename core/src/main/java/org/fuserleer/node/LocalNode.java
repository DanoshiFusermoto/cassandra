package org.fuserleer.node;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.fuserleer.API;
import org.fuserleer.Configuration;
import org.fuserleer.Universe;
import org.fuserleer.common.Agent;
import org.fuserleer.crypto.BLSKeyPair;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.Polymorphic;
import org.fuserleer.serialization.SerializerId2;
import org.java_websocket.WebSocketImpl;

@SerializerId2("node")
public final class LocalNode extends Node implements Polymorphic
{
	private static final Logger log = Logging.getLogger ();

	public static final LocalNode load(String name, Configuration configuration, boolean create) throws CryptoException 
	{
		try
		{
			BLSKeyPair BLSKey = BLSKeyPair.fromFile(new File(configuration.get("node.key.path", name+".key")), create);

			return new LocalNode(BLSKey,
								 configuration.get("network.port", Universe.getDefault().getPort()), 
								 configuration.get("api.port", API.DEFAULT_PORT),
								 configuration.get("websocket.port", WebSocketImpl.DEFAULT_PORT),
								 Universe.getDefault().getGenesis().getHeader(), 
								 Agent.AGENT, Agent.AGENT_VERSION, Agent.PROTOCOL_VERSION);
		}
		catch (IOException ex)
		{
			throw new IllegalStateException(ex);
		}
	}

	private BLSKeyPair BLSKey;
	
	public LocalNode(BLSKeyPair BLSKey, int networkPort, int apiPort, int websocketPort, BlockHeader block)
	{
		this(BLSKey, networkPort, apiPort, websocketPort, block, Agent.AGENT, Agent.AGENT_VERSION, Agent.PROTOCOL_VERSION);
	}

	public LocalNode(BLSKeyPair BLSKey, int networkPort, int apiPort, int websocketPort, BlockHeader block, String agent, int agentVersion, int protocolVersion)
	{
		super(Objects.requireNonNull(BLSKey, "Key is null").getPublicKey(), block, agent, agentVersion, protocolVersion, networkPort, websocketPort, apiPort, false);
		
		this.BLSKey = BLSKey;
	}

	public void fromPersisted(Node persisted)
	{
		Objects.requireNonNull(persisted, "Persisted local node is null");
		if (persisted.getIdentity().equals(this.BLSKey.getPublicKey()) == false)
			throw new IllegalArgumentException("Persisted node BLS key does not match "+this.BLSKey.getPublicKey());

		setHead(persisted.getHead());
		setNetworkPort(persisted.getNetworkPort());
		setAPIPort(persisted.getAPIPort());
		setWebsocketPort(persisted.getWebsocketPort());
		setSynced(false);
	}

	public BLSKeyPair getKeyPair() 
	{
		return this.BLSKey;
	}
}
