package org.fuserleer.node;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Objects;

import org.fuserleer.Configuration;
import org.fuserleer.Universe;
import org.fuserleer.common.Agent;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.ledger.BlockHeader;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.Polymorphic;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("node")
public final class LocalNode extends Node implements Polymorphic
{
	private static final Logger log = Logging.getLogger ();

	public static LocalNode create(String name, Configuration configuration)
	{
		ECKeyPair nodeKey = null;

		try
		{
			nodeKey = ECKeyPair.fromFile(new File(configuration.get("node.key.path", name+".key")), false);
		}
		catch (FileNotFoundException fnfex)
		{
			log.warn("node.key not found, generation required");
		}
		catch (CryptoException | IOException ex)
		{
			throw new IllegalStateException(ex);
		}
		
		if (nodeKey == null)
		{
			try
			{
				nodeKey = new ECKeyPair();
				ECKeyPair.toFile(new File(configuration.get("node.key.path", name+".key")), nodeKey);
			}
			catch (CryptoException | IOException ex)
			{
				throw new IllegalStateException(ex);
			}
		}
		
		return new LocalNode(nodeKey, configuration.get("network.port", Universe.getDefault().getPort()), Universe.getDefault().getGenesis().getHeader(), Agent.AGENT, Agent.AGENT_VERSION, Agent.PROTOCOL_VERSION);
	}
	
	private ECKeyPair key;
	
	public LocalNode(ECKeyPair key, int port, BlockHeader block)
	{
		this(key, port, block, Agent.AGENT, Agent.AGENT_VERSION, Agent.PROTOCOL_VERSION);
	}

	public LocalNode(ECKeyPair key, int port, BlockHeader block, String agent, int agentVersion, int protocolVersion)
	{
		super(Objects.requireNonNull(key, "Key is null").getPublicKey(), block, agent, agentVersion, protocolVersion, port);
		
		this.key = key;
	}

	public void fromPersisted(Node persisted)
	{
		Objects.requireNonNull(persisted, "Persisted local node is null");
		if (persisted.getIdentity().equals(this.key.getPublicKey()) == false)
			throw new IllegalArgumentException("Persisted node key does not match "+this.key);
		
		setHead(persisted.getHead());
		setPort(persisted.getPort());
	}

	public ECKeyPair getKey() 
	{
		return this.key;
	}
}
