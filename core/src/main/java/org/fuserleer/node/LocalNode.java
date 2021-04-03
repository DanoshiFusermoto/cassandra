package org.fuserleer.node;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.API;
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
import org.java_websocket.WebSocketImpl;

import com.google.common.collect.ImmutableSet;

import net.consensys.mikuli.crypto.BLS12381;
import net.consensys.mikuli.crypto.BLSKeyPair;
import net.consensys.mikuli.crypto.BLSPublicKey;
import net.consensys.mikuli.crypto.BLSSignature;
import net.consensys.mikuli.crypto.SignatureAndPublicKey;

@SerializerId2("node")
public final class LocalNode extends Node implements Polymorphic
{
	private static final Logger log = Logging.getLogger ();

	public static final LocalNode load(String name, Configuration configuration, boolean create) 
	{
		try
		{
			ECKeyPair identityKey;
			BLSKeyPair BLSKey;
			SignatureAndPublicKey binding;
	
			File file = new File(configuration.get("node.key.path", name+".key"));
			if (file.exists() == false) 
			{
				if (create == false)
					throw new FileNotFoundException("Node " + file.toString() + " not found");
	
				File dir = file.getParentFile();
				if (dir != null && dir.exists() == false && dir.mkdirs() == false)
					throw new FileNotFoundException("Failed to create directory: " + dir.toString());
	
				try (FileOutputStream io = new FileOutputStream(file)) 
				{
					try 
					{
						Set<PosixFilePermission> perms = ImmutableSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
						Files.setPosixFilePermissions(file.toPath(), perms);
					} 
					catch (UnsupportedOperationException ignoredException) 
					{
						// probably windows
					}
	
					identityKey = new ECKeyPair();
					BLSKey = new BLSKeyPair();
					binding = BLS12381.sign(BLSKey, identityKey.getPublicKey().getBytes());
					
					DataOutputStream dos = new DataOutputStream(io);
					dos.writeInt(identityKey.getPrivateKey().length);
					dos.write(identityKey.getPrivateKey());
					
					dos.writeInt(BLSKey.privateKey().toByteArray().length);
					dos.write(BLSKey.privateKey().toByteArray());
					
					dos.writeInt(binding.publicKey().toByteArray().length);
					dos.write(binding.publicKey().toByteArray());
					dos.writeInt(binding.signature().toByteArray().length);
					dos.write(binding.signature().toByteArray());
					dos.flush();
				}
			} 
			else 
			{
				try (FileInputStream io = new FileInputStream(file)) 
				{
					DataInputStream dis = new DataInputStream(io);
					byte[] identityKeyPairBytes = new byte[dis.readInt()];
					dis.readFully(identityKeyPairBytes);
					identityKey = new ECKeyPair(identityKeyPairBytes);
					
					byte[] BLSKeyPairBytes = new byte[dis.readInt()];
					dis.readFully(BLSKeyPairBytes);
					BLSKey = new BLSKeyPair(BLSKeyPairBytes);
	
					byte[] bindingPubKeyBytes = new byte[dis.readInt()];
					dis.readFully(bindingPubKeyBytes);
					byte[] bindingSignatureBytes = new byte[dis.readInt()];
					dis.readFully(bindingSignatureBytes);
					binding = new SignatureAndPublicKey(BLSSignature.from(bindingSignatureBytes), BLSPublicKey.from(bindingPubKeyBytes));
					
					if (BLS12381.verify(binding, identityKey.getPublicKey().getBytes()) == false)
						throw new CryptoException("BLS binding verification failed");
				}
			}
			
			return new LocalNode(identityKey, BLSKey, binding,
								 configuration.get("network.port", Universe.getDefault().getPort()), 
								 configuration.get("api.port", API.DEFAULT_PORT),
								 configuration.get("websocket.port", WebSocketImpl.DEFAULT_PORT),
								 Universe.getDefault().getGenesis().getHeader(), 
								 Agent.AGENT, Agent.AGENT_VERSION, Agent.PROTOCOL_VERSION);
		}
		catch (CryptoException | IOException ex)
		{
			throw new IllegalStateException(ex);
		}
	}

	/**
	 * Write a private key to a file.
	 *
	 * @param file  The file to store the private key to.
	 * @param key   The key to store.
	 * 
	 * @throws IOException If writing the file fails
	 */
	public static final void toFile(File file, ECKeyPair key) throws IOException 
	{
		File dir = file.getParentFile();
		if (dir != null && dir.exists() == false && dir.mkdirs() == false)
			throw new FileNotFoundException("Failed to create directory: " + dir.toString());

		try (FileOutputStream io = new FileOutputStream(file)) 
		{
			try 
			{
				Set<PosixFilePermission> perms = ImmutableSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
				Files.setPosixFilePermissions(file.toPath(), perms);
			} 
			catch (UnsupportedOperationException ignoredException) 
			{
				// probably windows
			}

			io.write(key.getPrivateKey());
		}
	}

	
	private ECKeyPair identityKey;
	private BLSKeyPair BLSKey;
	
	public LocalNode(ECKeyPair identityKey, BLSKeyPair BLSKey, SignatureAndPublicKey binding, int networkPort, int apiPort, int websocketPort, BlockHeader block)
	{
		this(identityKey, BLSKey, binding, networkPort, apiPort, websocketPort, block, Agent.AGENT, Agent.AGENT_VERSION, Agent.PROTOCOL_VERSION);
	}

	public LocalNode(ECKeyPair identityKey, BLSKeyPair BLSKey, SignatureAndPublicKey binding, int networkPort, int apiPort, int websocketPort, BlockHeader block, String agent, int agentVersion, int protocolVersion)
	{
		super(Objects.requireNonNull(identityKey, "Key is null").getPublicKey(), binding, block, agent, agentVersion, protocolVersion, networkPort, websocketPort, apiPort, false);
		
		this.identityKey = identityKey;
		this.BLSKey = BLSKey;
	}

	public void fromPersisted(Node persisted)
	{
		Objects.requireNonNull(persisted, "Persisted local node is null");
		if (persisted.getIdentity().equals(this.identityKey.getPublicKey()) == false)
			throw new IllegalArgumentException("Persisted node identity key does not match "+this.identityKey.getPublicKey());
		
		if (persisted.getBinding().publicKey().equals(this.BLSKey.publicKey()) == false)
			throw new IllegalArgumentException("Persisted node BLS key does not match "+this.getBinding().publicKey());

		setBinding(persisted.getBinding());
		setHead(persisted.getHead());
		setNetworkPort(persisted.getNetworkPort());
		setAPIPort(persisted.getAPIPort());
		setWebsocketPort(persisted.getWebsocketPort());
		setSynced(false);
	}

	public ECKeyPair getIdentityKey() 
	{
		return this.identityKey;
	}

	public BLSKeyPair getBLSKey() 
	{
		return this.BLSKey;
	}
}
