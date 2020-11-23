package org.fuserleer.network.peers;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.fuserleer.Context;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.database.DatabaseException;
import org.fuserleer.database.DatabaseStore;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.peers.filters.PeerFilter;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class PeerStore extends DatabaseStore
{
	private static final Logger networklog = Logging.getLogger("network");

	public static final int	MAX_CONNECTION_ATTEMPTS = 10;

	private Context				context;
	private Database 			peersDB;
	private SecondaryDatabase 	peerIdentityDB;

	private class PeerSecondaryKeyCreator implements SecondaryKeyCreator
	{
		@Override
		public boolean createSecondaryKey(SecondaryDatabase database, DatabaseEntry key, DatabaseEntry value, DatabaseEntry secondary)
		{
			if (database.getDatabaseName().equals("peer_identity") == true)
			{
				try
				{
					Peer peer = Serialization.getInstance().fromDson(value.getData(), Peer.class);
					if (peer.getNode() != null)
					{
						secondary.setData(peer.getNode().getIdentity().asHash().toByteArray());
						return true;
					}
	
					return false;
				}
				catch (Exception ex)
				{
					log.error("Identity key failed for Peer");
					return false;
				}
			}
			
			return false;
		}
	}

	public PeerStore(Context context) 
	{ 
		super(Objects.requireNonNull(context).getDatabaseEnvironment());
		
		this.context = context;
	}

	@Override
	public void start() throws StartupException
	{
		try
		{
			if (this.context.getConfiguration().getCommandLine().hasOption("clean") == true)
				clean();

			DatabaseConfig config = new DatabaseConfig();
			config.setAllowCreate(true);
			this.peersDB = getEnvironment().openDatabase(null, "peers", config);

			SecondaryConfig identityConfig = new SecondaryConfig();
			identityConfig.setAllowCreate(true);
			identityConfig.setKeyCreator(new PeerSecondaryKeyCreator());
			identityConfig.setSortedDuplicates(false);
			this.peerIdentityDB = getEnvironment().openSecondaryDatabase(null, "peer_identity", this.peersDB, identityConfig);

			try (Cursor cursor = this.peersDB.openCursor(null, null)) 
			{
				DatabaseEntry key = new DatabaseEntry();
				DatabaseEntry value = new DatabaseEntry();

				while (cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
				{
					Peer peer = Serialization.getInstance().fromDson(value.getData(), Peer.class);
					peer.setActiveAt(0l);
					peer.setConnectedAt(0l);
					byte[] bytes = Serialization.getInstance().toDson(peer, Output.PERSIST);
					this.peersDB.put(null, key, new DatabaseEntry(bytes));
				}
			}
			catch (Exception ex)
			{
				throw ex;
			}
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void stop() throws TerminationException
	{
		try
		{
			close();
		}
		catch (IOException ioex)
		{
			throw new TerminationException(ioex);
		}
	}

	@Override
	public void clean() throws DatabaseException
	{
		Transaction transaction = null;

		try
		{
			transaction = getEnvironment().beginTransaction(null, new TransactionConfig().setReadUncommitted(true));
			getEnvironment().truncateDatabase(transaction, "peers", false);
			getEnvironment().truncateDatabase(transaction, "peer_identity", false);
			transaction.commit();
		}
		catch (DatabaseNotFoundException dsnfex)
		{
			if (transaction != null)
				transaction.abort();

			log.warn(dsnfex.getMessage());
		}
		catch (Exception ex)
		{
			if (transaction != null)
				transaction.abort();

			throw new DatabaseException(ex);
		}
	}

	@Override
	public void close() throws IOException
	{
		super.close();

		if (this.peerIdentityDB != null) this.peerIdentityDB.close();
		if (this.peersDB != null) this.peersDB.close();
	}

	@Override
	public void flush() throws DatabaseException  { /* Not used */ }

	public boolean delete(URI host) throws IOException
	{
		Objects.requireNonNull(host, "Host URI is null");
		
		try
        {
			DatabaseEntry key = new DatabaseEntry(host.toString().toLowerCase().getBytes(StandardCharsets.UTF_8));
			OperationStatus status = this.peersDB.delete(null, key);
			if (status == OperationStatus.SUCCESS)
			{
				networklog.debug(this.context.getName()+": Deleted peer "+host);
				return true;
			}

			return false;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}

	public boolean delete(ECPublicKey identity) throws IOException
	{
		Objects.requireNonNull(identity, "Identity is null");

		try
        {
			DatabaseEntry key = new DatabaseEntry(identity.asHash().toByteArray());
			OperationStatus status = this.peerIdentityDB.delete(null, key);
			if (status == OperationStatus.SUCCESS)
			{
				networklog.debug(this.context.getName()+": Deleted peer "+identity);
				return true;
			}
			
			return false;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}

	public boolean store(Peer peer) throws IOException
	{
		Objects.requireNonNull(peer, "Peer is null");
		Objects.requireNonNull(peer.getNode(), "Peer node is null");
		
		try
        {
			// Ensure we only allow ONE instance of an identity //
			Peer existingPeer = get(peer.getNode().getIdentity());
			if (existingPeer != null && existingPeer.getURI().equals(peer.getURI()) == false)
			{
				DatabaseEntry key = new DatabaseEntry(peer.getNode().getIdentity().asHash().toByteArray());
				if (this.peerIdentityDB.delete(null, key) == OperationStatus.SUCCESS)
					networklog.debug("Removed "+existingPeer+" associated with "+peer.getNode().getIdentity());
				else
					throw new DatabaseException("Peer "+peer+" storage failed");
			}

			// merge argument peer with existing
			if (existingPeer != null)
				peer = peer.merge(existingPeer);

			DatabaseEntry key = new DatabaseEntry(peer.getURI().toString().toLowerCase().getBytes(StandardCharsets.UTF_8));
			byte[] bytes = Serialization.getInstance().toDson(peer, Output.PERSIST);
			DatabaseEntry value = new DatabaseEntry(bytes);

			if (this.peersDB.put(null, key, value) == OperationStatus.SUCCESS)
			{
				networklog.debug(this.context+": Updated "+peer);
				return true;
			}
			else
			{
				networklog.debug(this.context.getName()+": Failed to store "+peer);
				return false;
			}
		}
		catch (DatabaseException dbex)
		{
			throw dbex;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}

	public boolean has(URI host) throws IOException
	{
		Objects.requireNonNull(host, "Host URI is null");

		try
        {
			DatabaseEntry key = new DatabaseEntry(host.toString().toLowerCase().getBytes(StandardCharsets.UTF_8));

		    if (this.peersDB.get(null, key, null, LockMode.DEFAULT) == OperationStatus.SUCCESS)
		    	return true;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}

		return false;
	}

	public Peer get(URI host) throws IOException
	{
		Objects.requireNonNull(host, "Host URI is null");

		try (Cursor cursor = this.peersDB.openCursor(null, null)) 
		{
			DatabaseEntry key = new DatabaseEntry(host.toString().toLowerCase().getBytes(StandardCharsets.UTF_8));
		    DatabaseEntry value = new DatabaseEntry();

		    while (cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
		    {
		    	Peer peer = Serialization.getInstance().fromDson(value.getData(), Peer.class);

		    	if (peer.getURI().getHost().equalsIgnoreCase(host.getHost()) &&
		    		peer.getURI().getPort() == host.getPort())
		    		return peer;
		    }
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}

		return null;
	}

	public List<Peer> get(int index, int limit, PeerFilter filter) throws IOException
	{
		if (limit < 1)
			throw new IllegalArgumentException("Limit can not be less than 1");

		if (index < 0)
			throw new IllegalArgumentException("Index can not be less than 0");
		
		Objects.requireNonNull(filter, "PeerFilter is null");

		List<Peer>	peers = new ArrayList<>();
		try (Cursor cursor = this.peersDB.openCursor(null, null)) 
		{
			OperationStatus status = OperationStatus.SUCCESS;
		    DatabaseEntry key = new DatabaseEntry();
		    DatabaseEntry data = new DatabaseEntry();

	    	status = cursor.getFirst(key, data, LockMode.DEFAULT);

	    	if (index > 0)
	    	{
	    		long skipped = cursor.skipNext(index, key, data, LockMode.DEFAULT);

	    		if (skipped != index)
	    			status = OperationStatus.NOTFOUND;
	    	}

		    while (status == OperationStatus.SUCCESS)
		    {
		    	Peer peer = Serialization.getInstance().fromDson(data.getData(), Peer.class);

		    	if (filter.filter(peer) == false)
		    		peers.add(peer);

		    	if (peers.size() == limit)
		    		break;

		    	status = cursor.getNext(key, data, LockMode.DEFAULT);
		    }
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}

		return peers;
	}

	public List<Peer> get(PeerFilter filter) throws IOException
	{
		Objects.requireNonNull(filter, "PeerFilter is null");
		
		List<Peer>	peers = new ArrayList<>();
		try (Cursor cursor = peersDB.openCursor(null, null)) 
		{
		    DatabaseEntry key = new DatabaseEntry();
		    DatabaseEntry value = new DatabaseEntry();

		    while (cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
		    {
		    	Peer peer = Serialization.getInstance().fromDson(value.getData(), Peer.class);
		    	if (filter.filter(peer) == false)
		    		peers.add(peer);
		    }
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}

		return peers;
	}

	public boolean has(ECPublicKey identity) throws IOException
	{
		Objects.requireNonNull(identity, "Identity is null");

		try
        {
			DatabaseEntry search = new DatabaseEntry(identity.asHash().toByteArray());
			DatabaseEntry key = new DatabaseEntry();

			if (this.peerIdentityDB.get(null, search, key, null, LockMode.DEFAULT) == OperationStatus.SUCCESS)
		    	return true;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}

		return false;
	}

	public Peer get(ECPublicKey identity) throws IOException
	{
		Objects.requireNonNull(identity, "Identity is null");
		
		try
        {
			DatabaseEntry search = new DatabaseEntry(identity.asHash().toByteArray());
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry value = new DatabaseEntry();

			if (this.peerIdentityDB.get(null, search, key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
				return Serialization.getInstance().fromDson(value.getData(), Peer.class);
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}

		return null;
	}
}

