package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.bouncycastle.util.Arrays;
import org.fuserleer.Context;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.database.DatabaseException;
import org.fuserleer.database.DatabaseStore;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.Longs;

import com.google.common.primitives.Bytes;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

class VotePowerStore extends DatabaseStore
{
	private static final Logger powerLog = Logging.getLogger("power");

	private Context				context;
	private Database 			votePowerDB;
	private SecondaryDatabase 	voteIdentityDB;

	private class IdentitySecondaryKeyCreator implements SecondaryKeyCreator
	{
		@Override
		public boolean createSecondaryKey(SecondaryDatabase database, DatabaseEntry key, DatabaseEntry value, DatabaseEntry secondary)
		{
			if (database.getDatabaseName().equals("vote_identity") == true)
			{
				// Get identity from value
				secondary.setData(Arrays.copyOfRange(value.getData(), Long.BYTES, value.getSize()));
				return true;
			}
			
			return false;
		}
	}

	public VotePowerStore(Context context) 
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
			config.setTransactional(true);
			this.votePowerDB = getEnvironment().openDatabase(null, "vote_powers", config);

			SecondaryConfig identityConfig = new SecondaryConfig();
			identityConfig.setAllowCreate(true);
			identityConfig.setKeyCreator(new IdentitySecondaryKeyCreator());
			identityConfig.setSortedDuplicates(true);
			identityConfig.setTransactional(true);
			this.voteIdentityDB = getEnvironment().openSecondaryDatabase(null, "vote_identity", this.votePowerDB, identityConfig);
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
			getEnvironment().truncateDatabase(transaction, "vote_powers", false);
			getEnvironment().truncateDatabase(transaction, "vote_identity", false);
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

		if (this.voteIdentityDB != null) this.voteIdentityDB.close();
		if (this.votePowerDB != null) this.votePowerDB.close();
	}

	@Override
	public void flush() throws DatabaseException  { /* Not used */ }

	public Map<Long, Long> get(final ECPublicKey identity) throws IOException
	{
		Objects.requireNonNull(identity, "Identity is null");

		Map<Long, Long> powers = new HashMap<Long, Long>();
		DatabaseEntry search = new DatabaseEntry(identity.getBytes());
		DatabaseEntry key = new DatabaseEntry();
	    DatabaseEntry value = new DatabaseEntry();
	    
		try (SecondaryCursor cursor = this.voteIdentityDB.openCursor(null, null)) 
		{
			OperationStatus status = cursor.getSearchKey(search, key, value, LockMode.DEFAULT);
			while (status.equals(OperationStatus.SUCCESS) == true)
			{
				long height = Longs.fromByteArray(key.getData(), Long.BYTES);
				long power = Longs.fromByteArray(value.getData());
				powers.put(height, power);
				status = cursor.getNextDup(search, key, value, LockMode.DEFAULT);
			}
			
			return powers;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}

	public OperationStatus store(final ECPublicKey identity, final Map<Long, Long> powers) throws IOException
	{
		Objects.requireNonNull(identity, "Identity is null");
		Objects.requireNonNull(powers, "Powers is null");
		
		if (powers.isEmpty() == true)
			return OperationStatus.KEYEMPTY;
		
		Transaction transaction = this.votePowerDB.getEnvironment().beginTransaction(null, null);
		try
        {
			List<Long> heights = new ArrayList<Long>(powers.keySet());
			Collections.sort(heights);
			
			byte[] identityKeyPrefix = Arrays.copyOf(identity.getBytes(), Long.BYTES);
			for (long height : heights)
			{
				DatabaseEntry key = new DatabaseEntry(Bytes.concat(identityKeyPrefix, Longs.toByteArray(height)));
				DatabaseEntry value = new DatabaseEntry(Bytes.concat(Longs.toByteArray(powers.get(height)), identity.getBytes()));
				if (this.votePowerDB.put(transaction, key, value) == OperationStatus.SUCCESS)
					powerLog.debug("Store vote power of "+powers.get(height)+" @ "+height+" for "+identity);
				else
					throw new DatabaseException("Failed to vote power of "+powers.get(height)+" @ "+height+" for "+identity);
			}
			
			transaction.commit();
			return OperationStatus.SUCCESS;
		}
		catch (DatabaseException dbex)
		{
			transaction.abort();
			throw dbex;
		}
		catch (Exception e)
		{
			transaction.abort();
			throw new DatabaseException(e);
		}
	}

	public Collection<ECPublicKey> getIdentities() throws IOException
	{
		Set<ECPublicKey> identities = new HashSet<ECPublicKey>();
		DatabaseEntry key = new DatabaseEntry();
	    DatabaseEntry value = new DatabaseEntry();
	    
		try (Cursor cursor = this.voteIdentityDB.openCursor(null, null)) 
		{
			OperationStatus status = cursor.getFirst(key, value, LockMode.DEFAULT);
			while (status.equals(OperationStatus.SUCCESS) == true)
			{
				identities.add(ECPublicKey.from(key.getData()));
				status = cursor.getNext(key, value, LockMode.DEFAULT);
			}
			
			return identities;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}
}

