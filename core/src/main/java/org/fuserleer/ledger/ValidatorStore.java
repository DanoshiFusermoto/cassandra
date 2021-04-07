package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.bouncycastle.util.Arrays;
import org.fuserleer.Context;
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.database.DatabaseException;
import org.fuserleer.database.DatabaseStore;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.Numbers;

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
	private Database 			votePowerDatabase;
	private SecondaryDatabase 	votePowerOwnersDatabase;
	private Database 			identitiesDatabase;

	private class IdentitySecondaryKeyCreator implements SecondaryKeyCreator
	{
		@Override
		public boolean createSecondaryKey(SecondaryDatabase database, DatabaseEntry key, DatabaseEntry value, DatabaseEntry secondary)
		{
			if (database.getDatabaseName().equals("vote_power_owners") == true)
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
			this.votePowerDatabase = getEnvironment().openDatabase(null, "vote_powers", config);

			SecondaryConfig identityConfig = new SecondaryConfig();
			identityConfig.setAllowCreate(true);
			identityConfig.setKeyCreator(new IdentitySecondaryKeyCreator());
			identityConfig.setSortedDuplicates(true);
			identityConfig.setTransactional(true);
			this.votePowerOwnersDatabase = getEnvironment().openSecondaryDatabase(null, "vote_power_owners", this.votePowerDatabase, identityConfig);
			
			DatabaseConfig validatorConfig = new DatabaseConfig();
			validatorConfig.setAllowCreate(true);
			validatorConfig.setTransactional(false);
			this.identitiesDatabase = getEnvironment().openDatabase(null, "identities", validatorConfig);

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
			getEnvironment().truncateDatabase(transaction, "identities", false);
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

		if (this.identitiesDatabase != null) this.identitiesDatabase.close();
		if (this.votePowerOwnersDatabase != null) this.votePowerOwnersDatabase.close();
		if (this.votePowerDatabase != null) this.votePowerDatabase.close();
	}

	@Override
	public void flush() throws DatabaseException  { /* Not used */ }

	public long get(final BLSPublicKey identity, final long height) throws DatabaseException
	{
		Objects.requireNonNull(identity, "Identity is null");
		Numbers.isNegative(height, "Height is negative");

		byte[] identityBytes = identity.toByteArray();
		byte[] identityKeyPrefix = Arrays.copyOf(identityBytes, Long.BYTES);
		DatabaseEntry search = new DatabaseEntry(identityBytes);
		DatabaseEntry key = new DatabaseEntry(Bytes.concat(identityKeyPrefix, Longs.toByteArray(height)));
	    DatabaseEntry value = new DatabaseEntry();
	    
		OperationStatus status = this.votePowerDatabase.get(null, key, value, LockMode.DEFAULT);
		if(status.equals(OperationStatus.SUCCESS) == true)
			return Longs.fromByteArray(value.getData());
	    
		try (SecondaryCursor cursor = this.votePowerOwnersDatabase.openCursor(null, null)) 
		{
			status = cursor.getSearchKeyRange(search, key, value, LockMode.DEFAULT);
			if (status.equals(OperationStatus.SUCCESS) == true)
			{
				if (Arrays.areEqual(identity.toByteArray(), search.getData()) == true)
				{
					status = cursor.getNextNoDup(key, value, LockMode.DEFAULT);
					if (status.equals(OperationStatus.SUCCESS) == false)
						status = cursor.getLast(search, key, value, LockMode.DEFAULT);
					else
						status = cursor.getPrev(search, key, value, LockMode.DEFAULT);
				}
				else
				{
					status = cursor.getPrev(search, key, value, LockMode.DEFAULT);
					if (status.equals(OperationStatus.SUCCESS) == false)
						status = cursor.getFirst(search, key, value, LockMode.DEFAULT);
				}
				
				if (status.equals(OperationStatus.SUCCESS) == false || Arrays.areEqual(search.getData(), identityBytes) == false)
					return 0;
				
				return Longs.fromByteArray(value.getData());
			}
			
			return 0;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}
	
	public long increment(final BLSPublicKey identity, final long height) throws DatabaseException
	{
		Objects.requireNonNull(identity, "Identity is null");
		Numbers.isNegative(height, "Height is negative");

		byte[] identityBytes = identity.toByteArray();
		byte[] identityKeyPrefix = Arrays.copyOf(identityBytes, Long.BYTES);
		DatabaseEntry search = new DatabaseEntry(identityBytes);
		DatabaseEntry key = new DatabaseEntry(Bytes.concat(identityKeyPrefix, Longs.toByteArray(height)));
	    DatabaseEntry value = new DatabaseEntry();
	    
		Transaction transaction = this.votePowerDatabase.getEnvironment().beginTransaction(null, null);
		try
		{
			long powerMaxHeight = 0;
			long powerAtHeight = 0;
			OperationStatus status;

			try (SecondaryCursor cursor = this.votePowerOwnersDatabase.openCursor(transaction, null)) 
			{
				status = cursor.getSearchKeyRange(search, key, value, LockMode.DEFAULT);
				if (status.equals(OperationStatus.SUCCESS) == true)
				{
					if (Arrays.areEqual(identity.toByteArray(), search.getData()) == true)
					{
						status = cursor.getNextNoDup(key, value, LockMode.DEFAULT);
						if (status.equals(OperationStatus.SUCCESS) == false)
							status = cursor.getLast(search, key, value, LockMode.DEFAULT);
						else
							status = cursor.getPrev(search, key, value, LockMode.DEFAULT);
					}
					else
					{
						status = cursor.getPrev(search, key, value, LockMode.DEFAULT);
						if (status.equals(OperationStatus.SUCCESS) == false)
							status = cursor.getFirst(search, key, value, LockMode.DEFAULT);
					}
					
					if (status.equals(OperationStatus.SUCCESS) == true && Arrays.areEqual(search.getData(), identityBytes) == true)
					{
						powerMaxHeight = Longs.fromByteArray(key.getData(), Long.BYTES);
						powerAtHeight = Longs.fromByteArray(value.getData());
					}
				}
			}
			
			key = new DatabaseEntry(Bytes.concat(identityKeyPrefix, Longs.toByteArray(height)));
			status = this.votePowerDatabase.get(transaction, key, value, LockMode.DEFAULT);
			if(status.equals(OperationStatus.SUCCESS) == true)
				powerAtHeight = Longs.fromByteArray(value.getData());
			
			if (powerMaxHeight <= height)
			{
				for (long h = powerMaxHeight ; h <= height ; h++)
				{
					key = new DatabaseEntry(Bytes.concat(identityKeyPrefix, Longs.toByteArray(h)));
					value = new DatabaseEntry(Arrays.concatenate(Longs.toByteArray(powerAtHeight), identity.toByteArray()));
				    status = this.votePowerDatabase.put(transaction, key, value);
					if (status.equals(OperationStatus.SUCCESS) == false)
						throw new DatabaseException("Failed to set vote power for "+identity+" @ "+h);
				}	
			}

			for (long h = height ; h <= Math.max(height, powerMaxHeight) ; h++)
			{
				key = new DatabaseEntry(Bytes.concat(identityKeyPrefix, Longs.toByteArray(h)));
			    status = this.votePowerDatabase.get(transaction, key, value, LockMode.READ_UNCOMMITTED);

			    long current = 0;
				if (status.equals(OperationStatus.SUCCESS) == true)
					current = Longs.fromByteArray(value.getData());
				else
					throw new DatabaseException("Vote power state for "+identity+" @ "+height+" may be corrupted");
				
				long incremented = current+1;
				value = new DatabaseEntry(Arrays.concatenate(Longs.toByteArray(incremented), identity.toByteArray()));
				status = this.votePowerDatabase.put(transaction, key, value);
				if (status.equals(OperationStatus.SUCCESS) == false)
					throw new DatabaseException("Failed to set vote power for "+identity+" @ "+h);
			}
			
			transaction.commit();
			return powerAtHeight+1;
		}
		catch (Exception e)
		{
			transaction.abort();
			throw new DatabaseException(e);
		}
	}
	
	/**
	 * Sets the power at a given height for identity and returns the previous power value
	 * 
	 * @param identity
	 * @param height
	 * @param power
	 * @return
	 * @throws DatabaseException
	 */
	public long set(final BLSPublicKey identity, final long height, final long power) throws DatabaseException
	{
		Objects.requireNonNull(identity, "Identity is null");
		Numbers.isNegative(height, "Height is negative");

		byte[] identityBytes = identity.toByteArray();
		byte[] identityKeyPrefix = Arrays.copyOf(identityBytes, Long.BYTES);
		DatabaseEntry search = new DatabaseEntry(identityBytes);
		DatabaseEntry key = new DatabaseEntry(Bytes.concat(identityKeyPrefix, Longs.toByteArray(height)));
	    DatabaseEntry value = new DatabaseEntry();
	    
		Transaction transaction = this.votePowerDatabase.getEnvironment().beginTransaction(null, null);
		try
		{
			long powerMaxHeight = 0;
			long powerAtHeight = 0;
			OperationStatus status;

			try (SecondaryCursor cursor = this.votePowerOwnersDatabase.openCursor(transaction, null)) 
			{
				status = cursor.getSearchKeyRange(search, key, value, LockMode.DEFAULT);
				if (status.equals(OperationStatus.SUCCESS) == true)
				{
					if (Arrays.areEqual(identity.toByteArray(), search.getData()) == true)
					{
						status = cursor.getNextNoDup(key, value, LockMode.DEFAULT);
						if (status.equals(OperationStatus.SUCCESS) == false)
							status = cursor.getLast(search, key, value, LockMode.DEFAULT);
						else
							status = cursor.getPrev(search, key, value, LockMode.DEFAULT);
					}
					else
					{
						status = cursor.getPrev(search, key, value, LockMode.DEFAULT);
						if (status.equals(OperationStatus.SUCCESS) == false)
							status = cursor.getFirst(search, key, value, LockMode.DEFAULT);
					}
					
					if (status.equals(OperationStatus.SUCCESS) == true && Arrays.areEqual(search.getData(), identityBytes) == true)
					{
						powerMaxHeight = Longs.fromByteArray(key.getData(), Long.BYTES);
						powerAtHeight = Longs.fromByteArray(value.getData());
					}
				}
			}
			
			key = new DatabaseEntry(Bytes.concat(identityKeyPrefix, Longs.toByteArray(height)));
			status = this.votePowerDatabase.get(transaction, key, value, LockMode.DEFAULT);
			if(status.equals(OperationStatus.SUCCESS) == true)
				powerAtHeight = Longs.fromByteArray(value.getData());
			
			if (powerAtHeight == power)
			{
				transaction.abort();
				return powerAtHeight;
			}
			
			if (powerMaxHeight <= height)
			{
				for (long h = powerMaxHeight ; h <= height ; h++)
				{
					key = new DatabaseEntry(Bytes.concat(identityKeyPrefix, Longs.toByteArray(h)));
					value = new DatabaseEntry(Arrays.concatenate(Longs.toByteArray(powerAtHeight), identity.toByteArray()));
				    status = this.votePowerDatabase.put(transaction, key, value);
					if (status.equals(OperationStatus.SUCCESS) == false)
						throw new DatabaseException("Failed to set vote power for "+identity+" @ "+h);
				}	
			}

			for (long h = height ; h <= Math.max(height, powerMaxHeight) ; h++)
			{
				key = new DatabaseEntry(Bytes.concat(identityKeyPrefix, Longs.toByteArray(h)));
			    status = this.votePowerDatabase.get(transaction, key, value, LockMode.READ_UNCOMMITTED);

			    long current = 0;
				if (status.equals(OperationStatus.SUCCESS) == true)
					current = Longs.fromByteArray(value.getData());
				else
					throw new DatabaseException("Vote power state for "+identity+" @ "+height+" may be corrupted");
				
				if (current < power)
				{
					value = new DatabaseEntry(Arrays.concatenate(Longs.toByteArray(power), identity.toByteArray()));
					status = this.votePowerDatabase.put(transaction, key, value);
					if (status.equals(OperationStatus.SUCCESS) == false)
						throw new DatabaseException("Failed to set vote power for "+identity+" @ "+h);
				}
			}
			
			transaction.commit();
			return powerAtHeight;
		}
		catch (Exception e)
		{
			transaction.abort();
			throw new DatabaseException(e);
		}
	}

	public Collection<BLSPublicKey> getWithPower() throws DatabaseException
	{
		Set<BLSPublicKey> identities = new HashSet<BLSPublicKey>();
		DatabaseEntry key = new DatabaseEntry();
	    DatabaseEntry value = new DatabaseEntry();
	    
		try (Cursor cursor = this.votePowerOwnersDatabase.openCursor(null, null)) 
		{
			OperationStatus status = cursor.getFirst(key, value, LockMode.DEFAULT);
			while (status.equals(OperationStatus.SUCCESS) == true)
			{
				identities.add(BLSPublicKey.from(key.getData()));
				status = cursor.getNextNoDup(key, value, LockMode.DEFAULT);
			}
			
			return identities;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}

	public Collection<BLSPublicKey> get(long shardGroup, long numShardGroups) throws DatabaseException
	{
		Set<BLSPublicKey> identities = new HashSet<BLSPublicKey>();
		DatabaseEntry key = new DatabaseEntry();
	    DatabaseEntry value = new DatabaseEntry();
	    
		try (Cursor cursor = this.identitiesDatabase.openCursor(null, null)) 
		{
			OperationStatus status = cursor.getFirst(key, value, LockMode.DEFAULT);
			while (status.equals(OperationStatus.SUCCESS) == true)
			{
				BLSPublicKey identity = Serialization.getInstance().fromDson(value.getData(), BLSPublicKey.class);
				if (ShardMapper.toShardGroup(identity, numShardGroups) == shardGroup)
					identities.add(identity);
				
				status = cursor.getNextNoDup(key, value, LockMode.DEFAULT);
			}
			
			return identities;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}
	
	public Collection<BLSPublicKey> getIdentities() throws DatabaseException
	{
		Set<BLSPublicKey> identities = new HashSet<BLSPublicKey>();
		DatabaseEntry key = new DatabaseEntry();
	    DatabaseEntry value = new DatabaseEntry();
	    
		try (Cursor cursor = this.identitiesDatabase.openCursor(null, null)) 
		{
			OperationStatus status = cursor.getFirst(key, value, LockMode.DEFAULT);
			while (status.equals(OperationStatus.SUCCESS) == true)
			{
				identities.add(Serialization.getInstance().fromDson(value.getData(), BLSPublicKey.class));
				status = cursor.getNextNoDup(key, value, LockMode.DEFAULT);
			}
			
			return identities;
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}

	// FIXME need to handle cases where the identity is known but the binding has changed
	//		 attacker could present a changed binding to a single node, and BLS signatures
	//		 will then fail ... causing that node to be unable to sync, or perform excessive
	//		 work to resolve the issue.
	public OperationStatus store(final BLSPublicKey identity) throws DatabaseException
	{
		Objects.requireNonNull(identity, "Identity is null");
		
		try
        {
			DatabaseEntry key = new DatabaseEntry(identity.toByteArray());
			byte[] bytes = Serialization.getInstance().toDson(identity, Output.PERSIST);
			DatabaseEntry value = new DatabaseEntry(bytes);
			OperationStatus status = this.identitiesDatabase.putNoOverwrite(null, key, value);
			
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    	{
		    		powerLog.warn(this.context.getName()+": Identity "+identity+" is already present");
		    		return status;
		    	}
		    	else 
		    		throw new DatabaseException("Failed to store identity "+identity+" due to "+status.name());
		    }
		    
		    return status;
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
}

