package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Objects;

import org.fuserleer.Context;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.DatabaseException;
import org.fuserleer.database.DatabaseStore;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class LedgerStore extends DatabaseStore
{
	private static final Logger databaseLog = Logging.getLogger("database");
	
	private final Context context;

	private Database primitivesDatabase;

	public LedgerStore(Context context)
	{
		super(Objects.requireNonNull(context.getDatabaseEnvironment()));
		this.context = context;
		
		// GOT IT!
		databaseLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN | Logging.WARN);
	}

	@Override
	public void start() throws StartupException
	{
		try
		{
			if (this.context.getConfiguration().getCommandLine().hasOption("clean") == true)
				clean();
				
			DatabaseConfig primitivesConfig = new DatabaseConfig();
			primitivesConfig.setAllowCreate(true);
			primitivesConfig.setTransactional(true);
			primitivesConfig.setKeyPrefixing(true);

			Transaction transaction = null;
			try
			{
				transaction = getEnvironment().beginTransaction(null, new TransactionConfig().setReadUncommitted(true));

				this.primitivesDatabase = getEnvironment().openDatabase(transaction, "hackation.primitives", primitivesConfig);
				
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
		catch (IOException ioex)
		{
			throw new StartupException(ioex);
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
	public void close() throws IOException
	{
		super.close();

		if (this.primitivesDatabase != null) this.primitivesDatabase.close();
	}
	
	@Override
	public void clean() throws DatabaseException
	{
		Transaction transaction = null;

		try
		{
			transaction = getEnvironment().beginTransaction(null, new TransactionConfig().setReadUncommitted(true));
			getEnvironment().truncateDatabase(transaction, "hackation.primitives", false);

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
	public void flush() throws DatabaseException  { /* Not used */ }

	@SuppressWarnings("unchecked")
	public <T extends Primitive> T get(final Hash hash, final Class<T> primitive) throws IOException
	{
		try
        {
			DatabaseEntry key = new DatabaseEntry(hash.toByteArray());
			DatabaseEntry value = new DatabaseEntry();
			
			if (primitive.equals(Atom.class) == true || primitive.equals(BlockHeader.class) == true)
			{
				OperationStatus status = this.primitivesDatabase.get(null, key, value, LockMode.DEFAULT);
				if (status.equals(OperationStatus.SUCCESS) == true)
				{
					try
					{
						return (T) Serialization.getInstance().fromDson(value.getData(), primitive);
					}
					// FIXME Hack to catch this and convert to a SerializationException that is easier to handle.
					catch (IllegalArgumentException iaex)
					{
						throw new SerializationException(iaex.getMessage());
					}
				}
			}
			else if (primitive.equals(Block.class) == true)
			{
				throw new UnsupportedOperationException("Block retreival from store not implemented yet");
			}
			else 
				throw new IllegalArgumentException();
			
			return null;
        }
		catch (Exception ex)
		{
			if (ex instanceof IOException)
				throw ex;
			else
				throw new DatabaseException(ex);
		}
	}

	boolean has(Hash hash) throws DatabaseException
	{
		try
        {
			OperationStatus status = this.primitivesDatabase.get(null, new DatabaseEntry(hash.toByteArray()), null, LockMode.DEFAULT);
			if (status.equals(OperationStatus.SUCCESS) == true)
				return true;

			return false;
        }
		catch (Throwable t)
		{
			if (t instanceof DatabaseException)
				throw (DatabaseException)t;
			else
				throw new DatabaseException(t);
		}
	}
	
	final OperationStatus store(Block block) throws IOException 
	{
		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			BlockHeader blockHeader = block.toHeader();
			OperationStatus status = store(transaction, block.getHash(), blockHeader, Serialization.getInstance().toDson(blockHeader, DsonOutput.Output.PERSIST));
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    		databaseLog.warn(this.context.getName()+": Block header " + blockHeader + " is already present");
		    	else 
		    		throw new DatabaseException("Failed to store " + blockHeader.getHash() + " due to " + status.name());
		    } 
		    
		    for (Atom atom : block.getAtoms())
		    {
				status = store(transaction, atom.getHash(), atom, Serialization.getInstance().toDson(atom, DsonOutput.Output.PERSIST));
			    if (status.equals(OperationStatus.SUCCESS) == false) 
			    {
			    	if (status.equals(OperationStatus.KEYEXIST) == true) 
			    		databaseLog.warn(this.context.getName()+": Atom "+atom.getHash()+" in block "+blockHeader + " is already present");
			    	else 
			    		throw new DatabaseException("Failed to store atom "+atom.getHash()+" in block "+blockHeader + " due to " + status.name());
			    } 
		    }
		    
		    transaction.commit();
		    return OperationStatus.SUCCESS;
		} 
		catch (Exception ex) 
		{
			transaction.abort();
		    if (ex instanceof DatabaseException)
		    	throw ex; 
		    throw new DatabaseException(ex);
		} 
	}
	
	final OperationStatus store(Atom atom) throws IOException 
	{
		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			OperationStatus status = store(transaction, atom.getHash(), atom, Serialization.getInstance().toDson(atom, DsonOutput.Output.PERSIST));
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    		databaseLog.warn(this.context.getName()+": Atom "+atom.getHash()+" is already present");
		    	else 
		    		throw new DatabaseException("Failed to store atom "+atom.getHash()+" due to "+status.name());
		    } 

		    transaction.commit();
		    return OperationStatus.SUCCESS;
		} 
		catch (Exception ex) 
		{
			transaction.abort();
		    if (ex instanceof DatabaseException)
		    	throw ex; 
		    throw new DatabaseException(ex);
		} 
	}

	private OperationStatus store(Transaction transaction, Hash hash, Primitive primitive, byte[] bytes) throws IOException 
	{
		Objects.requireNonNull(hash);
		Objects.requireNonNull(bytes);
		DatabaseEntry key = new DatabaseEntry(hash.toByteArray());
		DatabaseEntry value = new DatabaseEntry(bytes);
		OperationStatus status = this.primitivesDatabase.putNoOverwrite(transaction, key, value);
		return status;
	}
}
