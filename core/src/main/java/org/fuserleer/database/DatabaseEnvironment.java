package org.fuserleer.database;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.bouncycastle.util.Arrays;
import org.fuserleer.Context;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public final class DatabaseEnvironment
{
	private static final Logger log = Logging.getLogger();

	private Database 	metaDatabase;
	private Context		context = null;
	private Environment	environment = null;
	private Map<Class<?>, DatabaseStore> 	databases = new HashMap<>();

    public DatabaseEnvironment(final Context context, final File home) 
    { 
    	this.context = Objects.requireNonNull(context, "Context is null");
    	
		home.mkdirs();

		System.setProperty("je.disable.java.adler32", "true");

		EnvironmentConfig environmentConfig = new EnvironmentConfig();
		environmentConfig.setTransactional(true);
		environmentConfig.setAllowCreate(true);
		environmentConfig.setLockTimeout(30, TimeUnit.SECONDS);
		environmentConfig.setDurability(Durability.COMMIT_NO_SYNC);
//		environmentConfig.setConfigParam(EnvironmentConfig.ENV_DUP_CONVERT_PRELOAD_ALL, "false");
		
		long logFileSize = 1000000000l;
		long logFileMinClean = 4000000000l;
		environmentConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, String.valueOf(logFileSize));
//		environmentConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, "1000000000");
		environmentConfig.setConfigParam(EnvironmentConfig.LOG_FILE_CACHE_SIZE, "1000");
		environmentConfig.setConfigParam(EnvironmentConfig.LOG_FLUSH_SYNC_INTERVAL, "60");
		environmentConfig.setConfigParam(EnvironmentConfig.LOG_FLUSH_NO_SYNC_INTERVAL, "10");
//		environmentConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
//		environmentConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
//		environmentConfig.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR, "false");
		environmentConfig.setConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER, "false");
		environmentConfig.setConfigParam(EnvironmentConfig.NODE_MAX_ENTRIES, "256");
		environmentConfig.setConfigParam(EnvironmentConfig.TREE_MAX_EMBEDDED_LN, "32");
		environmentConfig.setConfigParam(EnvironmentConfig.LOG_FAULT_READ_SIZE, "8192");

		// EVICTOR PARAMS //
		environmentConfig.setConfigParam(EnvironmentConfig.EVICTOR_MAX_THREADS, "1"); // TODO check one is enough, otherwise -> String.valueOf(Math.max(1, Runtime.getRuntime().availableProcessors() / 4)));
		environmentConfig.setConfigParam(EnvironmentConfig.EVICTOR_CRITICAL_PERCENTAGE, "10"); // TODO make sure this is accounted for in cache sizing!
		
		// CLEANER PARAMS //
		environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_WAKEUP_INTERVAL, "600");
		environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_AGE, String.valueOf((int) (logFileMinClean / logFileSize)));
		environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE, "65536");
		environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION, "50");
		environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION, "50");
//		environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION, "50");
		
		// CHECKPOINTER PARAMS //
		environmentConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, String.valueOf(1<<30));

		long cacheSize = (long)(Runtime.getRuntime().maxMemory()*0.125) / Integer.parseInt(this.context.getConfiguration().getCommandLine().getOptionValue("contexts", "1"));
		environmentConfig.setCacheSize(cacheSize);
//		environmentConfig.setCacheMode(CacheMode.EVICT_LN);

		this.environment = new Environment(home, environmentConfig);

		DatabaseConfig primaryConfig = new DatabaseConfig();
		primaryConfig.setAllowCreate(true);
		primaryConfig.setTransactional(true);

		this.metaDatabase = getEnvironment().openDatabase(null, "environment.meta_data", primaryConfig);
	}

	public void close()
	{
        flush();

        Collection<DatabaseStore> databases = new ArrayList<>(this.databases.values());
        for (DatabaseStore database : databases)
        {
			try
        	{
				database.close();
			}
        	catch (Exception ex)
			{
        		log.error("Failure stopping database "+database.getClass().getName(), ex);
			}
        }

        this.metaDatabase.close();
		this.metaDatabase = null;

       	this.environment.close();
       	this.environment = null;
	}

	public Environment getEnvironment()
	{
		return this.environment;
	}

	public void flush()
	{
        for (DatabaseStore database : this.databases.values())
        {
            try { database.flush(); } catch (Exception ex)
            {
            	log.error("Flushing "+database.getClass().getName()+" failed", ex);
    		}
        }
	}

	public void register(final DatabaseStore database)
	{
		Objects.requireNonNull(database, "Database to register is null");
		
		if (this.databases.containsKey(database.getClass()) == false)
			this.databases.put(database.getClass(), database);
	}

	public boolean isRegistered(final DatabaseStore database) 
	{
		Objects.requireNonNull(database, "Database is null");
		return this.databases.containsKey(database.getClass());
	}

	public void deregister(final DatabaseStore database)
	{
		Objects.requireNonNull(database, "Database to deregister is null");
		if (this.databases.containsKey(database.getClass()))
			this.databases.remove(database.getClass());
	}

	public OperationStatus put(final Transaction transaction, final String resource, final String key, final byte[] value)
	{
		return this.put(transaction, resource, new DatabaseEntry(key.getBytes()), new DatabaseEntry(value));
	}

	public OperationStatus put(final Transaction transaction, final String resource, final String key, final DatabaseEntry value)
	{
		return this.put(transaction, resource, new DatabaseEntry(key.getBytes()), value);
	}

	public OperationStatus put(final Transaction transaction, final String resource, final DatabaseEntry key, final DatabaseEntry value)
	{
		Objects.requireNonNull(resource, "Resource is null");
		if (resource.length() == 0)
			throw new IllegalArgumentException("Resource length is 0");

		Objects.requireNonNull(key, "Key is null");
		Objects.requireNonNull(key.getData(), "Key data is null");
		if (key.getData().length == 0)
			throw new IllegalArgumentException("Key data length is 0");

		Objects.requireNonNull(value, "Value is null");
		Objects.requireNonNull(value.getData(), "Value data is null");
		if (value.getData().length == 0)
			throw new IllegalArgumentException("Value data length is 0");

		// Create a key specific to the database //
		key.setData(Arrays.concatenate(resource.getBytes(StandardCharsets.UTF_8), key.getData()));

		return this.metaDatabase.put(transaction, key, value);
	}

	public byte[] get(final String resource, final String key)
	{
		DatabaseEntry value = new DatabaseEntry();

		if (this.get(resource, new DatabaseEntry(key.getBytes()), value) == OperationStatus.SUCCESS)
			return value.getData();

		return null;
	}

	public OperationStatus get(final String resource, final String key, final DatabaseEntry value)
	{
		return this.get(resource, new DatabaseEntry(key.getBytes()), value);
	}

	public OperationStatus get(final String resource, final DatabaseEntry key, final DatabaseEntry value)
	{
		Objects.requireNonNull(resource, "Resource is null");
		if (resource.length() == 0)
			throw new IllegalArgumentException("Resource length is 0");

		Objects.requireNonNull(key, "Key is null");
		Objects.requireNonNull(key.getData(), "Key data is null");
		if (key.getData().length == 0)
			throw new IllegalArgumentException("Key data length is 0");

		// Create a key specific to the database //
		key.setData(Arrays.concatenate(resource.getBytes(StandardCharsets.UTF_8), key.getData()));
		
		return this.metaDatabase.get(null, key, value, LockMode.READ_UNCOMMITTED);
	}
	
	public Transaction beginTransaction(final Transaction parent, final TransactionConfig config)
	{
		return this.environment.beginTransaction(parent, config);
	}
	
	public Database openDatabase(final Transaction transaction, final String name, final DatabaseConfig config)
	{
		return this.environment.openDatabase(transaction, name, config);
	}

	public SecondaryDatabase openSecondaryDatabase(final Transaction transaction, final String name, final Database database, final SecondaryConfig config)
	{
		return this.environment.openSecondaryDatabase(transaction, name, database, config);
	}

	public long truncateDatabase(final Transaction transaction, final String name, final boolean count)
	{
		return this.environment.truncateDatabase(transaction, name, count);
	}
}
