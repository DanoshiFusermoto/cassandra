package org.fuserleer.database;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executor;
import org.fuserleer.executors.ScheduledExecutable;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.Bytes;
import org.fuserleer.utils.Longs;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public final class SystemMetaData extends DatabaseStore implements Service
{
	private static final Logger log = Logging.getLogger();
	private static final int MAX_LENGTH = 256;

	private final Context context;
	private Map<String, Object> metaDataCache = new ConcurrentHashMap<>();
	private Database metaDataDB = null;
	private Future<?> flush;
	
	public SystemMetaData(final Context context)
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
	
			this.metaDataDB = getEnvironment().openDatabase(null, "meta_data", config);

			load();
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}

		this.flush = Executor.getInstance().scheduleWithFixedDelay(new ScheduledExecutable(1, 1, TimeUnit.SECONDS)
		{
			@Override
			public void execute()
			{
				try
				{
					flush();
				}
				catch (DatabaseException e)
				{
					log.error(e.getMessage(), e);
				}
			}
		});
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
			getEnvironment().truncateDatabase(transaction, "meta_data", false);
			transaction.commit();
			this.metaDataCache.clear();
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
		if (this.flush != null)
			this.flush.cancel(false);

		super.close();

		this.metaDataDB.close();
	}

	@Override
	public synchronized void flush() throws DatabaseException
	{
		try
        {
			for (Map.Entry<String, Object> e : this.metaDataCache.entrySet())
			{
				DatabaseEntry key = new DatabaseEntry(e.getKey().getBytes(StandardCharsets.UTF_8));
				Object value = e.getValue();
				Class<?> valueClass = value.getClass();
				final byte[] bytes;
				if (valueClass.equals(String.class)) {
					String stringValue = (String) value;
					byte[] stringBytes = stringValue.getBytes(StandardCharsets.UTF_8);
					bytes = new byte[1 + stringBytes.length];
					bytes[0] = 'S';
					System.arraycopy(stringBytes, 0, bytes, 1, stringBytes.length);
				} else if (valueClass.equals(Long.class)) {
					Long longValue = (Long) value;
					bytes = new byte[1 + Long.BYTES];
					bytes[0] = 'L';
					Longs.copyTo(longValue.longValue(), bytes, 1);
				} else if (valueClass.equals(byte[].class)) {
					byte[] bytesValue = (byte[]) value;
					bytes = new byte[1 + bytesValue.length];
					bytes[0] = 'B';
					System.arraycopy(bytesValue, 0, bytes, 1, bytesValue.length);
				} else {
					throw new IllegalArgumentException("Unknown value type: " + valueClass.getName());
				}

				this.metaDataDB.put(null, key, new DatabaseEntry(bytes));
			}
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}

	// SYSTEM METRICS //
	public boolean has(final String name)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);
		
		return this.metaDataCache.containsKey(name);
	}

	public String get(final String name, final String option)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		Objects.requireNonNull(option, "Option is null");
		if (option.length() == 0)
			throw new IllegalArgumentException("Option length is zero");
		if (option.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Option length is greater than "+MAX_LENGTH);

		Object value = this.metaDataCache.get(name);

		if (value == null)
			return option;

		return asString(value);
	}

	public long get(final String name, final long option)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		Object value = this.metaDataCache.get(name);

		if (value == null)
			return option;

		return asLong(value);
	}

	public byte[] get(final String name, final byte[] option)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		Objects.requireNonNull(option, "Option is null");
		if (option.length == 0)
			throw new IllegalArgumentException("Option length is zero");
		if (option.length > MAX_LENGTH)
			throw new IllegalArgumentException("Option length is greater than "+MAX_LENGTH);

		Object value = this.metaDataCache.get(name);

		if (value == null)
			return option;

		return asBytes(value);
	}

	public long increment(final String name)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		return (long) this.metaDataCache.compute(name, (k, v) -> {
			long value = (long) this.metaDataCache.getOrDefault(k, 0l) + 1;
			return value;
		});
	}

	public long increment(final String name, final long increment)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		return (long) this.metaDataCache.compute(name, (k, v) -> {
			long value = (long) this.metaDataCache.getOrDefault(k, 0l) + increment;
			return value;
		});
	}

	public long decrement(final String name)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		return (long) this.metaDataCache.compute(name, (k, v) -> {
			long value = (long) this.metaDataCache.getOrDefault(k, 0l) - 1;
			return value;
		});
	}

	public long decrement(final String name, long decrement)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		return (long) this.metaDataCache.compute(name, (k, v) -> {
			long value = (long) this.metaDataCache.getOrDefault(k, 0l) - decrement;
			return value;
		});
	}

	public void put(final String name, final String value)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		Objects.requireNonNull(value, "Value is null");
		if (value.length() == 0)
			throw new IllegalArgumentException("Value length is zero");
		if (value.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Value length is greater than "+MAX_LENGTH);

		this.metaDataCache.put(name, value);
	}

	public void put(final String name, final long value)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		this.metaDataCache.put(name, value);
	}

	public void put(final String name, final byte[] value)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		Objects.requireNonNull(value, "Value is null");
		if (value.length == 0)
			throw new IllegalArgumentException("Value length is zero");
		if (value.length > MAX_LENGTH)
			throw new IllegalArgumentException("Value length is greater than "+MAX_LENGTH);

		// Take a defensive copy
		this.metaDataCache.put(name, value.clone());
	}

	/**
	 * Gets the meta data from the DB
	 *
	 * @throws DatabaseException
	 */
	private void load() throws DatabaseException
	{
		try (Cursor cursor = this.metaDataDB.openCursor(null, null))
        {
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry value = new DatabaseEntry();

			this.metaDataCache.clear();

			while (cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
			{
				String keyString = Bytes.toString(key.getData()).toLowerCase();
				byte[] bytes = value.getData();
				byte[] newBytes = new byte[bytes.length - 1];
				System.arraycopy(bytes, 1, newBytes, 0, newBytes.length);
				switch (bytes[0]) {
				case 'S':
					this.metaDataCache.put(keyString, Bytes.toString(newBytes));
					break;
				case 'L':
					this.metaDataCache.put(keyString, Longs.fromByteArray(newBytes));
					break;
				case 'B':
					this.metaDataCache.put(keyString, newBytes);
					break;
				default:
					throw new IllegalArgumentException("Unknown type byte: " + bytes[0]);
				}
			}
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}

	private static String asString(final Object value)
	{
		Objects.requireNonNull(value, "Value is null");
		return (String) value;
	}

	private static long asLong(final Object value)
	{
		Objects.requireNonNull(value, "Value is null");
		return ((Long) value).longValue();
	}

	private static byte[] asBytes(final Object value)
	{
		Objects.requireNonNull(value, "Value is null");
		return (byte[]) value;
	}
}
