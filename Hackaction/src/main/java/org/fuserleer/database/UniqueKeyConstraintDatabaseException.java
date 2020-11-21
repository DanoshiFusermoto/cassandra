package org.fuserleer.database;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.sleepycat.je.DatabaseEntry;

public class UniqueKeyConstraintDatabaseException extends DatabaseException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7880501810564801476L;
	
	private final String database;
	private final DatabaseEntry key;

	public UniqueKeyConstraintDatabaseException(String database, DatabaseEntry key)
	{
		super("Unique key "+toString(key)+" violates constraints in database "+database);

		this.key = key;
		this.database = database;
	}

	public String getDatabase()
	{
		return this.database;
	}

	public DatabaseEntry getKey()
	{
		return this.key;
	}

	private static String toString(DatabaseEntry de) 
	{
		byte[] bytes = de.getData();
		return IntStream.range(0, bytes.length).map(i -> bytes[i] & 0xFF).mapToObj(i -> String.format("%02X", i)).collect(Collectors.joining(" "));
	}
}