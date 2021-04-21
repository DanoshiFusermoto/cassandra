package org.fuserleer.ledger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.bouncycastle.util.Arrays;
import org.fuserleer.Context;
import org.fuserleer.Universe;
import org.fuserleer.collections.LRUCacheMap;
import org.fuserleer.common.Order;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Certificate;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.DatabaseException;
import org.fuserleer.database.DatabaseStore;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;
import org.fuserleer.utils.Numbers;
import org.fuserleer.utils.UInt256;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class LedgerStore extends DatabaseStore implements LedgerProvider
{
	private static final Logger databaseLog = Logging.getLogger("database");
	
	private final Context context;

	private Database primitives;
	private Database stateOperations;
	private Database stateCommits;
	private Database stateAssociations;
	private Database syncChain;
	private Database syncCache;
	
	private final Map<Hash, Hash> primitiveLRW;

	public LedgerStore(Context context)
	{
		super(Objects.requireNonNull(context.getDatabaseEnvironment()));
		this.context = context;
		this.primitiveLRW = Collections.synchronizedMap(new LRUCacheMap<Hash, Hash>(this.context.getConfiguration().get("ledger.lrw.cache", 1<<16)));
		
		// GOT IT!
//		databaseLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
		databaseLog.setLevels(Logging.ERROR | Logging.FATAL); // | Logging.INFO | Logging.WARN);
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
			
			DatabaseConfig syncChainConfig = new DatabaseConfig();
			syncChainConfig.setAllowCreate(true);
			syncChainConfig.setTransactional(true);
			syncChainConfig.setKeyPrefixing(true);

			DatabaseConfig syncCacheConfig = new DatabaseConfig();
			syncCacheConfig.setAllowCreate(true);
			syncCacheConfig.setTransactional(true);
			syncCacheConfig.setKeyPrefixing(true);
			syncCacheConfig.setSortedDuplicates(true);

			DatabaseConfig stateOperationsConfig = new DatabaseConfig();
			stateOperationsConfig.setAllowCreate(true);
			stateOperationsConfig.setTransactional(true);
			stateOperationsConfig.setKeyPrefixing(true);

			DatabaseConfig stateCommitsConfig = new DatabaseConfig();
			stateCommitsConfig.setAllowCreate(true);
			stateCommitsConfig.setTransactional(true);
			stateCommitsConfig.setKeyPrefixing(true);

			DatabaseConfig stateAssociationsConfig = new DatabaseConfig();
			stateAssociationsConfig.setAllowCreate(true);
			stateAssociationsConfig.setTransactional(true);
			stateAssociationsConfig.setSortedDuplicates(true);
			stateAssociationsConfig.setKeyPrefixing(true);

			Transaction transaction = null;
			try
			{
				transaction = getEnvironment().beginTransaction(null, new TransactionConfig().setReadUncommitted(true));

				this.primitives = getEnvironment().openDatabase(transaction, "hackation.primitives", primitivesConfig);
				this.syncChain = getEnvironment().openDatabase(transaction, "hackation.sync.chain", syncChainConfig);
				this.syncCache = getEnvironment().openDatabase(transaction, "hackation.sync.cache", syncCacheConfig);
				this.stateCommits = getEnvironment().openDatabase(transaction, "hackation.state.commits", stateCommitsConfig);
				this.stateOperations = getEnvironment().openDatabase(transaction, "hackation.state.operations", stateOperationsConfig);
				this.stateAssociations = getEnvironment().openDatabase(transaction, "hackation.state.associations", stateAssociationsConfig);

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

		if (this.stateAssociations != null) this.stateAssociations.close();
		if (this.stateOperations != null) this.stateOperations.close();
		if (this.stateCommits != null) this.stateCommits.close();
		if (this.syncChain != null) this.syncChain.close();
		if (this.syncCache != null) this.syncCache.close();
		if (this.primitives != null) this.primitives.close();
	}
	
	@Override
	public void clean() throws DatabaseException
	{
		Transaction transaction = null;

		try
		{
			transaction = getEnvironment().beginTransaction(null, new TransactionConfig().setReadUncommitted(true));
			getEnvironment().truncateDatabase(transaction, "hackation.primitives", false);
			getEnvironment().truncateDatabase(transaction, "hackation.sync.chain", false);
			getEnvironment().truncateDatabase(transaction, "hackation.sync.cache", false);
			getEnvironment().truncateDatabase(transaction, "hackation.state.commits", false);
			getEnvironment().truncateDatabase(transaction, "hackation.state.operations", false);
			getEnvironment().truncateDatabase(transaction, "hackation.state.associations", false);

			transaction.commit();
			
			this.primitiveLRW.clear();
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

	// PRIMITIVES //
	@SuppressWarnings("unchecked")
	public <T extends Primitive> T get(final Hash hash, final Class<T> primitive) throws IOException
	{
		try
        {
			DatabaseEntry key = new DatabaseEntry(hash.toByteArray());
			DatabaseEntry value = new DatabaseEntry();
			
			if (primitive.equals(Atom.class) == true || primitive.equals(BlockHeader.class) == true || 
				Vote.class.isAssignableFrom(primitive) == true || Certificate.class.isAssignableFrom(primitive) == true)
			{
				OperationStatus status = this.primitives.get(null, key, value, LockMode.DEFAULT);
				if (status.equals(OperationStatus.SUCCESS) == true)
				{
					T result;
					
					try
					{
						result = (T) Serialization.getInstance().fromDson(value.getData(), primitive);
					}
					// FIXME Hack to catch this and convert to a SerializationException that is easier to handle.
					catch (IllegalArgumentException iaex)
					{
						throw new SerializationException(iaex.getMessage());
					}
					
					if (primitive.equals(Atom.class) == true)
					{
						// Get the state objects for this Atom, updating with latest values if required
						Map<Hash, StateObject> states = new HashMap<Hash, StateObject>();
						for (StateObject stateObject : ((Atom)result).getStates().values())
						{
							UInt256 stateValue = this.get(stateObject.getKey());
							if (stateValue != null)
								states.put(stateObject.getHash(), new StateObject(stateObject.getKey(), stateValue));
							else
								states.put(stateObject.getHash(),  new StateObject(stateObject.getKey()));
						}
						((Atom)result).setStates(states);
					}
					
					return result;
				}
			}
			else if (primitive.equals(Block.class) == true)
			{
				OperationStatus status = this.primitives.get(null, key, value, LockMode.DEFAULT);
				if (status.equals(OperationStatus.SUCCESS) != true)
					return null;
				
				BlockHeader header = (BlockHeader) Serialization.getInstance().fromDson(value.getData(), BlockHeader.class);
				List<Atom> atoms = new ArrayList<Atom>();
				for (Hash atomHash : header.getInventory(InventoryType.ATOMS))
				{
					key = new DatabaseEntry(atomHash.toByteArray());
					status = this.primitives.get(null, key, value, LockMode.DEFAULT);
					if (status.equals(OperationStatus.SUCCESS) != true)
						throw new IllegalStateException("Found block header "+hash+" but contained atom "+atomHash+" is missing");

					Atom atom;
					try
					{
						atom = Serialization.getInstance().fromDson(value.getData(), Atom.class);
					}
					// FIXME Hack to catch this and convert to a SerializationException that is easier to handle.
					catch (IllegalArgumentException iaex)
					{
						throw new SerializationException(iaex.getMessage());
					}
					
					// Get the state objects for this Atom, updating with latest values if required
					Map<Hash, StateObject> states = new HashMap<Hash, StateObject>();
					for (StateObject stateObject : atom.getStates().values())
					{
						UInt256 stateValue = this.get(stateObject.getKey());
						if (stateValue != null)
							states.put(stateObject.getHash(), new StateObject(stateObject.getKey(), stateValue));
						else
							states.put(stateObject.getHash(), new StateObject(stateObject.getKey()));
					}
					atom.setStates(states);
					
					atoms.add(atom);
				}
				
				List<AtomCertificate> certificates = new ArrayList<AtomCertificate>();
				for (Hash certificateHash : header.getInventory(InventoryType.CERTIFICATES))
				{
					key = new DatabaseEntry(certificateHash.toByteArray());
					status = this.primitives.get(null, key, value, LockMode.DEFAULT);
					if (status.equals(OperationStatus.SUCCESS) != true)
						throw new IllegalStateException("Found block header "+hash+" but contained certificate "+certificateHash+" is missing");

					AtomCertificate certificate;
					try
					{
						certificate = Serialization.getInstance().fromDson(value.getData(), AtomCertificate.class);
					}
					// FIXME Hack to catch this and convert to a SerializationException that is easier to handle.
					catch (IllegalArgumentException iaex)
					{
						throw new SerializationException(iaex.getMessage());
					}
					
					certificates.add(certificate);
				}

				return (T) new Block(header, atoms, certificates);
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

	boolean has(final Hash hash) throws DatabaseException
	{
		Objects.requireNonNull(hash, "Hash is null");
		Hash.notZero(hash, "Hash is ZERO");
		
		try
        {
			if (this.primitiveLRW.containsKey(hash) == true)
				return true;
			
			OperationStatus status = this.primitives.get(null, new DatabaseEntry(hash.toByteArray()), null, LockMode.DEFAULT);
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
	
	final OperationStatus store(final Block block) throws IOException 
	{
		Objects.requireNonNull(block, "Block is null");
		
		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			Map<Hash, Hash> LRWItems = new HashMap<Hash, Hash>();
			OperationStatus status;
			BlockHeader blockHeader = block.getHeader();
			if (this.primitiveLRW.containsKey(blockHeader.getHash()) == false)
			{
				status = store(transaction, block.getHash(), blockHeader, Serialization.getInstance().toDson(blockHeader, DsonOutput.Output.PERSIST));
			    if (status.equals(OperationStatus.SUCCESS) == false) 
			    {
			    	if (status.equals(OperationStatus.KEYEXIST) == true) 
			    		databaseLog.warn(this.context.getName()+": Block header " + blockHeader + " is already present");
			    	else 
			    		throw new DatabaseException("Failed to store " + blockHeader.getHash() + " due to " + status.name());
			    } 
			    LRWItems.put(blockHeader.getHash(), blockHeader.getHash());
			}
		    
		    for (Atom atom : block.getAtoms())
		    {
				if (this.primitiveLRW.containsKey(atom.getHash()) == false)
				{
					status = store(transaction, atom.getHash(), atom, Serialization.getInstance().toDson(atom, DsonOutput.Output.PERSIST));
				    if (status.equals(OperationStatus.SUCCESS) == false) 
				    {
				    	if (status.equals(OperationStatus.KEYEXIST) == true) 
				    		databaseLog.warn(this.context.getName()+": Atom "+atom.getHash()+" in block "+blockHeader + " is already present");
				    	else 
				    		throw new DatabaseException("Failed to store atom "+atom.getHash()+" in block "+blockHeader + " due to " + status.name());
				    }
				    LRWItems.put(atom.getHash(), atom.getHash());
				}
		    }
		    
		    for (AtomCertificate certificate : block.getCertificates())
		    {
				if (this.primitiveLRW.containsKey(certificate.getHash()) == false)
				{
					status = store(transaction, certificate.getHash(), certificate, Serialization.getInstance().toDson(certificate, DsonOutput.Output.PERSIST));
				    if (status.equals(OperationStatus.SUCCESS) == false) 
				    {
				    	if (status.equals(OperationStatus.KEYEXIST) == true) 
				    		databaseLog.warn(this.context.getName()+": Atom certificate "+certificate.getHash()+" for "+certificate.getAtom()+" in block "+blockHeader + " is already present");
				    	else 
				    		throw new DatabaseException("Failed to store atom certificate "+certificate.getHash()+" for "+certificate.getAtom()+" in block "+blockHeader + " due to " + status.name());
				    } 
				    LRWItems.put(certificate.getHash(), certificate.getHash());
				}
		    }

		    transaction.commit();
		    this.primitiveLRW.putAll(LRWItems);
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
	
	final OperationStatus store(final BlockHeader blockHeader) throws IOException 
	{
		Objects.requireNonNull(blockHeader, "Block header is null");
		
		if (this.primitiveLRW.containsKey(blockHeader.getHash()) == true)
			return OperationStatus.KEYEXIST;

		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			OperationStatus status = store(transaction, blockHeader.getHash(), blockHeader, Serialization.getInstance().toDson(blockHeader, DsonOutput.Output.PERSIST));
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    		databaseLog.warn(this.context.getName()+": Block header " + blockHeader + " is already present");
		    	else 
		    		throw new DatabaseException("Failed to store " + blockHeader.getHash() + " due to " + status.name());
		    } 
		    
			status = storeSyncInventory(transaction, blockHeader.getHeight(), blockHeader.getHash(), BlockHeader.class);
		    if (status.equals(OperationStatus.SUCCESS) == false) 
	    		throw new DatabaseException("Failed to store block header "+blockHeader.getHash()+" in sync inventory due to "+status.name());

		    transaction.commit();
		    this.primitiveLRW.put(blockHeader.getHash(), blockHeader.getHash());
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

	final OperationStatus store(final long height, final Atom atom) throws IOException 
	{
		Objects.requireNonNull(atom, "Atom is null");
		Numbers.isNegative(height, "Height is negative");
		
		if (this.primitiveLRW.containsKey(atom.getHash()) == true)
			return OperationStatus.KEYEXIST;

		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			OperationStatus status = store(transaction, atom.getHash(), atom, Serialization.getInstance().toDson(atom, DsonOutput.Output.PERSIST));
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    	{
		    		databaseLog.warn(this.context.getName()+": Atom "+atom.getHash()+" is already present");
		    		transaction.abort();
		    		return status;
		    	}
		    	else 
		    		throw new DatabaseException("Failed to store atom "+atom.getHash()+" due to "+status.name());
		    } 

			status = storeSyncInventory(transaction, height, atom.getHash(), Atom.class);
		    if (status.equals(OperationStatus.SUCCESS) == false) 
	    		throw new DatabaseException("Failed to store atom "+atom.getHash()+" in sync inventory due to "+status.name());

		    transaction.commit();
		    this.primitiveLRW.put(atom.getHash(), atom.getHash());
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

	final OperationStatus store(final StateInputs stateInputs) throws IOException 
	{
		Objects.requireNonNull(stateInputs, "State inputs is null");
		
		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			OperationStatus status = store(transaction, stateInputs.getHash(), stateInputs, Serialization.getInstance().toDson(stateInputs, DsonOutput.Output.PERSIST));
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    	{
		    		databaseLog.warn(this.context.getName()+": State inputs for atom "+stateInputs.getAtom()+" in block "+stateInputs.getBlock()+" are already present");
		    		transaction.abort();
		    		return status;
		    	}
		    	else 
		    		throw new DatabaseException("Failed to store state inputs for atom "+stateInputs.getAtom()+" in block "+stateInputs.getBlock()+" due to "+status.name());
		    } 

			status = storeSyncInventory(transaction, Longs.fromByteArray(stateInputs.getBlock().toByteArray()), stateInputs.getHash(), StateInputs.class);
		    if (status.equals(OperationStatus.SUCCESS) == false) 
	    		throw new DatabaseException("Failed to store state inputs for "+stateInputs.getAtom()+" in block "+stateInputs.getBlock()+" in sync inventory due to "+status.name());

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

	private OperationStatus store(final Transaction transaction, final Hash hash, final Primitive primitive, final byte[] bytes) throws IOException 
	{
		Objects.requireNonNull(hash, "Hash is null");
		Hash.notZero(hash, "Hash is ZERO");
		Objects.requireNonNull(primitive, "Primitive is null");
		Objects.requireNonNull(bytes, "Bytes is null");
		Numbers.isZero(bytes.length, "Bytes is empty");
		
		DatabaseEntry key = new DatabaseEntry(hash.toByteArray());
		DatabaseEntry value = new DatabaseEntry(bytes);
		OperationStatus status = this.primitives.putNoOverwrite(transaction, key, value);
		return status;
	}
	
	final OperationStatus store(final long height, final AtomVote vote) throws IOException 
	{
		Objects.requireNonNull(vote, "Vote is null");
		Numbers.isNegative(height, "Height is negative");
		
		if (this.primitiveLRW.containsKey(vote.getHash()) == true)
			return OperationStatus.KEYEXIST;

		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			OperationStatus status = store(transaction, vote.getHash(), vote, Serialization.getInstance().toDson(vote, DsonOutput.Output.PERSIST));
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    	{
		    		databaseLog.warn(this.context.getName()+": Atom pool votes "+vote.getHash()+" is already present");
		    		transaction.abort();
		    		return status;
		    	}
		    	else 
		    		throw new DatabaseException("Failed to store atom pool votes "+vote.getHash()+" due to "+status.name());
		    } 
		    
			status = storeSyncInventory(transaction, height, vote.getHash(), AtomVote.class);
		    if (status.equals(OperationStatus.SUCCESS) == false) 
	    		throw new DatabaseException("Failed to store atom vote "+vote.getHash()+" in sync inventory due to "+status.name());

		    transaction.commit();
		    this.primitiveLRW.put(vote.getHash(), vote.getHash());
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

	final OperationStatus store(final BlockVote vote) throws IOException 
	{
		Objects.requireNonNull(vote, "Vote is null");
		
		if (this.primitiveLRW.containsKey(vote.getHash()) == true)
			return OperationStatus.KEYEXIST;

		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			OperationStatus status = store(transaction, vote.getHash(), vote, Serialization.getInstance().toDson(vote, DsonOutput.Output.PERSIST));
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    	{
		    		databaseLog.warn(this.context.getName()+": Block vote "+vote.getHash()+" is already present");
		    		transaction.abort();
		    		return status;
		    	}
		    	else 
		    		throw new DatabaseException("Failed to store block votes "+vote.getHash()+" due to "+status.name());
		    } 

			status = storeSyncInventory(transaction, vote.getHeight(), vote.getHash(), BlockVote.class);
		    if (status.equals(OperationStatus.SUCCESS) == false) 
	    		throw new DatabaseException("Failed to store block vote "+vote.getHash()+" in sync inventory due to "+status.name());

		    transaction.commit();
		    this.primitiveLRW.put(vote.getHash(), vote.getHash());
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
	
	final OperationStatus store(final StateVote vote) throws IOException 
	{
		Objects.requireNonNull(vote, "Vote is null");
		
		if (this.primitiveLRW.containsKey(vote.getHash()) == true)
			return OperationStatus.KEYEXIST;

		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			OperationStatus status = store(transaction, vote.getHash(), vote, Serialization.getInstance().toDson(vote, DsonOutput.Output.PERSIST));
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    	{
		    		databaseLog.warn(this.context.getName()+": State vote "+vote.getHash()+" is already present");
		    		transaction.abort();
		    		return status;
		    	}
		    	else 
		    		throw new DatabaseException("Failed to store state vote "+vote.getHash()+" due to "+status.name());
		    } 

			status = storeSyncInventory(transaction, vote.getHeight(), vote.getHash(), StateVote.class);
		    if (status.equals(OperationStatus.SUCCESS) == false) 
	    		throw new DatabaseException("Failed to store state vote "+vote.getHash()+" in sync inventory due to "+status.name());

		    transaction.commit();
		    this.primitiveLRW.put(vote.getHash(), vote.getHash());
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

	final OperationStatus store(final StateCertificate certificate) throws IOException 
	{
		Objects.requireNonNull(certificate, "State certificate is null");
		
		if (this.primitiveLRW.containsKey(certificate.getHash()) == true)
			return OperationStatus.KEYEXIST;
		
		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			OperationStatus status = store(transaction, certificate.getHash(), certificate, Serialization.getInstance().toDson(certificate, DsonOutput.Output.PERSIST));
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    	{
		    		databaseLog.warn(this.context.getName()+": State certificate "+certificate.getHash()+" is already present");
		    		transaction.abort();
		    		return status;
		    	}
		    	else 
		    		throw new DatabaseException("Failed to store state certificate "+certificate.getHash()+" due to "+status.name());
		    } 

			status = storeSyncInventory(transaction, certificate.getHeight(), certificate.getHash(), StateCertificate.class);
		    if (status.equals(OperationStatus.SUCCESS) == false) 
	    		throw new DatabaseException("Failed to store state certificate "+certificate.getHash()+" in sync inventory due to "+status.name());
		    
		    transaction.commit();
		    this.primitiveLRW.put(certificate.getHash(), certificate.getHash());
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

	final OperationStatus store(final long height, final AtomCertificate certificate) throws IOException 
	{
		Objects.requireNonNull(certificate, "Certificate is null");
		Numbers.isNegative(height, "Height is negative");

		if (this.primitiveLRW.containsKey(certificate.getHash()) == true)
			return OperationStatus.KEYEXIST;

		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			OperationStatus status = store(transaction, certificate.getHash(), certificate, Serialization.getInstance().toDson(certificate, DsonOutput.Output.PERSIST));
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    	{
		    		databaseLog.warn(this.context.getName()+": Certificate "+certificate.getHash()+" is already present");
		    		transaction.abort();
		    		return status;
		    	}
		    	else 
		    		throw new DatabaseException("Failed to store certificate "+certificate.getHash()+" due to "+status.name());
		    } 
		    
			status = storeSyncInventory(transaction, height, certificate.getHash(), AtomCertificate.class);
		    if (status.equals(OperationStatus.SUCCESS) == false) 
	    		throw new DatabaseException("Failed to store atom certificate "+certificate.getHash()+" in sync inventory due to "+status.name());

		    transaction.commit();
		    this.primitiveLRW.put(certificate.getHash(), certificate.getHash());
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

	// STATE //
	final Commit search(final StateKey<?, ?> key) throws IOException
	{
		Objects.requireNonNull(key, "State key is null");

		try
        {
			DatabaseEntry stateKey = new DatabaseEntry(key.get().toByteArray());
			DatabaseEntry stateCommitValue = new DatabaseEntry();
			OperationStatus status = this.stateCommits.get(null, stateKey, stateCommitValue, LockMode.DEFAULT);
			if (status.equals(OperationStatus.SUCCESS) == true)
				return Commit.from(stateCommitValue.getData()); //Serialization.getInstance().fromDson(stateCommitValue.getData(), Commit.class);
			
			return null;
        }
		catch (Exception ex)
		{
			if (ex instanceof DatabaseException)
				throw ex;
			else
				throw new DatabaseException(ex);
		}
	}
	
	@Override
	public final CommitStatus has(final StateKey<?, ?> key) throws IOException
	{
		Objects.requireNonNull(key, "State key is null");

		try
        {
			DatabaseEntry stateKey = new DatabaseEntry(key.get().toByteArray());
			OperationStatus status = this.stateOperations.get(null, stateKey, null, LockMode.DEFAULT);
			if (status.equals(OperationStatus.SUCCESS) == true)
				return CommitStatus.COMMITTED;
			
			return CommitStatus.NONE;
        }
		catch (Exception ex)
		{
			if (ex instanceof DatabaseException)
				throw ex;
			else
				throw new DatabaseException(ex);
		}
	}
	
	@Override
	public UInt256 get(final StateKey<?, ?> key) throws IOException
	{
		Objects.requireNonNull(key, "State key is null");

		try
        {
			DatabaseEntry stateKey = new DatabaseEntry(key.get().toByteArray());
			DatabaseEntry stateOpValue = new DatabaseEntry();
			OperationStatus status = this.stateOperations.get(null, stateKey, stateOpValue, LockMode.DEFAULT);
			if (status.equals(OperationStatus.SUCCESS) == true)
			{
				StateOp stateOp = StateOp.from(stateOpValue.getData()); //Serialization.getInstance().fromDson(stateOpValue.getData(), StateOp.class);
				return stateOp.value();
			}
			
			return null;
        }
		catch (Exception ex)
		{
			if (ex instanceof DatabaseException)
				throw ex;
			else
				throw new DatabaseException(ex);
		}
	}

	final OperationStatus commit(final Block block) throws IOException
	{
	    Objects.requireNonNull(block, "Block to commit is null");
	    
//    	if (block.getHash().equals(Universe.getDefault().getGenesis().getHash()) == false && block.getHeader().getCertificate() == null)
//	    	throw new IllegalStateException(this.context.getName()+": Block "+block.getHash()+" does not have a certificate");
		
	    Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
	    try 
	    {	
	    	OperationStatus status;
			Map<Hash, Hash> LRWItems = new HashMap<Hash, Hash>();

	    	if (block.getHash().equals(Universe.getDefault().getGenesis().getHash()) == false)
	    	{
	    		StateAddress prevBlockStateAddress = new StateAddress(Block.class, block.getHeader().getPrevious());
				DatabaseEntry prevBlockStateAddressKey = new DatabaseEntry(prevBlockStateAddress.get().toByteArray());
				status = this.stateCommits.get(transaction, prevBlockStateAddressKey, null, LockMode.DEFAULT);
				if (status.equals(OperationStatus.SUCCESS) == false)
					throw new IllegalStateException("Previous block "+block.getHeader().getPrevious()+" not found in state for "+block.getHash());
	    	}
			
	    	StateAddress blockStateAddress = new StateAddress(Block.class, block.getHeader().getHash());
    		Path blockPath = new Path(blockStateAddress.get(), ImmutableMap.of(Elements.BLOCK, block.getHeader().getHash()));
    		Commit blockCommit = new Commit(block.getHeader().getIndex(), blockPath, Collections.emptyList(), block.getHeader().getTimestamp(), false);

    		DatabaseEntry blockStateAddressKey = new DatabaseEntry(blockStateAddress.get().toByteArray());
    		DatabaseEntry blockCommitValue = new DatabaseEntry(blockCommit.toByteArray()); //Serialization.getInstance().toDson(blockCommit, Output.PERSIST));
			status = this.stateCommits.putNoOverwrite(transaction, blockStateAddressKey, blockCommitValue);
			if (status.equals(OperationStatus.SUCCESS) != true)
			{
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    	{
		    		databaseLog.warn(this.context.getName()+": Block "+block.getHash()+" is already committed");
		    		transaction.abort();
		    		return status;
		    	}
		    	else 
		    		throw new DatabaseException("Failed to commit block "+block.getHash()+" due to "+status.name());
			}
			
			// Update primitives
			DatabaseEntry blockHeaderKey = new DatabaseEntry(block.getHash().toByteArray());
			DatabaseEntry blockHeaderValue = new DatabaseEntry(Serialization.getInstance().toDson(block.getHeader(), DsonOutput.Output.PERSIST));
			status = this.primitives.put(transaction, blockHeaderKey, blockHeaderValue);
		    if (status.equals(OperationStatus.SUCCESS) != true) 
	    		throw new DatabaseException("Failed to commit block header "+block.getHash()+" due to " + status.name());
		    
			LRWItems.put(block.getHash(), block.getHash());

		    for (Atom atom : block.getAtoms())
		    {
				if (this.primitiveLRW.containsKey(atom.getHash()) == false)
				{
					DatabaseEntry atomKey = new DatabaseEntry(atom.getHash().toByteArray());
					DatabaseEntry atomValue = new DatabaseEntry(Serialization.getInstance().toDson(atom, DsonOutput.Output.PERSIST));
					status = this.primitives.put(transaction, atomKey, atomValue);
				    if (status.equals(OperationStatus.SUCCESS) != true) 
			    		throw new DatabaseException("Failed to commit block atom "+atom.getHash()+" in block "+block.getHash()+" due to " + status.name());

				    LRWItems.put(atom.getHash(), atom.getHash());
				}
		    }
		    
		    for (AtomCertificate certificate : block.getCertificates())
		    {
				if (this.primitiveLRW.containsKey(certificate.getHash()) == false)
				{
					DatabaseEntry certificateKey = new DatabaseEntry(certificate.getHash().toByteArray());
					DatabaseEntry certificateValue = new DatabaseEntry(Serialization.getInstance().toDson(certificate, DsonOutput.Output.PERSIST));
					status = this.primitives.put(transaction, certificateKey, certificateValue);
				    if (status.equals(OperationStatus.SUCCESS) != true) 
			    		throw new DatabaseException("Failed to commit block certificate "+certificate.getHash()+" in block "+block.getHash()+" due to " + status.name());

				    LRWItems.put(certificate.getHash(), certificate.getHash());
				}
		    }

		    // Atom indexable commit
		    // Prevents atoms from being thrash included in multiple blocks 
		    for (Atom atom : block.getAtoms())
		    {
		    	StateAddress	atomStateAddress = new StateAddress(Atom.class, atom.getHash());
		    	Path 			atomPath = new Path(atomStateAddress.get(), ImmutableMap.of(Elements.BLOCK, block.getHeader().getHash(), Elements.ATOM, atom.getHash()));
		    	Commit			atomCommit = new Commit(block.getHeader().getIndexOf(InventoryType.ATOMS, atom.getHash()), atomPath, Collections.emptyList(), block.getHeader().getTimestamp(), false);
	    		DatabaseEntry 	atomStateAddressKey = new DatabaseEntry(atomStateAddress.get().toByteArray());
	    		DatabaseEntry 	atomCommitValue = new DatabaseEntry(atomCommit.toByteArray()); //Serialization.getInstance().toDson(atomCommit, Output.PERSIST));
				status = this.stateCommits.putNoOverwrite(transaction, atomStateAddressKey, atomCommitValue);
			    if (status.equals(OperationStatus.SUCCESS) != true) 
		    		throw new DatabaseException("Failed to commit atom state "+atom.getHash()+" in block "+block.getHash()+" due to " + status.name());
				else if (databaseLog.hasLevel(Logging.DEBUG) == true)
					databaseLog.debug(this.context.getName()+": Stored atom state "+atomStateAddress);
		    }
		    
		    // Update the atom indexable commit with the certificate and store the certificate indexable
		    for (AtomCertificate certificate : block.getCertificates())
		    {
		    	StateAddress	atomStateAddress = new StateAddress(Atom.class, certificate.getAtom());
		    	Commit			atomCommit = null;
	    		DatabaseEntry 	atomStateAddressKey = new DatabaseEntry(atomStateAddress.get().toByteArray());
	    		DatabaseEntry 	atomCommitValue = new DatabaseEntry();

	    		status = this.stateCommits.get(transaction, atomStateAddressKey, atomCommitValue, LockMode.DEFAULT);
			    if (status.equals(OperationStatus.SUCCESS) != true) 
		    		throw new DatabaseException("Failed to commit atom certificate "+certificate.getHash()+" for atom "+certificate.getAtom()+" in block "+block.getHash()+" due to " + status.name());
			    
			    atomCommit = Commit.from(atomCommitValue.getData()); // Serialization.getInstance().fromDson(atomCommitValue.getData(), Commit.class);
			    atomCommit.getPath().add(Elements.CERTIFICATE, certificate.getHash());
				atomCommitValue = new DatabaseEntry(atomCommit.toByteArray()); //Serialization.getInstance().toDson(atomCommit, Output.ALL));
		    	
				StateAddress	certificateStateAddress = new StateAddress(AtomCertificate.class, certificate.getHash());
		    	Path 			certificatePath = new Path(certificateStateAddress.get(), ImmutableMap.of(Elements.BLOCK, block.getHeader().getHash()));
		    	Commit			certificateCommit = new Commit(block.getHeader().getIndexOf(InventoryType.CERTIFICATES, certificate.getHash()), certificatePath, Collections.emptyList(), block.getHeader().getTimestamp(), false);
		    	DatabaseEntry 	certificateStateAddressKey = new DatabaseEntry(certificateStateAddress.get().toByteArray());
				DatabaseEntry 	certificateCommitValue = new DatabaseEntry(certificateCommit.toByteArray()); //Serialization.getInstance().toDson(certificateCommit, Output.ALL));
				
				status = this.stateCommits.put(transaction, atomStateAddressKey, atomCommitValue);
			    if (status.equals(OperationStatus.SUCCESS) != true) 
		    		throw new DatabaseException("Failed to commit atom certificate "+certificate.getHash()+" for atom "+certificate.getAtom()+" in block "+block.getHash()+" due to " + status.name());

			    status = this.stateCommits.put(transaction, certificateStateAddressKey, certificateCommitValue);
			    if (status.equals(OperationStatus.SUCCESS) != true) 
		    		throw new DatabaseException("Failed to commit atom certificate "+certificate.getHash()+" for atom "+certificate.getAtom()+" in block "+block.getHash()+" due to " + status.name());
		    }

		    DatabaseEntry syncKey = new DatabaseEntry(Longs.toByteArray(block.getHeader().getHeight()));
			DatabaseEntry syncValue = new DatabaseEntry(block.getHeader().getHash().toByteArray());
			status = this.syncChain.putNoOverwrite(transaction, syncKey, syncValue);
		    if (status.equals(OperationStatus.SUCCESS) != true) 
	    		throw new DatabaseException("Failed to commit to sync chain "+block.getHash()+" due to " + status.name());

	    	transaction.commit();
	    	this.primitiveLRW.putAll(LRWItems);
	    	return status;
	    } 
	    catch (Exception ex) 
	    {
			databaseLog.error(this.context.getName()+": Block commit aborting", ex);
	    	transaction.abort();
	    	if (ex instanceof DatabaseException)
	    		throw ex; 
	    	throw new DatabaseException(ex);
	    } 
	}
	
	final OperationStatus timedOut(final Hash atom) throws IOException
	{
	    Objects.requireNonNull(atom, "Atom to timeout is null");

	    Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
	    try 
	    {	
	    	OperationStatus status;
	    	StateAddress	atomStateAddress = new StateAddress(Atom.class, atom);
    		DatabaseEntry 	atomStateAddressKey = new DatabaseEntry(atomStateAddress.get().toByteArray());
    		DatabaseEntry 	atomCommitValue = new DatabaseEntry();
    		status = this.stateCommits.get(transaction, atomStateAddressKey, atomCommitValue, LockMode.RMW);
		    if (status.equals(OperationStatus.SUCCESS) != true) 
	    		throw new DatabaseException("Expected atom commit state for "+atom+" but failed due to " + status.name());
	    	
		    Commit atomCommit = Commit.from(atomCommitValue.getData());
		    atomCommit.setTimedOut();
    		atomCommitValue.setData(atomCommit.toByteArray());
			status = this.stateCommits.put(transaction, atomStateAddressKey, atomCommitValue);
		    if (status.equals(OperationStatus.SUCCESS) != true) 
	    		throw new DatabaseException("Failed to update atom state for "+atom+" in block "+atomCommit.getPath().get(Elements.BLOCK)+" due to " + status.name());
			else if (databaseLog.hasLevel(Logging.DEBUG) == true)
				databaseLog.debug(this.context.getName()+": Updated atom state for time out "+atomStateAddress);
	    	transaction.commit();
	    	return status;
	    } 
	    catch (Exception ex) 
	    {
			databaseLog.error(this.context.getName()+": Atom state timedout commit aborting for "+atom, ex);
	    	transaction.abort();
	    	if (ex instanceof DatabaseException)
	    		throw ex; 
	    	throw new DatabaseException(ex);
	    } 
	}

	final void commit(final Collection<CommitOperation> commits) throws IOException
	{
	    Objects.requireNonNull(commits, "Commit operations is null");
	    Numbers.isZero(commits.size(), "Commit operations is empty");
		
	    Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
	    try 
	    {
    		OperationStatus status;
    		
    		for (CommitOperation commit : commits)
    		{
				final DatabaseEntry atomKey = new DatabaseEntry(commit.getAtom().getHash().toByteArray());
	
				if (1==0)
	    		{
			    	final StateAddress	atomStateAddress = new StateAddress(Atom.class, commit.getAtom().getHash());
		    		final DatabaseEntry atomStateAddressKey = new DatabaseEntry(atomStateAddress.get().toByteArray());
		    		final DatabaseEntry atomCommitValue = new DatabaseEntry();
			    		
		    		status = this.stateCommits.get(transaction, atomStateAddressKey, atomCommitValue, LockMode.DEFAULT);
		    		if (status.equals(OperationStatus.SUCCESS) == false)
		   				throw new DatabaseException("Atom state commit "+commit.getAtom().getHash()+" not found or has error "+status.name());
			    		
		    		final Commit atomCommit = Commit.from(atomCommitValue.getData()); //Serialization.getInstance().fromDson(atomCommitValue.getData(), Commit.class);
					if (atomCommit.getPath().get(Elements.BLOCK).equals(commit.getHead().getHash()) == false)
						throw new DatabaseException("Atom state commit "+commit.getAtom().getHash()+" references block "+atomCommit.getPath().get(Elements.BLOCK)+" not expected "+commit.getHead().getHash());
	
					final StateAddress	blockStateAddress = new StateAddress(Block.class, commit.getHead().getHash());
					final DatabaseEntry blockStateAddressKey = new DatabaseEntry(blockStateAddress.get().toByteArray());
	
					if (commit.getHead().getHash().equals(Universe.getDefault().getGenesis().getHash()) == false)
					{
			    		status = this.stateCommits.get(transaction, blockStateAddressKey, null, LockMode.DEFAULT);
			    		if (status.equals(OperationStatus.SUCCESS) == false)
		    				throw new DatabaseException("Expected committed block "+commit.getHead().getHash()+" not found "+status.name());
					}
	    		}
					
				// Atom should exist in primitives
				if (this.primitiveLRW.containsKey(commit.getAtom().getHash()) == false)
				{
		    		status = this.primitives.get(transaction, atomKey, null, LockMode.DEFAULT);
		    		if (status.equals(OperationStatus.SUCCESS) == false)
						throw new DatabaseException("Atom "+commit.getAtom().getHash()+" not found or has error "+status.name());
				}
	    		
				// Update atom in primitives as it should now be carrying state values at the time of the commit
				DatabaseEntry atomValue = new DatabaseEntry(Serialization.getInstance().toDson(commit.getAtom(), DsonOutput.Output.PERSIST));
				status = this.primitives.put(transaction, atomKey, atomValue);
			    if (status.equals(OperationStatus.SUCCESS) != true) 
		    		throw new DatabaseException("Failed to commit atom "+commit.getAtom().getHash()+" with state objects " + status.name());
	    		
				for (StateOp stateOp : commit.getStateOps())
				{
					if (stateOp.ins().equals(Instruction.SET) == false)
						continue;
					
	        		long shardGroup = ShardMapper.toShardGroup(stateOp.key().get(), this.context.getLedger().numShardGroups(commit.getHead().getHeight()));
					DatabaseEntry stateOpKey = new DatabaseEntry(stateOp.key().get().toByteArray());
					DatabaseEntry stateOpValue = new DatabaseEntry(stateOp.toByteArray()); //Serialization.getInstance().toDson(stateOp, Output.PERSIST));
					status = this.stateOperations.put(transaction, stateOpKey, stateOpValue);
					if (status.equals(OperationStatus.SUCCESS) == false)
						throw new DatabaseException("Failed to commit state "+stateOp+" for commit "+commit.getHead().getHeight()+":"+commit.getAtom().getHash()+" due to "+status.name()); 
					else if (databaseLog.hasLevel(Logging.DEBUG) == true)
						databaseLog.debug(this.context.getName()+": Stored state op "+stateOp+" in shard group "+shardGroup);
				}
	
				for (Path path : commit.getStatePaths())
				{
					path.validate();
	
	        		long shardGroup = ShardMapper.toShardGroup(path.endpoint(), this.context.getLedger().numShardGroups(commit.getHead().getHeight()));
	        		Commit stateCommit = new Commit(commit.getHead().getIndexOf(InventoryType.ATOMS, commit.getAtom().getHash()), path, Collections.emptyList(), commit.getTimestamp(), false);
					DatabaseEntry stateKey = new DatabaseEntry(path.endpoint().toByteArray());
					DatabaseEntry stateCommitValue = new DatabaseEntry(stateCommit.toByteArray()); //Serialization.getInstance().toDson(stateCommit, Output.PERSIST));
					status = this.stateCommits.put(transaction, stateKey, stateCommitValue);
					if (status.equals(OperationStatus.SUCCESS) == false)
						throw new DatabaseException("Failed to commit state "+path+" for commit "+commit.getHead().getHeight()+":"+commit.getAtom().getHash()+" due to "+status.name()); 
					else if (databaseLog.hasLevel(Logging.DEBUG) == true)
						databaseLog.debug(this.context.getName()+": Stored state path in shard group "+shardGroup+" "+path);
				}
				
		    	for (Path path : commit.getAssociationPaths()) 
		    	{
		    		path.validate();
	
	        		Commit associationCommit = new Commit(commit.getHead().getIndexOf(InventoryType.ATOMS, commit.getAtom().getHash()), path, Collections.emptyList(), commit.getTimestamp(), false);
	        		DatabaseEntry associationKey = new DatabaseEntry(path.endpoint().toByteArray());
					DatabaseEntry associationCommitValue = new DatabaseEntry(associationCommit.toByteArray()); //Serialization.getInstance().toDson(associationCommit, Output.PERSIST));
	
					status = this.stateAssociations.put(transaction, associationKey, associationCommitValue);
		    		if (status.equals(OperationStatus.SUCCESS) == false)
						throw new DatabaseException("Failed to commit association "+path.endpoint()+" for commit "+commit.getHead().getHeight()+":"+commit.getAtom().getHash()+" due to "+status.name()); 
		    	}
    		}

	    	transaction.commit();
	    } 
	    catch (Exception ex) 
	    {
			databaseLog.error(this.context.getName()+": State commit of "+commits.size()+" operations aborting", ex);
	    	transaction.abort();
	    	if (ex instanceof DatabaseException)
	    		throw ex; 
	    	throw new DatabaseException(ex);
	    } 
	}
	
	// SYNC //
	boolean has(final long height) throws DatabaseException
	{
		Numbers.isNegative(height, "Height is negative");
		
		try
        {
			OperationStatus status = this.syncChain.get(null, new DatabaseEntry(Longs.toByteArray(height)), null, LockMode.DEFAULT);
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

	Hash head() throws DatabaseException
	{
		try(Cursor cursor = this.syncChain.openCursor(null, null))
		{
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry value = new DatabaseEntry();
			OperationStatus status = cursor.getLast(key, value, LockMode.DEFAULT);
			if (status.equals(OperationStatus.SUCCESS) == true)
				return new Hash(value.getData());

			return Universe.getDefault().getGenesis().getHash();
        }
		catch (Throwable t)
		{
			if (t instanceof DatabaseException)
				throw (DatabaseException)t;
			else
				throw new DatabaseException(t);
		}
	}

	// SYNC //
	Hash getSyncBlock(final long height) throws DatabaseException
	{
		Numbers.isNegative(height, "Height is negative");
		
		try
        {
			DatabaseEntry blockHash = new DatabaseEntry();
			OperationStatus status = this.syncChain.get(null, new DatabaseEntry(Longs.toByteArray(height)), blockHash, LockMode.DEFAULT);
			if (status.equals(OperationStatus.SUCCESS) == true)
				return new Hash(blockHash.getData());

			return null;
        }
		catch (Throwable t)
		{
			if (t instanceof DatabaseException)
				throw (DatabaseException)t;
			else
				throw new DatabaseException(t);
		}
	}

	Collection<Hash> getSyncInventory(final long height, final Class<? extends Primitive> type) throws DatabaseException
	{
		Objects.requireNonNull(type, "Type is null");
		Numbers.isNegative(height, "Height is negative");
		
		List<Hash> items = new ArrayList<Hash>();
		try(Cursor cursor = this.syncCache.openCursor(null, null))
        {
			DatabaseEntry key = new DatabaseEntry(Longs.toByteArray(height));
			DatabaseEntry value = new DatabaseEntry();
			OperationStatus status = cursor.getSearchKey(key, value, LockMode.DEFAULT);
			while(status.equals(OperationStatus.SUCCESS) == true)
			{
				String typeName = new String(value.getData(), Hash.BYTES, value.getSize() - Hash.BYTES);
				if (Serialization.getInstance().getClassForId(typeName).equals(type) == true)
					items.add(new Hash(value.getData(), 0, Hash.BYTES));
				
				status = cursor.getNextDup(key, value, LockMode.DEFAULT);
			}
        }
		catch (Throwable t)
		{
			if (t instanceof DatabaseException)
				throw (DatabaseException)t;
			else
				throw new DatabaseException(t);
		}

		return items;
	}
	
	private OperationStatus storeSyncInventory(final Transaction transaction, final long height, final Hash item, final Class<? extends Primitive> type)
	{
		Objects.requireNonNull(transaction, "Transaction is null");
		Objects.requireNonNull(item, "Item hash is null");
		Objects.requireNonNull(type, "Type is null");
		Numbers.isNegative(height, "Height is negative");
		Hash.notZero(item, "item hash is ZERO");
		
		DatabaseEntry key = new DatabaseEntry(Longs.toByteArray(height));
		DatabaseEntry value = new DatabaseEntry(Arrays.concatenate(item.toByteArray(), Serialization.getInstance().getIdForClass(type).getBytes(StandardCharsets.UTF_8)));
		OperationStatus status = this.syncCache.put(transaction, key, value);
		return status;
	}

	// IDENTIFIER SEARCH //
	final Collection<Commit> search(final AssociationSearchQuery query) throws IOException
	{
		Objects.requireNonNull(query, "Search query is null");
		
		Cursor searchCursor = this.stateAssociations.openCursor(null, null);
		try
		{
			long nextOffset = -1;
			List<Commit> commits = new ArrayList<Commit>();
			DatabaseEntry key = new DatabaseEntry(query.getAssociations().get(0).toByteArray());
			DatabaseEntry value = new DatabaseEntry();
			OperationStatus status = searchCursor.getSearchKey(key, value, LockMode.DEFAULT);
			if (status.equals(OperationStatus.SUCCESS) && query.getOrder().equals(Order.DESCENDING) == true)
			{
				status = searchCursor.getNextNoDup(key, value, LockMode.DEFAULT);
				if (status.equals(OperationStatus.SUCCESS))
					status = searchCursor.getPrev(key, value, LockMode.DEFAULT);
				else
					status = searchCursor.getLast(key, value, LockMode.DEFAULT);
			}
			
			if (status.equals(OperationStatus.SUCCESS) == true)
			{
				nextOffset = Commit.from(value.getData()).getIndex(); //Serialization.getInstance().fromDson(value.getData(), Commit.class).getIndex();
				while(status.equals(OperationStatus.SUCCESS) == true && query.getOffset() > -1 && 
					  ((query.getOrder().equals(Order.ASCENDING) == true && nextOffset <= query.getOffset()) ||
					   (query.getOrder().equals(Order.DESCENDING) == true && nextOffset >= query.getOffset())))
				{
					if (query.getOrder().equals(Order.DESCENDING) == true)
						status = searchCursor.getPrevDup(key, value, LockMode.DEFAULT);
					else
						status = searchCursor.getNextDup(key, value, LockMode.DEFAULT);
	
					nextOffset = Commit.from(value.getData()).getIndex(); //Serialization.getInstance().fromDson(value.getData(), Commit.class).getIndex();
				}
	
				while(status.equals(OperationStatus.SUCCESS) == true && commits.size() < query.getLimit())
				{
					if (query.getOrder().equals(Order.ASCENDING) == true && nextOffset > query.getOffset() ||
					    query.getOrder().equals(Order.DESCENDING) == true && nextOffset < query.getOffset())
					{
						Commit commit = Commit.from(value.getData()); //Serialization.getInstance().fromDson(value.getData(), Commit.class);
						commits.add(commit);
					}

					if (query.getOrder().equals(Order.DESCENDING) == true)
						status = searchCursor.getPrevDup(key, value, LockMode.DEFAULT);
					else
						status = searchCursor.getNextDup(key, value, LockMode.DEFAULT);
				}
			}
			
//			AssociationSearchResponse<Commit> response = new AssociationSearchResponse<Commit>(query, nextOffset, commits, status.equals(OperationStatus.NOTFOUND));
			return commits;
		}
		catch (Throwable t)
		{
			if (t instanceof DatabaseException)
				throw (DatabaseException)t;
			else
				throw new DatabaseException(t);
		}
		finally
		{
			searchCursor.close();
		}
	}
	
	// PRIMITIVE CURSORS
	Hash getNext(final Hash hash, final Class<? extends Primitive> primitive) throws DatabaseException
	{
		Objects.requireNonNull(hash, "Hash is null");
		Objects.requireNonNull(primitive, "Primitive is null");
		
		try(Cursor cursor = this.primitives.openCursor(null, null))
        {
			DatabaseEntry key = new DatabaseEntry(hash.toByteArray());

			OperationStatus status = cursor.getSearchKey(key, null, LockMode.DEFAULT);
			if (status.equals(OperationStatus.NOTFOUND) == true)
				return null;

			status = cursor.getNextNoDup(key, null, LockMode.DEFAULT);
			if (status.equals(OperationStatus.NOTFOUND) == true)
				return null;

			return new Hash(key.getData()); 
        }
		catch (Throwable t)
		{
			if (t instanceof DatabaseException)
				throw (DatabaseException)t;
			else
				throw new DatabaseException(t);
		}
	}

	Hash getPrev(final Hash hash, final Class<? extends Primitive> primitive) throws DatabaseException
	{
		Objects.requireNonNull(hash, "Hash is null");
		Objects.requireNonNull(primitive, "Primitive is null");

		try(Cursor cursor = this.primitives.openCursor(null, null))
        {
			DatabaseEntry key = new DatabaseEntry(hash.toByteArray());

			OperationStatus status = cursor.getSearchKey(key, null, LockMode.DEFAULT);
			if (status.equals(OperationStatus.NOTFOUND) == true)
				return null;

			status = cursor.getPrevNoDup(key, null, LockMode.DEFAULT);
			if (status.equals(OperationStatus.NOTFOUND) == true)
				return null;

			return new Hash(key.getData()); 
        }
		catch (Throwable t)
		{
			if (t instanceof DatabaseException)
				throw (DatabaseException)t;
			else
				throw new DatabaseException(t);
		}
	}
}
