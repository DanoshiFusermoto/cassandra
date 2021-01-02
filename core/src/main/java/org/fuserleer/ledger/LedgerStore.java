package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.bouncycastle.util.Arrays;
import org.fuserleer.Context;
import org.fuserleer.Universe;
import org.fuserleer.common.Order;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Certificate;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.DatabaseException;
import org.fuserleer.database.DatabaseStore;
import org.fuserleer.database.Identifier;
import org.fuserleer.database.Indexable;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.IndexableCommit.Path;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializationException;

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
	private Database state;
	private Database stateIndexables;
	private Database stateIdentifiers;
	private Database syncChain;

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
			
			DatabaseConfig syncConfig = new DatabaseConfig();
			syncConfig.setAllowCreate(true);
			syncConfig.setTransactional(true);
			syncConfig.setKeyPrefixing(true);

			DatabaseConfig stateConfig = new DatabaseConfig();
			stateConfig.setAllowCreate(true);
			stateConfig.setTransactional(true);
			stateConfig.setKeyPrefixing(true);

			DatabaseConfig stateIndexablesConfig = new DatabaseConfig();
			stateIndexablesConfig.setAllowCreate(true);
			stateIndexablesConfig.setTransactional(true);
			stateIndexablesConfig.setKeyPrefixing(true);

			DatabaseConfig stateFieldsConfig = new DatabaseConfig();
			stateFieldsConfig.setAllowCreate(true);
			stateFieldsConfig.setTransactional(true);
			stateFieldsConfig.setKeyPrefixing(true);

			DatabaseConfig stateIdentifiersConfig = new DatabaseConfig();
			stateIdentifiersConfig.setAllowCreate(true);
			stateIdentifiersConfig.setTransactional(true);
			stateIdentifiersConfig.setSortedDuplicates(true);
			stateIdentifiersConfig.setKeyPrefixing(true);

			Transaction transaction = null;
			try
			{
				transaction = getEnvironment().beginTransaction(null, new TransactionConfig().setReadUncommitted(true));

				this.primitives = getEnvironment().openDatabase(transaction, "hackation.primitives", primitivesConfig);
				this.syncChain = getEnvironment().openDatabase(transaction, "hackation.sync.chain", primitivesConfig);
				this.state = getEnvironment().openDatabase(transaction, "hackation.state", stateConfig);
//				this.stateFields = getEnvironment().openDatabase(transaction, "hackation.state.fields", stateFieldsConfig);
				this.stateIndexables = getEnvironment().openDatabase(transaction, "hackation.state.indexables", stateIndexablesConfig);
				this.stateIdentifiers = getEnvironment().openDatabase(transaction, "hackation.state.identifiers", stateIdentifiersConfig);

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

		if (this.stateIdentifiers != null) this.stateIdentifiers.close();
		if (this.stateIndexables != null) this.stateIndexables.close();
//		if (this.stateFields != null) this.stateFields.close();
		if (this.state != null) this.state.close();
		if (this.syncChain != null) this.syncChain.close();
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
			getEnvironment().truncateDatabase(transaction, "hackation.state", false);
			getEnvironment().truncateDatabase(transaction, "hackation.state.indexables", false);
			getEnvironment().truncateDatabase(transaction, "hackation.state.identifiers", false);
//			getEnvironment().truncateDatabase(transaction, "hackation.state.fields", false);

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

	// PRIMITIVES //
	@SuppressWarnings("unchecked")
	public <T extends Primitive> T get(final Hash hash, final Class<T> primitive) throws IOException
	{
		try
        {
			DatabaseEntry key = new DatabaseEntry(hash.toByteArray());
			DatabaseEntry value = new DatabaseEntry();
			
			if (primitive.equals(Atom.class) == true || primitive.equals(BlockHeader.class) == true || 
				Vote.class.isAssignableFrom(primitive) == true  || Certificate.class.isAssignableFrom(primitive) == true)
			{
				OperationStatus status = this.primitives.get(null, key, value, LockMode.DEFAULT);
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

	boolean has(Hash hash) throws DatabaseException
	{
		try
        {
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
	
	final OperationStatus store(Block block) throws IOException 
	{
		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			BlockHeader blockHeader = block.getHeader();
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
		    
		    for (AtomCertificate certificate : block.getCertificates())
		    {
				status = store(transaction, certificate.getHash(), certificate, Serialization.getInstance().toDson(certificate, DsonOutput.Output.PERSIST));
			    if (status.equals(OperationStatus.SUCCESS) == false) 
			    {
			    	if (status.equals(OperationStatus.KEYEXIST) == true) 
			    		databaseLog.warn(this.context.getName()+": Certificate "+certificate.getHash()+" in block "+blockHeader + " is already present");
			    	else 
			    		throw new DatabaseException("Failed to store certificate "+certificate.getHash()+" in block "+blockHeader + " due to " + status.name());
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
		    	{
		    		databaseLog.warn(this.context.getName()+": Atom "+atom.getHash()+" is already present");
		    		transaction.abort();
		    		return status;
		    	}
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
		OperationStatus status = this.primitives.putNoOverwrite(transaction, key, value);
		return status;
	}
	
	final OperationStatus store(AtomVote votes) throws IOException 
	{
		Objects.requireNonNull(votes, "Votes is null");
		
		Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
		try 
		{
			OperationStatus status = store(transaction, votes.getHash(), votes, Serialization.getInstance().toDson(votes, DsonOutput.Output.PERSIST));
		    if (status.equals(OperationStatus.SUCCESS) == false) 
		    {
		    	if (status.equals(OperationStatus.KEYEXIST) == true) 
		    	{
		    		databaseLog.warn(this.context.getName()+": Atom pool votes "+votes.getHash()+" is already present");
		    		transaction.abort();
		    		return status;
		    	}
		    	else 
		    		throw new DatabaseException("Failed to store atom pool votes "+votes.getHash()+" due to "+status.name());
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

	final OperationStatus store(BlockVote vote) throws IOException 
	{
		Objects.requireNonNull(vote, "Vote is null");
		
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
	
	final OperationStatus store(StateVote vote) throws IOException 
	{
		Objects.requireNonNull(vote, "Vote is null");
		
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

	final OperationStatus store(AtomCertificate certificate) throws IOException 
	{
		Objects.requireNonNull(certificate, "Certificate is null");
		
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

	// STATE //
	final IndexableCommit search(final Indexable indexable) throws IOException
	{
		Objects.requireNonNull(indexable);

		try
        {
			DatabaseEntry key = new DatabaseEntry(indexable.toByteArray());
			DatabaseEntry value = new DatabaseEntry();
			OperationStatus status = this.stateIndexables.get(null, key, value, LockMode.DEFAULT);
			if (status.equals(OperationStatus.SUCCESS) == true)
				return Serialization.getInstance().fromDson(value.getData(), IndexableCommit.class);
			
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
	public final CommitState has(final Indexable indexable) throws IOException
	{
		Objects.requireNonNull(indexable);

		try
        {
			DatabaseEntry key = new DatabaseEntry(indexable.toByteArray());
			OperationStatus status = this.stateIndexables.get(null, key, null, LockMode.DEFAULT);
			if (status.equals(OperationStatus.SUCCESS) == true)
				return CommitState.COMMITTED;
			
			return CommitState.NONE;
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
	public StateOpResult<?> evaluate(final StateOp stateOp) throws IOException 
	{
		Objects.requireNonNull(stateOp);
		if (stateOp.ins().equals(Instruction.SET) == true)
			throw new IllegalArgumentException("Evaluation of StateOp.SET is illegal");
		
		try
        {
			DatabaseEntry key = new DatabaseEntry(stateOp.key().toByteArray());
			if (stateOp.ins().equals(Instruction.EXISTS) == true || stateOp.ins().equals(Instruction.NOT_EXISTS) == true)
			{
				OperationStatus status = this.state.get(null, key, null, LockMode.DEFAULT);
				if (status.equals(OperationStatus.SUCCESS) == true)
				{
					if (stateOp.ins().equals(Instruction.EXISTS) == true)
						return new StateOpResult<Boolean>(stateOp, CommitState.COMMITTED);
					else
						return new StateOpResult<Boolean>(stateOp, new ValidationException(stateOp.toString()));  // FIXME DependencyNotFoundException needed here
				}
				else
				{
					if (stateOp.ins().equals(Instruction.NOT_EXISTS) == true)
						return new StateOpResult<Boolean>(stateOp, true, CommitState.NONE);
					else
						return new StateOpResult<Boolean>(stateOp, new ValidationException(stateOp.toString()));
				}
			}
			else if (stateOp.ins().equals(Instruction.EQUAL) == true || stateOp.ins().equals(Instruction.NOT_EQUAL) == true)
			{
				DatabaseEntry value = new DatabaseEntry();
				OperationStatus status = this.state.get(null, key, value, LockMode.DEFAULT);
				if (status.equals(OperationStatus.SUCCESS) == true)
				{
					if (stateOp.ins().equals(Instruction.EQUAL) == true)
					{
						if (Arrays.areEqual(stateOp.value().toByteArray(), value.getData()) == true)
							return new StateOpResult<Boolean>(stateOp, CommitState.COMMITTED);
						else
							return new StateOpResult<Boolean>(stateOp, CommitState.COMMITTED, new ValidationException(stateOp.toString()));
					}
					else
					{
						if (Arrays.areEqual(stateOp.value().toByteArray(), value.getData()) == false)
							return new StateOpResult<Boolean>(stateOp, CommitState.COMMITTED);
						else
							return new StateOpResult<Boolean>(stateOp, CommitState.COMMITTED, new ValidationException(stateOp.toString()));
					}
				}
				else
					return new StateOpResult<Boolean>(stateOp, new ValidationException(stateOp.toString()));
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
	    Objects.requireNonNull(block);
	    
//    	if (block.getHash().equals(Universe.getDefault().getGenesis().getHash()) == false && block.getHeader().getCertificate() == null)
//	    	throw new IllegalStateException(this.context.getName()+": Block "+block.getHash()+" does not have a certificate");
		
	    Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
	    try 
	    {	
	    	OperationStatus status;
	    	
	    	if (block.getHash().equals(Universe.getDefault().getGenesis().getHash()) == false)
	    	{
	    		Indexable prevBlockIndexable = Indexable.from(block.getHeader().getPrevious(), BlockHeader.class);
				DatabaseEntry prevKey = new DatabaseEntry(prevBlockIndexable.toByteArray());
				status = this.stateIndexables.get(transaction, prevKey, null, LockMode.DEFAULT);
				if (status.equals(OperationStatus.SUCCESS) == false)
					throw new IllegalStateException("Previous block "+block.getHeader().getPrevious()+" not found in state for "+block.getHash());
	    	}
			
    		Indexable blockIndexable = Indexable.from(block.getHeader().getHash(), BlockHeader.class);
    		IndexableCommit blockIndexableCommit = new IndexableCommit(block.getHeader().getIndex(), blockIndexable, Collections.emptyList(), block.getHeader().getTimestamp());

    		DatabaseEntry blockIndexableKey = new DatabaseEntry(blockIndexable.toByteArray());
    		DatabaseEntry blockIndexableValue = new DatabaseEntry(Serialization.getInstance().toDson(blockIndexableCommit, Output.PERSIST));
			status = this.stateIndexables.putNoOverwrite(transaction, blockIndexableKey, blockIndexableValue);
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

		    // TODO these should be redundant here, or at least can be made to be 
		    for (Atom atom : block.getAtoms())
		    {
				DatabaseEntry atomKey = new DatabaseEntry(atom.getHash().toByteArray());
				DatabaseEntry atomValue = new DatabaseEntry(Serialization.getInstance().toDson(atom, DsonOutput.Output.PERSIST));
				status = this.primitives.put(transaction, atomKey, atomValue);
			    if (status.equals(OperationStatus.SUCCESS) != true) 
		    		throw new DatabaseException("Failed to commit block atom "+atom.getHash()+" in block "+block.getHash()+" due to " + status.name());
		    }
		    
		    for (AtomCertificate certificate : block.getCertificates())
		    {
				DatabaseEntry certificateKey = new DatabaseEntry(certificate.getHash().toByteArray());
				DatabaseEntry certificateValue = new DatabaseEntry(Serialization.getInstance().toDson(certificate, DsonOutput.Output.PERSIST));
				status = this.primitives.put(transaction, certificateKey, certificateValue);
			    if (status.equals(OperationStatus.SUCCESS) != true) 
		    		throw new DatabaseException("Failed to commit block certificate "+certificate.getHash()+" in block "+block.getHash()+" due to " + status.name());
		    }

		    // Atom indexable commit
		    // Prevents atoms from being thrash included in multiple blocks 
		    for (Atom atom : block.getAtoms())
		    {
		    	Indexable 		atomIndexable = Indexable.from(atom.getHash(), Atom.class);
		    	IndexableCommit atomIndexableCommit = new IndexableCommit(block.getHeader().getIndexOf(InventoryType.ATOMS, atom.getHash()), atomIndexable, Collections.emptyList(), block.getHeader().getTimestamp(), Indexable.from(block.getHeader().getHash(), BlockHeader.class));
	    		DatabaseEntry 	atomIndexableKey = new DatabaseEntry(atomIndexable.toByteArray());
	    		DatabaseEntry 	atomIndexableCommitValue = new DatabaseEntry(Serialization.getInstance().toDson(atomIndexableCommit, Output.PERSIST));
				status = this.stateIndexables.putNoOverwrite(transaction, atomIndexableKey, atomIndexableCommitValue);
			    if (status.equals(OperationStatus.SUCCESS) != true) 
		    		throw new DatabaseException("Failed to commit atom indexable "+atom.getHash()+" in block "+block.getHash()+" due to " + status.name());
				else if (databaseLog.hasLevel(Logging.DEBUG) == true)
					databaseLog.debug(this.context.getName()+": Stored indexable "+atomIndexable);
		    }
		    
		    // Update the atom indexable commit with the certificate and store the certificate indexable
		    for (AtomCertificate certificate : block.getCertificates())
		    {
		    	Indexable 		atomIndexable = Indexable.from(certificate.getAtom(), Atom.class);
		    	IndexableCommit atomIndexableCommit = null;
	    		DatabaseEntry 	atomIndexableKey = new DatabaseEntry(atomIndexable.toByteArray());
	    		DatabaseEntry 	atomIndexableCommitValue = new DatabaseEntry();

	    		status = this.stateIndexables.get(transaction, atomIndexableKey, atomIndexableCommitValue, LockMode.DEFAULT);
			    if (status.equals(OperationStatus.SUCCESS) != true) 
		    		throw new DatabaseException("Failed to commit atom certificate "+certificate.getHash()+" for atom "+certificate.getAtom()+" in block "+block.getHash()+" due to " + status.name());
			    
			    atomIndexableCommit = Serialization.getInstance().fromDson(atomIndexableCommitValue.getData(), IndexableCommit.class);
		    	
		    	Indexable 		certificateIndexable = Indexable.from(certificate.getAtom(), AtomCertificate.class);
		    	IndexableCommit updatedAtomIndexableCommit = new IndexableCommit(atomIndexableCommit.getIndex(), atomIndexable, atomIndexableCommit.getMerkleProofs(), atomIndexableCommit.getTimestamp(), Indexable.from(atomIndexableCommit.get(Path.BLOCK), BlockHeader.class), Indexable.from(certificate.getHash(), Certificate.class));
		    	DatabaseEntry certificateIndexableKey = new DatabaseEntry(certificateIndexable.toByteArray());
				DatabaseEntry certificateIndexableCommitValue = new DatabaseEntry(Serialization.getInstance().toDson(updatedAtomIndexableCommit, Output.ALL));
				
				status = this.stateIndexables.put(transaction, atomIndexableKey, certificateIndexableCommitValue);
			    if (status.equals(OperationStatus.SUCCESS) != true) 
		    		throw new DatabaseException("Failed to commit atom certificate "+certificate.getHash()+" for atom "+certificate.getAtom()+" in block "+block.getHash()+" due to " + status.name());

			    status = this.stateIndexables.put(transaction, certificateIndexableKey, certificateIndexableCommitValue);
			    if (status.equals(OperationStatus.SUCCESS) != true) 
		    		throw new DatabaseException("Failed to commit atom certificate "+certificate.getHash()+" for atom "+certificate.getAtom()+" in block "+block.getHash()+" due to " + status.name());
		    }

		    DatabaseEntry syncKey = new DatabaseEntry(Longs.toByteArray(block.getHeader().getHeight()));
			DatabaseEntry syncValue = new DatabaseEntry(block.getHeader().getHash().toByteArray());
			status = this.syncChain.putNoOverwrite(transaction, syncKey, syncValue);
		    if (status.equals(OperationStatus.SUCCESS) != true) 
	    		throw new DatabaseException("Failed to commit to sync chain "+block.getHash()+" due to " + status.name());

	    	transaction.commit();
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
	
	final void commit(Collection<CommitOperation> commits) throws IOException
	{
	    Objects.requireNonNull(commits);
		
	    Transaction transaction = this.context.getDatabaseEnvironment().beginTransaction(null, null);
	    try 
	    {
	    	final Iterator<CommitOperation> commitIterator = commits.iterator();
	    	while (commitIterator.hasNext()) 
	    	{
	    		CommitOperation operation = commitIterator.next();
	    		OperationStatus status;

	    		final Indexable 	atomIndexable = Indexable.from(operation.getAtom().getHash(), Atom.class);
	    		final DatabaseEntry atomIndexableKey = new DatabaseEntry(atomIndexable.toByteArray());
	    		final DatabaseEntry atomIndexableCommitValue = new DatabaseEntry();
	    		
	    		status = this.stateIndexables.get(transaction, atomIndexableKey, atomIndexableCommitValue, LockMode.DEFAULT);
	    		if (status.equals(OperationStatus.SUCCESS) == false)
    				throw new DatabaseException("Atom indexable "+operation.getAtom().getHash()+" not found or has error "+status.name());
	    		
	    		final IndexableCommit atomIndexableCommit = Serialization.getInstance().fromDson(atomIndexableCommitValue.getData(), IndexableCommit.class);
	    		if (atomIndexableCommit.get(Path.BLOCK).equals(operation.getHead().getHash()) == false)
    				throw new DatabaseException("Atom indexable commit "+operation.getAtom().getHash()+" references block "+atomIndexableCommit.get(Path.BLOCK)+" not expected "+operation.getHead().getHash());
	    		
	    		final Indexable	blockIndexable = Indexable.from(operation.getHead().getHash(), BlockHeader.class);
	    		final DatabaseEntry blockIndexableKey = new DatabaseEntry(blockIndexable.toByteArray());
	    		final DatabaseEntry atomKey = new DatabaseEntry(operation.getAtom().getHash().toByteArray());
//	    		if (operation.getType().equals(StateOperation.Type.STORE)) 
	    		{
	    			if (operation.getHead().getHash().equals(Universe.getDefault().getGenesis().getHash()) == false)
	    			{
			    		status = this.stateIndexables.get(transaction, blockIndexableKey, null, LockMode.DEFAULT);
			    		if (status.equals(OperationStatus.SUCCESS) == false)
		    				throw new DatabaseException("Expected committed block "+operation.getHead().getHash()+" not found "+status.name());
	    			}
	    			
		    		status = this.primitives.get(transaction, atomKey, null, LockMode.DEFAULT);
		    		if (status.equals(OperationStatus.SUCCESS) == false)
	    				throw new DatabaseException("Atom "+operation.getAtom().getHash()+" not found or has error "+status.name());
		    		
	    			for (StateOp stateOp : operation.getStateOps())
	    			{
	    				if (stateOp.ins().equals(Instruction.SET) == false)
	    					continue;
	    				
	    				DatabaseEntry stateKey = new DatabaseEntry(stateOp.key().toByteArray());
	    				DatabaseEntry stateValue = new DatabaseEntry(stateOp.value().toByteArray());
	    				status = this.state.put(transaction, stateKey, stateValue);
	    				if (status.equals(OperationStatus.SUCCESS) == false)
	    					throw new DatabaseException("Failed to commit state "+stateOp+" for commit "+operation.getHead().getHeight()+":"+operation.getAtom().getHash()+" due to "+status.name()); 
	    				else if (databaseLog.hasLevel(Logging.DEBUG) == true)
	    					databaseLog.debug(this.context.getName()+": Stored state "+stateOp);
	    			}

	    			for (Indexable indexable : operation.getIndexables())
	    			{
	    				IndexableCommit indexableCommit = new IndexableCommit(operation.getHead().getIndexOf(InventoryType.ATOMS, operation.getAtom().getHash()), indexable, Collections.emptyList(), operation.getTimestamp(), Indexable.from(operation.getHead().getHash(), BlockHeader.class), Indexable.from(operation.getAtom(), Atom.class));
	    				DatabaseEntry indexableKey = new DatabaseEntry(indexable.toByteArray());
	    				DatabaseEntry indexableValue = new DatabaseEntry(Serialization.getInstance().toDson(indexableCommit, Output.PERSIST));
	    				status = this.stateIndexables.put(transaction, indexableKey, indexableValue);
	    				if (status.equals(OperationStatus.SUCCESS) == false)
	    					throw new DatabaseException("Failed to commit indexable "+indexable+" for commit "+operation.getHead().getHeight()+":"+operation.getAtom().getHash()+" due to "+status.name()); 
	    				else if (databaseLog.hasLevel(Logging.DEBUG) == true)
	    					databaseLog.debug(this.context.getName()+": Stored indexable "+indexable);
	    			}
	    			
	    	    	for (Identifier identifier : operation.getIdentifiers()) 
	    	    	{
	    	    		status = this.stateIdentifiers.put(transaction, new DatabaseEntry(identifier.toByteArray()), atomIndexableCommitValue);
	    	    		if (status.equals(OperationStatus.SUCCESS) == false)
	    					throw new DatabaseException("Failed to commit identifier "+identifier+" for commit "+operation.getHead().getHeight()+":"+operation.getAtom().getHash()+" due to "+status.name()); 
	    	    	} 
	    		}
/*	    		else if (operation.getType().equals(StateOperation.Type.DELETE)) 
	    		{
		    		status = this.stateIndexables.get(transaction, blockIndexableKey, null, LockMode.DEFAULT);
		    		if (status.equals(OperationStatus.SUCCESS) == false)
	    				throw new DatabaseException("Expected committed block "+operation.getHead().getHash()+" not found "+status.name());
	    			
		    		status = this.primitives.get(transaction, atomKey, null, LockMode.DEFAULT);
		    		if (status.equals(OperationStatus.SUCCESS) == false)
	    				throw new DatabaseException("Atom "+operation.getAtom().getHash()+" not found or has error "+status.name());
		    		
	    			for (Indexable indexable : operation.getAtom().getIndexables())
	    			{
	    	    		status = this.stateIndexables.delete(transaction, new DatabaseEntry(indexable.toByteArray()));
	    				if (status.equals(OperationStatus.SUCCESS) == false)
	    					databaseLog.warn(this.context.getName()+": Failed to uncommit indexable "+indexable+" for commit "+operation.getHead().getHeight()+":"+operation.getAtom().getHash()+" due to "+status.name()+" (could be pruned)");
	    				else if (databaseLog.hasLevel(Logging.DEBUG) == true)
	    					databaseLog.debug(this.context.getName()+": Deleted indexable "+indexable);
	    			}

	    			try(Cursor identifierCursor = this.stateIdentifiers.openCursor(transaction, null))
	    			{
	    		    	for (Identifier identifier : operation.getIdentifiers()) 
	    		    	{
	    		    		status = identifierCursor.getSearchBoth(new DatabaseEntry(identifier.toByteArray()), atomIndexableCommitValue, LockMode.DEFAULT);
	    					if (status.equals(OperationStatus.SUCCESS) == false)
	    						databaseLog.warn(this.context.getName()+": Failed to uncommit identifier "+identifier+" for commit "+operation.getHead().getHeight()+":"+operation.getAtom().getHash()+" due to "+status.name()+" (could be pruned)");
	    					else
	    					{
	    						status = identifierCursor.delete();
	    						if (status.equals(OperationStatus.SUCCESS) == false)
	    							throw new DatabaseException("Failed to uncommit identifier "+identifier+" for commit "+operation.getHead().getHeight()+":"+operation.getAtom().getHash()+" due to "+status.name());
	    					}
	    				}
	    			}
	    		}*/
	    	}

	    	transaction.commit();
	    } 
	    catch (Exception ex) 
	    {
			databaseLog.error(this.context.getName()+": State commit aborting", ex);
	    	transaction.abort();
	    	if (ex instanceof DatabaseException)
	    		throw ex; 
	    	throw new DatabaseException(ex);
	    } 
	}
	
	// SYNC //
	boolean has(long height) throws DatabaseException
	{
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

	Hash get(long height) throws DatabaseException
	{
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

	// IDENTIFIER SEARCH //
	final SearchResponse<IndexableCommit> search(final SearchQuery query) throws IOException
	{
		Objects.requireNonNull(query);
		
		Cursor searchCursor = this.stateIdentifiers.openCursor(null, null);
		try
		{
			long nextOffset = -1;
			List<IndexableCommit> commits = new ArrayList<IndexableCommit>();
			DatabaseEntry key = new DatabaseEntry(query.getIdentifiers().get(0).toByteArray());
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
				nextOffset = Serialization.getInstance().fromDson(value.getData(), IndexableCommit.class).getIndex();
				while(status.equals(OperationStatus.SUCCESS) == true && query.getOffset() > -1 && 
					  ((query.getOrder().equals(Order.ASCENDING) == true && nextOffset <= query.getOffset()) ||
					   (query.getOrder().equals(Order.DESCENDING) == true && nextOffset >= query.getOffset())))
				{
					if (query.getOrder().equals(Order.DESCENDING) == true)
						status = searchCursor.getPrevDup(key, value, LockMode.DEFAULT);
					else
						status = searchCursor.getNextDup(key, value, LockMode.DEFAULT);
	
					nextOffset = Serialization.getInstance().fromDson(value.getData(), IndexableCommit.class).getIndex();
				}
	
				while(status.equals(OperationStatus.SUCCESS) == true && commits.size() < query.getLimit())
				{
					if (query.getOrder().equals(Order.ASCENDING) == true && nextOffset > query.getOffset() ||
					    query.getOrder().equals(Order.DESCENDING) == true && nextOffset < query.getOffset())
					{
						IndexableCommit commit = Serialization.getInstance().fromDson(value.getData(), IndexableCommit.class);
						commits.add(commit);
					}

					if (query.getOrder().equals(Order.DESCENDING) == true)
						status = searchCursor.getPrevDup(key, value, LockMode.DEFAULT);
					else
						status = searchCursor.getNextDup(key, value, LockMode.DEFAULT);
				}
			}
			
			SearchResponse<IndexableCommit> response = new SearchResponse<IndexableCommit>(query, nextOffset, commits, status.equals(OperationStatus.NOTFOUND));
			return response;
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
