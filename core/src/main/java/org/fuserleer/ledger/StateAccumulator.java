package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.fuserleer.Context;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.Fields;
import org.fuserleer.database.Indexable;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public final class StateAccumulator implements LedgerInterface
{
	private static final Logger stateLog = Logging.getLogger("state");

	private final Context context;

	private final StateProvider parent;
	private final Map<Hash, Fields> fields;
	private final Map<Hash, StateOperation> operations;
	private final Map<Indexable, StateOperation> indexables;
	private final ReentrantLock lock = new ReentrantLock(true);
	
	StateAccumulator(Context context, StateProvider parent)
	{
		this.context = Objects.requireNonNull(context);
		this.parent = Objects.requireNonNull(parent);
		this.fields = new LinkedHashMap<Hash, Fields>();
		this.operations = new HashMap<Hash, StateOperation>();
		this.indexables = new LinkedHashMap<Indexable, StateOperation>();
		
//		stateLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN | Logging.WARN);
	}
	
	public void reset()
	{
		this.lock.lock();
		try
		{
			this.indexables.clear();
			this.fields.clear();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	// TODO needs to return a map
	Set<Entry<Hash, Fields>> getFields()
	{
		this.lock.lock();
		try
		{
			return Collections.unmodifiableSet(new HashSet<Entry<Hash, Fields>>(this.fields.entrySet()));
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	@Override
	public CommitState state(Indexable indexable) throws IOException
	{
		Objects.requireNonNull(indexable);
		
		this.lock.lock();
		try
		{
			StateOperation stateOperation = this.indexables.get(indexable);
			if (stateOperation != null)
				return stateOperation.getState();

			return this.parent.state(indexable);
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	@Override
	public <T extends Primitive> SearchResponse<T> get(final SearchQuery query, final Class<T> type, final Spin spin) throws IOException
	{
		throw new UnsupportedOperationException("Searching of queries is not supported");
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends Primitive> T get(final Indexable indexable, final Class<T> container) throws IOException
	{
		Objects.requireNonNull(indexable);
		
		this.lock.lock();
		try
		{
			T result = null;
			StateOperation stateOperation = this.indexables.get(indexable);
			if (stateOperation != null)
			{
//				if (stateOperation.getType().equals(StateOperation.Type.DELETE) == true)
//					return null;
//				else if (stateOperation.getType().equals(StateOperation.Type.STORE) == true)
				{
					if (Block.class.isAssignableFrom(container) == true)
					{
						Block block = this.context.getLedger().get(stateOperation.getHead().getHash(), Block.class);
						if (block == null)
							throw new IllegalStateException("Found indexable commit but unable to locate block");

						result = (T) block;
					}
					else if (BlockHeader.class.isAssignableFrom(container) == true)
					{
						BlockHeader blockHeader = this.context.getLedger().get(stateOperation.getHead().getHash(), BlockHeader.class);
						if (blockHeader == null)
							throw new IllegalStateException("Found indexable commit but unable to locate block header");

						result = (T) blockHeader;
					}
					else if (Atom.class.isAssignableFrom(container) == true)
					{
						Atom atom = this.context.getLedger().get(stateOperation.getAtom().getHash(), Atom.class);
						if (atom == null)
							throw new IllegalStateException("Found indexable commit but unable to locate atom");

						result = (T) atom;
					}
					else if (Particle.class.isAssignableFrom(container) == true)
					{
						Atom atom = this.context.getLedger().get(stateOperation.getAtom().getHash(), Atom.class);
						if (atom == null)
							throw new IllegalStateException("Found indexable commit but unable to locate atom");

						for (Particle particle : atom.getParticles())
						{
							if (container.isAssignableFrom(particle.getClass()) == false)
								continue;
								
							if (particle.getHash().equals(indexable.getKey()) == true || 
								particle.getIndexables().contains(indexable) == true)
							{
								result = (T) particle;
								break;
							}
						}
					}
				}
			}
			else
				result = this.context.getLedger().get(indexable, container);
			
			// TODO what about field within Block / BlockHeader containers and Particles? 
			if (result instanceof Atom)
				init(((Atom)result).getHash(), ((Atom)result).getFields());
				
			return result;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	// FIELDS //
	// TODO what happens with these on a DELETE op?  A DELETE could simply be part of a reorg, so don't want to actually delete them
	private void init(final Hash hash, final Fields fields)
	{
		Objects.requireNonNull(hash);
		Objects.requireNonNull(fields);
		
		this.lock.lock();
		try
		{
			this.fields.put(hash, fields);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public void set(final Hash hash, final Fields fields)
	{
		Objects.requireNonNull(hash);
		Objects.requireNonNull(fields);
		
		this.lock.lock();
		try
		{
			if (this.fields.containsKey(hash) == false)
				throw new IllegalStateException("Fields for "+hash+" not initialized");

			this.fields.put(hash, fields);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	void lock(final BlockHeader block, final Atom atom)
	{
		Objects.requireNonNull(block);
		Objects.requireNonNull(atom);
		
		this.lock.lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Locking state in "+atom.getHash());

			StateOperation operation = new StateOperation(block, atom, CommitState.LOCKED);
			for (Indexable indexable : operation.getIndexables())
			{
				if (this.indexables.containsKey(indexable) == true)
					throw new IllegalStateException("Indexable "+indexable+" is locked");
			}
			
			for (Indexable indexable : operation.getIndexables())
			{
				this.indexables.put(indexable, operation);

				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": Locked state "+indexable+" via "+operation);
			}

			this.operations.put(atom.getHash(), operation);
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	void precommit(final Atom atom) throws IOException
	{
		Objects.requireNonNull(atom);
		
		this.lock.lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Precommitting state in "+atom.getHash());

			StateOperation operation = this.operations.get(atom.getHash());
			if (operation == null)
				throw new IllegalStateException("Operation for atom "+atom.getHash()+" not found");

			operation.setState(CommitState.PRECOMMITTED);

			if (stateLog.hasLevel(Logging.DEBUG) == true)
				for (Indexable indexable : operation.getIndexables())
					stateLog.debug(this.context.getName()+": Precommitted state "+indexable+" via "+operation);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	void commit(final Atom atom) throws IOException
	{
		Objects.requireNonNull(atom);
		
		this.lock.lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Committing state in "+atom.getHash());

			StateOperation operation = this.operations.remove(atom.getHash());
			if (operation == null)
				throw new IllegalStateException("Operation for atom "+atom.getHash()+" not found");

			operation.setState(CommitState.COMMITTED);

			Fields fields = this.fields.remove(operation.getAtom().getHash());
			if (fields != null)
				operation.getAtom().setFields(fields);

			this.context.getLedger().getLedgerStore().commit(Collections.singletonList(operation), this.fields.entrySet());

			for (Indexable indexable : operation.getIndexables())
			{
				this.indexables.remove(indexable);
				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": Committed state "+indexable+" via "+operation);
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}

	void abort(final Atom atom) throws IOException
	{
		Objects.requireNonNull(atom);
		
		this.lock.lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Aborting state in "+atom.getHash());

			StateOperation operation = this.operations.remove(atom.getHash());
			if (operation == null)
				throw new IllegalStateException("Operation for atom "+atom.getHash()+" not found");

			operation.setState(CommitState.ABORTED);

			for (Indexable indexable : operation.getIndexables())
			{
				this.indexables.remove(indexable);
				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": Aborted state "+indexable+" via "+operation);
			}
			
			this.fields.remove(operation.getAtom().getHash());
		}
		finally
		{
			this.lock.unlock();
		}
	}

	void alignTo(BlockHeader header)
	{
		this.lock.lock();
		try
		{
			Set<Atom> atomsRemoved = new HashSet<Atom>();
			Iterator<Indexable> stateOperationIterator = this.indexables.keySet().iterator();
			while(stateOperationIterator.hasNext() == true)
			{
				Indexable indexable = stateOperationIterator.next();
				StateOperation stateOperation = this.indexables.get(indexable);
				if (stateOperation.getHead().getHeight() <= header.getHeight())
				{
					atomsRemoved.add(stateOperation.getAtom());
					stateOperationIterator.remove();
				}
			}

			for (Atom atom : atomsRemoved)
				this.fields.remove(atom.getHash());
		}
		finally
		{
			this.lock.unlock();
		}
	}

	void commits(BlockHeader header) throws IOException
	{
		this.lock.lock();
		try
		{
			LinkedHashSet<StateOperation> stateOperationsToCommit = new LinkedHashSet<StateOperation>();
			Iterator<Indexable> stateOperationIterator = this.indexables.keySet().iterator();
			while(stateOperationIterator.hasNext() == true)
			{
				Indexable indexable = stateOperationIterator.next();
				StateOperation stateOperation = this.indexables.get(indexable);
				if (stateOperation.getHead().getHeight() > header.getHeight() && 
					stateOperation.getState().equals(CommitState.PRECOMMITTED) && 
					stateOperationsToCommit.contains(stateOperation) == false)
				{						
					Fields fields = this.fields.get(stateOperation.getAtom().getHash());
					if (fields != null)
						stateOperation.getAtom().setFields(fields);

					stateOperationsToCommit.add(stateOperation);
				}
			}
			
			this.context.getLedger().getLedgerStore().commit(stateOperationsToCommit, this.fields.entrySet());

			stateOperationIterator = this.indexables.keySet().iterator();
			while(stateOperationIterator.hasNext() == true)
			{
				Indexable indexable = stateOperationIterator.next(); 
				StateOperation stateOperation = this.indexables.get(indexable);
				if (stateOperationsToCommit.contains(stateOperation))
				{
					this.fields.remove(stateOperation.getAtom().getHash());
					stateOperationIterator.remove();
				}
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public int size() 
	{
		return this.indexables.size();
	}
}
