package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

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

	private final Context context ;

	private final List<StateOperation> stateOperationQueue;
	private final Map<Hash, Fields> fields;
	private final Map<Indexable, StateOperation> indexables;
	private final ReentrantLock lock = new ReentrantLock(true);
	
	StateAccumulator(Context context)
	{
		this.context = Objects.requireNonNull(context);
		this.stateOperationQueue = new ArrayList<StateOperation>();
		this.fields = new LinkedHashMap<Hash, Fields>();
		this.indexables = new LinkedHashMap<Indexable, StateOperation>();
		
		stateLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN | Logging.WARN);
	}
	
	StateAccumulator(StateAccumulator other)
	{
		other.lock.lock();
		try
		{
			this.context = other.context;
			this.stateOperationQueue = new ArrayList<StateOperation>(other.stateOperationQueue);
			this.fields = new LinkedHashMap<Hash, Fields>(other.fields);
			this.indexables = new LinkedHashMap<Indexable, StateOperation>(other.indexables);
		}
		finally
		{
			other.lock.unlock();
		}
	}

	public void reset()
	{
		this.lock.lock();
		try
		{
			this.indexables.clear();
			this.fields.clear();
			this.stateOperationQueue.clear();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	Collection<StateOperation> getOperations()
	{
		this.lock.lock();
		try
		{
			return Collections.unmodifiableList(new ArrayList<StateOperation>(this.stateOperationQueue));
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
	public boolean state(Indexable indexable) throws IOException
	{
		Objects.requireNonNull(indexable);
		
		this.lock.lock();
		try
		{
			StateOperation stateOperation = this.indexables.get(indexable);
			if (stateOperation != null)
			{
				if (stateOperation.getType().equals(StateOperation.Type.DELETE) == true)
					return false;
				else if (stateOperation.getType().equals(StateOperation.Type.STORE) == true)
					return true;
			}

			// TODO will need a distributed search here when sharded
			IndexableCommit search = this.context.getLedger().getLedgerStore().search(indexable);
			if (search != null)
				return true;
						
			return false;
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
				if (stateOperation.getType().equals(StateOperation.Type.DELETE) == true)
					return null;
				else if (stateOperation.getType().equals(StateOperation.Type.STORE) == true)
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

	void delete(final BlockHeader block, final Atom atom) throws IOException
	{
		Objects.requireNonNull(block);
		
		this.lock.lock();
		try
		{
			StateOperation operation = new StateOperation(StateOperation.Type.DELETE, block, atom);
			for (Indexable indexable : operation.getIndexables())
			{
				this.indexables.put(indexable, operation);

				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": Deleted state "+indexable+" via "+operation);
			}
				
			this.stateOperationQueue.add(operation);
			this.fields.remove(operation.getAtom().getHash());
		}
		finally
		{
			this.lock.unlock();
		}
	}

	void store(final BlockHeader block, final Atom atom) throws IOException
	{
		Objects.requireNonNull(block);
		Objects.requireNonNull(atom);
		
		this.lock.lock();
		try
		{
			StateOperation operation = new StateOperation(StateOperation.Type.STORE, block, atom);
			for (Indexable indexable : operation.getIndexables())
			{
				this.indexables.put(indexable, operation);

				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": Stored state "+indexable+" via "+operation);
			}
			
			this.stateOperationQueue.add(operation);
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	void store(final Block block) throws IOException
	{
		Objects.requireNonNull(block);
		
		this.lock.lock();
		try
		{
			for (Atom atom : block.getAtoms())
				store(block.getHeader(), atom);
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
			Iterator<StateOperation> stateOperationIterator = this.stateOperationQueue.iterator();
			while(stateOperationIterator.hasNext() == true)
			{
				StateOperation stateOperation = stateOperationIterator.next();
				if (stateOperation.getHead().getHeight() <= header.getHeight())
				{
					for (Indexable indexable : stateOperation.getIndexables())
						this.indexables.remove(indexable);

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

	void commit(BlockHeader header) throws IOException
	{
		this.lock.lock();
		try
		{
			List<StateOperation> stateOperationsToCommit = new ArrayList<StateOperation>();
			Iterator<StateOperation> stateOperationIterator = this.stateOperationQueue.iterator();
			while(stateOperationIterator.hasNext() == true)
			{
				StateOperation stateOperation = stateOperationIterator.next();
				if (stateOperation.getType().equals(StateOperation.Type.STORE))
				{						
					Fields fields = this.fields.get(stateOperation.getAtom().getHash());
					if (fields != null)
						stateOperation.getAtom().setFields(fields);
				}
				else if (stateOperation.getType().equals(StateOperation.Type.DELETE))
				{
					// TODO need to do anything with the fields on a delete?
				}
				else 
					throw new IllegalStateException("Illegal StateOperation "+stateOperation);
				
				if (stateOperation.getHead().getHeight() > header.getHeight())
					break;
				
				stateOperationsToCommit.add(stateOperation);
			}
			
			this.context.getLedger().getLedgerStore().commit(stateOperationsToCommit, this.fields.entrySet());

			stateOperationIterator = this.stateOperationQueue.iterator();
			while(stateOperationIterator.hasNext() == true)
			{
				StateOperation stateOperation = stateOperationIterator.next();
				if (stateOperation.getHead().getHeight() <= header.getHeight())
				{
					for (Indexable indexable : stateOperation.getIndexables())
						this.indexables.remove(indexable);

//					this.fields.remove(stateOperation.getAtom().getHash());
					stateOperationIterator.remove();
				}
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public int storing() 
	{
		return this.stateOperationQueue.stream().filter(op -> op.getType().equals(StateOperation.Type.STORE)).collect(Collectors.counting()).intValue();
	}

	public int deleting() 
	{
		return this.stateOperationQueue.stream().filter(op -> op.getType().equals(StateOperation.Type.DELETE)).collect(Collectors.counting()).intValue();
	}

	public int size() 
	{
		return this.stateOperationQueue.size();
	}
}
