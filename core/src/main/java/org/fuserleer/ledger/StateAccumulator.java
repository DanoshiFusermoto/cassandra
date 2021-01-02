package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.fuserleer.Context;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.Indexable;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public final class StateAccumulator implements LedgerInterface, LedgerProvider
{
	private static final Logger stateLog = Logging.getLogger("state");

	private final Context context;

	private final LedgerProvider parent;
	private final Map<Hash, CommitOperation> operations;
	private final Map<Hash, CommitOperation> stateLocks;
	private final Map<Indexable, CommitOperation> indexables;
	private final ReentrantLock lock = new ReentrantLock(true);
	
	StateAccumulator(Context context, LedgerProvider parent)
	{
		this.context = Objects.requireNonNull(context);
		this.parent = Objects.requireNonNull(parent);
		this.operations = new HashMap<Hash, CommitOperation>();
		this.indexables = new HashMap<Indexable, CommitOperation>();
		this.stateLocks = new LinkedHashMap<Hash, CommitOperation>();
		
//		stateLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN | Logging.WARN);
	}
	
	public void reset()
	{
		this.lock.lock();
		try
		{
			this.stateLocks.clear();
			this.indexables.clear();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	@Override
	public StateOpResult<?> evaluate(StateOp stateOp) throws IOException
	{
		return this.parent.evaluate(stateOp);
	}

	@Override
	public CommitState has(Indexable indexable) throws IOException
	{
		Objects.requireNonNull(indexable);
		
		this.lock.lock();
		try
		{
			CommitOperation stateOperation = this.indexables.get(indexable);
			if (stateOperation != null)
				return stateOperation.getState();

			return this.parent.has(indexable);
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
	public <T extends Primitive> T get(final Indexable indexable) throws IOException
	{
		Objects.requireNonNull(indexable);
		
		this.lock.lock();
		try
		{
			CommitOperation stateOperation = this.indexables.get(indexable);
			if (stateOperation != null)
			{
//				if (stateOperation.getType().equals(StateOperation.Type.DELETE) == true)
//					return null;
//				else if (stateOperation.getType().equals(StateOperation.Type.STORE) == true)
				{
					if (Block.class.isAssignableFrom(indexable.getContainer()) == true)
					{
						Block block = this.context.getLedger().get(stateOperation.getHead().getHash(), Block.class);
						if (block == null)
							throw new IllegalStateException("Found indexable state operation but unable to locate block");

						return (T) block;
					}
					else if (BlockHeader.class.isAssignableFrom(indexable.getContainer()) == true)
					{
						BlockHeader blockHeader = this.context.getLedger().get(stateOperation.getHead().getHash(), BlockHeader.class);
						if (blockHeader == null)
							throw new IllegalStateException("Found indexable state operation but unable to locate block header");

						return (T) blockHeader;
					}
					else if (Atom.class.isAssignableFrom(indexable.getContainer()) == true)
					{
						Atom atom = this.context.getLedger().get(stateOperation.getAtom().getHash(), Atom.class);
						if (atom == null)
							throw new IllegalStateException("Found indexable state operation but unable to locate atom");

						return (T) atom;
					}
					else if (Particle.class.isAssignableFrom(indexable.getContainer()) == true)
					{
						Atom atom = this.context.getLedger().get(stateOperation.getAtom().getHash(), Atom.class);
						if (atom == null)
							throw new IllegalStateException("Found indexable state operation but unable to locate atom");

						for (Particle particle : atom.getParticles())
						{
							if (particle.getHash().equals(indexable.getKey()) == true)
								return (T) particle;
						}
					}
				}
			}
			else
				return this.context.getLedger().get(indexable);

			return null;
		}
		finally
		{
			this.lock.unlock();
		}
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
			CommitOperation stateOperation = this.indexables.get(indexable);
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
			
			return result;
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

			CommitOperation operation = new CommitOperation(block, atom, CommitState.LOCKED);
			// State ops
			for (Hash state : operation.getStates())
			{
				if (this.stateLocks.containsKey(state) == true)
					throw new IllegalStateException("State "+state+" is locked");
			}

			for (Hash state : operation.getStates())
			{
				this.stateLocks.put(state, operation);

				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": Locked state "+state+" via "+operation);
			}

			// Indexables
			for (Indexable indexable : operation.getIndexables())
			{
				if (this.indexables.containsKey(indexable) == true)
					throw new IllegalStateException("Indexable "+indexable+" is locked");
			}
			
			for (Indexable indexable : operation.getIndexables())
			{
				this.indexables.put(indexable, operation);

				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": Locked indexable "+indexable+" via "+operation);
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

			CommitOperation operation = this.operations.get(atom.getHash());
			if (operation == null)
				throw new IllegalStateException("Operation for atom "+atom.getHash()+" not found");

			operation.setState(CommitState.PRECOMMITTED);

			if (stateLog.hasLevel(Logging.DEBUG) == true)
			{
				for (StateOp stateOp : operation.getStateOps())
					stateLog.debug(this.context.getName()+": Precommitted state "+stateOp+" via "+operation);

				for (Indexable indexable : operation.getIndexables())
					stateLog.debug(this.context.getName()+": Precommitted indexable "+indexable+" via "+operation);
			}
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

			CommitOperation operation = this.operations.remove(atom.getHash());
			if (operation == null)
				throw new IllegalStateException("Operation for atom "+atom.getHash()+" not found");

			operation.setState(CommitState.COMMITTED);

			this.context.getLedger().getLedgerStore().commit(Collections.singletonList(operation));

			for (Hash state : operation.getStates())
			{
				this.stateLocks.remove(state);
				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": Committed state "+state+" via "+operation);
			}
			
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

			CommitOperation operation = this.operations.remove(atom.getHash());
			if (operation == null)
				throw new IllegalStateException("Operation for atom "+atom.getHash()+" not found");

			operation.setState(CommitState.ABORTED);

			for (Hash state : operation.getStates())
			{
				this.stateLocks.remove(state);
				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": Aborted state "+state+" via "+operation);
			}

			for (Indexable indexable : operation.getIndexables())
			{
				this.indexables.remove(indexable);
				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": Aborted indexable "+indexable+" via "+operation);
			}
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
			Iterator<CommitOperation> commitOperationIterator = this.operations.values().iterator();
			while(commitOperationIterator.hasNext() == true)
			{
				CommitOperation commitOperation = commitOperationIterator.next();
				if (commitOperation.getHead().getHeight() <= header.getHeight())
				{
					atomsRemoved.add(commitOperation.getAtom());
					for (Hash state : commitOperation.getStates())
						this.stateLocks.remove(state);

					for (Indexable indexable : commitOperation.getIndexables())
						this.indexables.remove(indexable);

					commitOperationIterator.remove();
				}
			}
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
			LinkedHashSet<CommitOperation> operationsToCommit = new LinkedHashSet<CommitOperation>();
			Iterator<CommitOperation> commitOperationIterator = this.operations.values().iterator();
			while(commitOperationIterator.hasNext() == true)
			{
				CommitOperation commitOperation = commitOperationIterator.next();
				if (commitOperation.getHead().getHeight() <= header.getHeight() && 
					commitOperation.getState().equals(CommitState.PRECOMMITTED) && 
					operationsToCommit.contains(commitOperation) == false)
				{						
					operationsToCommit.add(commitOperation);
				}
			}
			
			this.context.getLedger().getLedgerStore().commit(operationsToCommit);

			for (CommitOperation commitOperation : operationsToCommit)
			{
				for (Hash state : commitOperation.getStates())
					this.stateLocks.remove(state);

				for (Indexable indexable : commitOperation.getIndexables())
					this.indexables.remove(indexable);
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public int size() 
	{
		return this.stateLocks.size();
	}
}
