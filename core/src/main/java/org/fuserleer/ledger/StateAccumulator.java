package org.fuserleer.ledger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.crypto.Hash;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.UInt256;

public final class StateAccumulator implements LedgerProvider
{
	private static final Logger stateLog = Logging.getLogger("state");

	private final Context context;

	private final LedgerProvider provider;
	private final Map<Hash, PendingAtom> pending;
	private final Map<Hash, PendingAtom> locked;
	private final ReentrantLock lock = new ReentrantLock(true);
	private final String name;
	
	StateAccumulator(Context context, String name, LedgerProvider provider)
	{
		this.context = Objects.requireNonNull(context);
		this.provider = Objects.requireNonNull(provider);
		this.pending = new HashMap<Hash, PendingAtom>();
		this.locked = new LinkedHashMap<Hash, PendingAtom>();
		this.name = name;
	}

	StateAccumulator shadow()
	{
		StateAccumulator shadow;
		
		this.lock.lock();
		try
		{
			shadow = new StateAccumulator(this.context, "Shadowed "+this.name, this.provider);
			shadow.pending.putAll(this.pending);
			shadow.locked.putAll(this.locked);
		}
		finally
		{
			this.lock.unlock();
		}
	
		return shadow;
	}
	
	public void reset()
	{
		this.lock.lock();
		try
		{
			this.pending.clear();
			this.locked.clear();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	@Override
	public CommitStatus has(final StateKey<?, ?> key) throws IOException
	{
		Objects.requireNonNull(key);
		
		this.lock.lock();
		try
		{
			// TODO may need some functionality here to access un-committed state key information
//			PendingAtom pendingAtom = this.states.get(key.get());
//			if (pendingAtom != null)
//				return pendingAtom.getStatus();

			return this.provider.has(key);
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	@Override
	public UInt256 get(StateKey<?, ?> key) throws IOException
	{
		// TODO returns committed values only, perhaps want this to return pending stuff?
		return this.provider.get(key);
	}

	void lock(final PendingAtom pendingAtom) throws StateLockedException
	{
		Objects.requireNonNull(pendingAtom);
		
		this.lock.lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Locking state in "+pendingAtom.getHash());
			
			if (this.pending.containsKey(pendingAtom.getHash()) == true)
				throw new IllegalStateException("Atom "+pendingAtom.getHash()+" is already pending and locked");
			
			// State ops
			for (StateKey<?, ?> state : pendingAtom.getStateKeys())
			{
				if (isLocked(state) == true)
					throw new StateLockedException(state, pendingAtom.getHash());
			}

			for (StateKey<?, ?> state : pendingAtom.getStateKeys())
			{
				this.locked.put(state.get(), pendingAtom);

				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": "+this.name+" Locked state "+state+" via "+pendingAtom.getHash());
			}

			this.pending.put(pendingAtom.getHash(), pendingAtom);
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	void unlock(final PendingAtom pendingAtom) throws StateLockedException
	{
		Objects.requireNonNull(pendingAtom);
		
		this.lock.lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Unlocking state in "+pendingAtom.getHash());
			
			if (this.pending.remove(pendingAtom.getHash()) == null)
				throw new IllegalStateException("Atom "+pendingAtom.getHash()+" is not found");
			
			// State ops
			for (StateKey<?, ?> state : pendingAtom.getStateKeys())
			{
				if (isLocked(state) == false)
					throw new StateLockedException("State "+state+" required by "+pendingAtom.getHash()+" is NOT locked", state, pendingAtom.getHash());
			}

			for (StateKey<?, ?> state : pendingAtom.getStateKeys())
			{
				if (this.locked.remove(state.get(), pendingAtom) == false)
					throw new StateLockedException("State "+state+" is not locked by "+pendingAtom.getHash(), state, pendingAtom.getHash());

				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": "+this.name+" Unlocked state "+state+" via "+pendingAtom.getHash());
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	void remove(PendingAtom pendingAtom) throws StateLockedException
	{
		Objects.requireNonNull(pendingAtom);
		
		this.lock.lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Removing atom "+pendingAtom.getHash());
			
			if (this.pending.containsKey(pendingAtom.getHash()) == false)
				throw new IllegalStateException("Atom "+pendingAtom.getHash()+" is not found");
			
			// State ops
			for (StateKey<?, ?> state : pendingAtom.getStateKeys())
			{
				if (isLocked(state) == true && this.locked.get(state.get()).equals(pendingAtom) == true)
					throw new StateLockedException("Can not remove atom "+pendingAtom.getHash()+" when owns locked state "+state, state, pendingAtom.getHash());
			}

			this.pending.remove(pendingAtom.getHash());
		}
		finally
		{
			this.lock.unlock();
		}
	}


	boolean isLocked(StateKey<?, ?> state)
	{
		this.lock.lock();
		try
		{
			if (this.locked.containsKey(state.get()) == true)
			{
				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": "+this.name+" State "+state.get()+" is locked");

				return true;
			}

			return false;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	Set<StateKey<?, ?>> provision(final BlockHeader block, final PendingAtom pendingAtom) throws IOException
	{
		Objects.requireNonNull(pendingAtom);
		
		this.lock.lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": "+this.name+" Provisioning state in "+pendingAtom.getHash());

			if (this.pending.containsKey(pendingAtom.getHash()) == false)
				throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" not found");

			Set<StateKey<?, ?>> stateKeys = pendingAtom.provision(block);

			if (stateLog.hasLevel(Logging.DEBUG) == true)
			{
				for (StateKey<?, ?> stateKey : stateKeys)
					stateLog.debug(this.context.getName()+": "+this.name+" Provisioning state "+stateKey.get()+" via "+pendingAtom.getHash());
			}
			
			return stateKeys;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	void commit(final PendingAtom pendingAtom) throws IOException
	{
		Objects.requireNonNull(pendingAtom);
		
		this.lock.lock();
		try
		{
			CommitOperation commitOperation = pendingAtom.getCommitOperation();
			
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": "+this.name+" Committing state with "+commitOperation.getType()+" in "+pendingAtom.getHash());

			if (this.pending.remove(pendingAtom.getHash(), pendingAtom) == false)
				throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" not found");

			this.context.getLedger().getLedgerStore().commit(commitOperation);

			for (StateOp stateOp : commitOperation.getStateOps())
			{
				if (stateOp.ins().equals(Instruction.SET) == true)
					stateLog.debug(this.context.getName()+": "+this.name+" Committed state "+stateOp.key().get()+" with "+commitOperation.getType()+" via "+pendingAtom.getHash());
			}
			
			Set<StateKey<?, ?>> unlocked = new HashSet<StateKey<?, ?>>();
			for (StateOp stateOp : pendingAtom.getStateOps())
			{
				if (unlocked.contains(stateOp.key()) == false)
				{
					if (this.locked.remove(stateOp.key().get(), pendingAtom) == false)
						throw new IllegalStateException("Locked state "+stateOp.key()+" in "+pendingAtom.getHash()+" not found");
				
					unlocked.add(stateOp.key());
					
					if (stateLog.hasLevel(Logging.DEBUG) == true)
						stateLog.debug(this.context.getName()+": "+this.name+" Unlocked state "+stateOp.key().get()+" via "+pendingAtom.getHash());
				}
			}
			
			if (stateLog.hasLevel(Logging.DEBUG) == true)
			{
				for (Path path : commitOperation.getStatePaths())
					stateLog.debug(this.context.getName()+": Committed state path "+path+" with "+commitOperation.getType()+" via "+pendingAtom.getHash());
			
				for (Path path : commitOperation.getAssociationPaths())
					stateLog.debug(this.context.getName()+": Committed association path "+path+" with "+commitOperation.getType()+" via "+pendingAtom.getHash());
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}

	void abort(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom);
		
		this.lock.lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": "+this.name+" Aborting state in "+pendingAtom.getHash());

			if (this.pending.remove(pendingAtom.getHash(), pendingAtom) == false)
				throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" not found");

			Set<StateKey<?, ?>> unlocked = new HashSet<StateKey<?, ?>>();
			for (StateOp stateOp : pendingAtom.getStateOps())
			{
				if (unlocked.contains(stateOp.key()) == false)
				{
					if (this.locked.remove(stateOp.key().get(), pendingAtom) == false)
						throw new IllegalStateException("Locked state "+stateOp.key()+" in "+pendingAtom.getHash()+" not found");

					unlocked.add(stateOp.key());
					
					if (stateLog.hasLevel(Logging.DEBUG) == true)
						stateLog.debug(this.context.getName()+": "+this.name+" Aborted state "+stateOp.key().get()+" via "+pendingAtom.getHash());
				}
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}

	Set<PendingAtom> getPendingAtoms() 
	{
		this.lock.lock();
		try
		{
			return this.pending.values().stream().collect(Collectors.toSet());
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public int numPendingAtoms() 
	{
		return this.pending.size();
	}

	public int numLocked() 
	{
		return this.locked.size();
	}
	
	LedgerProvider getProvider()
	{
		return this.provider;
	}
}
