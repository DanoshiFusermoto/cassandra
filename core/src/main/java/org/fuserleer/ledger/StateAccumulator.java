package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.crypto.Hash;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.Numbers;
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
	
	StateAccumulator(final Context context, final String name, final LedgerProvider provider)
	{
		Objects.requireNonNull(context, "Context is null");
		Objects.requireNonNull(provider, "Ledger provider is null");
		Objects.requireNonNull(name, "Name is null");
		Numbers.isZero(name.length(), "Name is empty");

		this.context = context;
		this.provider = provider;
		this.name = name;
		this.pending = new HashMap<Hash, PendingAtom>();
		this.locked = new LinkedHashMap<Hash, PendingAtom>();
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
		Objects.requireNonNull(key, "State key is null");
		
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
	public UInt256 get(final StateKey<?, ?> key) throws IOException
	{
		Objects.requireNonNull(key, "State key is null");

		// TODO returns committed values only, perhaps want this to return pending stuff?
		return this.provider.get(key);
	}
	
	public Collection<Hash> locked()
	{
		this.lock.lock();
		try
		{
			List<Hash> locked = new ArrayList<Hash>(this.locked.keySet());
			Collections.sort(locked);
			return locked;
		}
		finally
		{
			this.lock.unlock();
		}
	}


	void lock(final Collection<PendingAtom> pendingAtoms) throws StateLockedException
	{
		Objects.requireNonNull(pendingAtoms, "Pending atoms is null");
		
		if (pendingAtoms.isEmpty() == true)
			return;
		
		this.lock.lock();
		try
		{
			for (PendingAtom pendingAtom : pendingAtoms)
				lock(pendingAtom);
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	void lock(final PendingAtom pendingAtom) throws StateLockedException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		this.lock.lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Locking state in "+pendingAtom.getHash());
			
			PendingAtom locked = this.pending.get(pendingAtom.getHash()); 
			if (locked != null)
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
	
	void unlock(final Collection<PendingAtom> pendingAtoms) throws StateLockedException
	{
		Objects.requireNonNull(pendingAtoms, "Pending atoms is null");
		
		if (pendingAtoms.isEmpty() == true)
			return;
		
		this.lock.lock();
		try
		{
			for (PendingAtom pendingAtom : pendingAtoms)
				unlock(pendingAtom);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	void unlock(final PendingAtom pendingAtom) throws StateLockedException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
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
	
	boolean isLocked(final StateKey<?, ?> key)
	{
		Objects.requireNonNull(key, "State key is null");
		
		this.lock.lock();
		try
		{
			if (key.scope().equals(Atom.class) == true)
			{
				if (this.pending.containsKey(key.key()) == false)
					return false;
				else
					return true;
			}
			
			if (this.locked.containsKey(key.get()) == true)
			{
				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": "+this.name+" State "+key.get()+" is locked");

				return true;
			}

			return false;
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
