package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

public final class StateAccumulator implements LedgerProvider
{
	private static final Logger stateLog = Logging.getLogger("state");
	
	public enum StateLockType
	{
		NONE, EXCLUSIVE, NONEXCLUSIVE
	}

	private final Context context;
	
	private final LedgerProvider provider;
	private final Map<Hash, PendingAtom> pending;
	private final Map<Hash, PendingAtom> exclusive;
	private final Multimap<Hash, PendingAtom> nonexclusive;
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
		this.exclusive = new LinkedHashMap<Hash, PendingAtom>();
		this.nonexclusive = LinkedHashMultimap.create();
	}

	StateAccumulator shadow()
	{
		StateAccumulator shadow;
		
		this.lock.lock();
		try
		{
			shadow = new StateAccumulator(this.context, "Shadowed "+this.name, this.provider);
			shadow.pending.putAll(this.pending);
			shadow.exclusive.putAll(this.exclusive);
			shadow.nonexclusive.putAll(this.nonexclusive);
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
			this.exclusive.clear();
			this.nonexclusive.clear();
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
	
	public Collection<Hash> locked(boolean exclusive)
	{
		this.lock.lock();
		try
		{
			List<Hash> locked;
			
			if (exclusive == false)
				locked = new ArrayList<Hash>(this.nonexclusive.keySet());
			else
				locked = new ArrayList<Hash>(this.exclusive.keySet());
				
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
			for (StateOp stateOp : pendingAtom.getStateOps())
			{
				if (lockable(stateOp.key(), pendingAtom, stateOp.ins().exclusive()) == false)
					throw new StateLockedException(stateOp.key(), pendingAtom.getHash());
			}

			for (StateOp stateOp : pendingAtom.getStateOps())
			{
				if (stateOp.ins().exclusive() == true)
					this.exclusive.put(stateOp.key().get(), pendingAtom);
				else
					this.nonexclusive.put(stateOp.key().get(), pendingAtom);

				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": "+this.name+" Locked state "+stateOp+" "+(stateOp.ins().exclusive() == true ? "exclusively":"non-exclusively")+" via "+pendingAtom.getHash());
			}

			this.pending.put(pendingAtom.getHash(), pendingAtom);
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	void lock(final PendingAtom pendingAtom, final Collection<StateOp> stateOps) throws StateLockedException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		this.lock.lock();
		try
		{
			if (stateLog.hasLevel(Logging.DEBUG) == true)
				stateLog.debug(this.context.getName()+": Locking state "+stateOps+" in "+pendingAtom.getHash());

			PendingAtom locked = this.pending.get(pendingAtom.getHash()); 
			if (locked == null)
				throw new IllegalStateException("Atom "+pendingAtom.getHash()+" is already not pending and locked");
			
			for (StateOp stateOp : stateOps)
			{
				if (lockable(stateOp.key(), pendingAtom, stateOp.ins().exclusive()) == false)
					throw new StateLockedException(stateOp.key(), pendingAtom.getHash());
			}

			for (StateOp stateOp : stateOps)
			{
				if (stateOp.ins().exclusive() == true)
					this.exclusive.put(stateOp.key().get(), pendingAtom);
				else
					this.nonexclusive.put(stateOp.key().get(), pendingAtom);
	
				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": "+this.name+" Locked state "+stateOp+" "+(stateOp.ins().exclusive() == true ? "exclusively":"non-exclusively")+" via "+pendingAtom.getHash());
			}
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
			for (StateOp stateOp : pendingAtom.getStateOps())
			{
				if (stateOp.ins().exclusive() == true)
				{
					if (isLocked(stateOp.key()).equals(StateLockType.EXCLUSIVE) == false)
						throw new StateLockedException("State "+stateOp+" required by "+pendingAtom.getHash()+" is NOT locked exclusively", stateOp.key(), pendingAtom.getHash());
				}
				else
				{
					if (isLocked(stateOp.key()).equals(StateLockType.NONE) == true)
						throw new StateLockedException("State "+stateOp+" required by "+pendingAtom.getHash()+" is NOT locked non-exclusively", stateOp.key(), pendingAtom.getHash());
				}
			}

			Set<StateKey<?, ?>> unlocked = new HashSet<StateKey<?,?>>();
			for (StateOp stateOp : pendingAtom.getStateOps())
			{
				if (unlocked.contains(stateOp.key()) == true)
					continue;
				
				if (stateOp.ins().exclusive() == false)
					continue;

				if (this.exclusive.remove(stateOp.key().get(), pendingAtom) == false)
					throw new StateLockedException("State "+stateOp+" in "+pendingAtom.getHash()+" is NOT locked exclusively", stateOp.key(), pendingAtom.getHash());
					
				unlocked.add(stateOp.key());

				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": "+this.name+" Unlocked state "+stateOp+" via "+pendingAtom.getHash());
			}

			unlocked.clear();
			for (StateOp stateOp : pendingAtom.getStateOps())
			{
				if (unlocked.contains(stateOp.key()) == true)
					continue;
				
				if (stateOp.ins().exclusive() == true)
					continue;

				if (this.nonexclusive.remove(stateOp.key().get(), pendingAtom) == false)
					throw new StateLockedException("State "+stateOp+" in "+pendingAtom.getHash()+" is NOT locked non-exclusively", stateOp.key(), pendingAtom.getHash());

				unlocked.add(stateOp.key());
				
				if (stateLog.hasLevel(Logging.DEBUG) == true)
					stateLog.debug(this.context.getName()+": "+this.name+" Unlocked state "+stateOp+" via "+pendingAtom.getHash());
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	private boolean lockable(final StateKey<?, ?> key, final PendingAtom pendingAtom, boolean exclusive)
	{
		Objects.requireNonNull(key, "State key is null");
		
		this.lock.lock();
		try
		{
			if (key.scope().equals(Atom.class) == true)
			{
				if (this.pending.containsKey(key.key()) == true)
					return false;
				else
					return true;
			}
			
			if (exclusive == true)
			{
				if (this.nonexclusive.containsKey(key.get()) == true)
					return false;
				
				if (this.exclusive.containsKey(key.get()) == false) 
					return true;
				
				// Reentrant
				PendingAtom exclusiveLockHolder = this.exclusive.get(key.get());
				if (exclusiveLockHolder.equals(pendingAtom) == false)
					return false;
			}
			else
			{
				PendingAtom exclusiveLockHolder = this.exclusive.get(key.get());
				if (exclusiveLockHolder != null && exclusiveLockHolder.equals(pendingAtom) == false)
					return false;
			}
			
			return true;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	StateLockType isLocked(final StateKey<?, ?> key)
	{
		Objects.requireNonNull(key, "State key is null");
		
		this.lock.lock();
		try
		{
			if (key.scope().equals(Atom.class) == true)
			{
				if (this.pending.containsKey(key.key()) == false)
					return StateLockType.NONE;
				else
					return StateLockType.EXCLUSIVE;
			}
			
			if (this.exclusive.containsKey(key.get()) == true) 
				return StateLockType.EXCLUSIVE;
				
			if (this.nonexclusive.containsKey(key.get()) == true)
				return StateLockType.NONEXCLUSIVE;

			return StateLockType.NONE;
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

	public int numLocked(boolean exclusive) 
	{
		if (exclusive == false)
			return this.nonexclusive.size();
		else
			return this.exclusive.size();
	}
	
	LedgerProvider getProvider()
	{
		return this.provider;
	}
}
