package org.fuserleer.collections;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.fuserleer.utils.Ints;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.Numbers;

public class MappedBlockingQueue<K, V> 
{
	private final LinkedHashMap<K, V> map;
	private final int capacity;

	/*
     * Concurrency control uses the classic two-condition algorithm
     * found in any textbook.
     */

    /** Main lock guarding all access */
    final ReentrantReadWriteLock lock;
    /** Condition for waiting takes */
    private final Condition notEmpty;
    /** Condition for waiting puts */
    private final Condition notFull;

	private int count;

	public MappedBlockingQueue(int capacity) 
	{
		Ints.greaterThan(capacity, 0, "Capacity is less than 1");
		
		this.map = new LinkedHashMap<K, V>(capacity);
		this.lock = new ReentrantReadWriteLock(true);
		this.notEmpty = this.lock.writeLock().newCondition();
		this.notFull =  this.lock.writeLock().newCondition();
		this.count = 0;
		this.capacity = capacity;
	}

	public int capacity() 
	{
		return this.capacity;
	}
	
	/**
     * Atomically removes all of the elements from this queue.
     * The queue will be empty after this call returns.
     */
    public void clear() 
    {
        this.lock.writeLock().lock();
        try 
        {
        	this.map.clear();
            this.notFull.signalAll();
        } 
        finally 
        {
            this.lock.writeLock().unlock();
        }
    }

	public boolean isEmpty() 
	{
        this.lock.readLock().lock();
        try 
        {
			return this.map.isEmpty();
        }
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public int size() 
	{
        this.lock.readLock().lock();
        try 
        {
            return this.count;
        } 
        finally 
        {
            this.lock.readLock().unlock();
        }
    }
	
	public int drainTo(final Collection<? super V> collection)
	{
		Objects.requireNonNull(collection, "Collection to drain to is null");
		if (collection == this)
            throw new IllegalArgumentException("Can not drain to self");
		
        this.lock.writeLock().lock();
		try
		{
			int drained = 0;

			Iterator<V> mapIterator = this.map.values().iterator();
			while(mapIterator.hasNext() == true)
			{
				V value = mapIterator.next();
				collection.add(value);
				mapIterator.remove();
				drained++;
			}
	
			if (drained > 0)
			{
				this.count -= drained;
				this.notFull.signalAll();
			}

			return drained;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public int drainTo(final Collection<? super V> collection, final int maxElements)
	{
		Objects.requireNonNull(collection, "Collection to drain to is null");
		if (collection == this)
            throw new IllegalArgumentException("Can not drain to self");
		
		Ints.greaterThan(maxElements, 0, "Max elements is less than 1");

		this.lock.writeLock().lock();
		try
		{
			int drained = 0;
			Iterator<V> mapIterator = this.map.values().iterator();
			while(drained < maxElements && mapIterator.hasNext() == true)
			{
				V value = mapIterator.next();
				collection.add(value);
				mapIterator.remove();
				drained++;
			}

			if (drained > 0)
			{
				this.count -= drained;
				this.notFull.signalAll();
			}

			return drained;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public int drainTo(final Map<? super K, ? super V> collection)
	{
		Objects.requireNonNull(collection, "Map to drain to is null");
		if (collection == this)
            throw new IllegalArgumentException("Can not drain to self");
		
        this.lock.writeLock().lock();
		try
		{
			int drained = 0;

			Iterator<Entry<K, V>> mapIterator = this.map.entrySet().iterator();
			while(mapIterator.hasNext() == true)
			{
				Entry<K, V> entry = mapIterator.next();
				collection.put(entry.getKey(), entry.getValue());
				mapIterator.remove();
				drained++;
			}
	
			if (drained > 0)
			{
				this.count -= drained;
				this.notFull.signalAll();
			}

			return drained;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public int drainTo(final Map<? super K, ? super V> collection, final int maxElements)
	{
		Objects.requireNonNull(collection, "Map to drain to is null");
		if (collection == this)
            throw new IllegalArgumentException("Can not drain to self");
		
		Ints.greaterThan(maxElements, 0, "Max elements is less than 1");

		this.lock.writeLock().lock();
		try
		{
			int drained = 0;
			Iterator<Entry<K, V>> mapIterator = this.map.entrySet().iterator();
			while(drained < maxElements && mapIterator.hasNext() == true)
			{
				Entry<K, V> entry = mapIterator.next();
				collection.put(entry.getKey(), entry.getValue());
				mapIterator.remove();
				drained++;
			}

			if (drained > 0)
			{
				this.count -= drained;
				this.notFull.signalAll();
			}

			return drained;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public void put(final K key, final V value)
	{
		Objects.requireNonNull(key, "Key to put is null");
		Objects.requireNonNull(value, "Value to put is null");
		
		this.lock.writeLock().lock();
		try
		{
			while(this.count >= this.capacity)
			{
				if (this.count > this.capacity)
					throw new IllegalStateException("Count "+this.count+" should never be greater than capacity "+this.capacity);
				
				this.notFull.awaitUninterruptibly();
			}
			
			if (this.map.put(key, value) == null)
			{
				this.count++;
				this.notEmpty.signal();
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public V putIfAbsent(final K key, final V value)
	{
		Objects.requireNonNull(key, "Key to put is null");
		Objects.requireNonNull(value, "Value to put is null");
		
		this.lock.writeLock().lock();
		try
		{
			V current = this.map.get(key);
			if (current != null)
				return current;
			
			while(this.count >= this.capacity)
			{
				if (this.count > this.capacity)
					throw new IllegalStateException("Count "+this.count+" should never be greater than capacity "+this.capacity);
				
				this.notFull.awaitUninterruptibly();
			}
			
			if (this.map.put(key, value) == null)
			{
				this.count++;
				this.notEmpty.signal();
				return null;
			}
			
			throw new IllegalStateException("Expected key "+key+" not to be assigned");
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	@SuppressWarnings("unchecked")
	public Collection<K> putAll(final Map<K, V> values)
	{
		Objects.requireNonNull(values, "Map to put is null");
		if (values.isEmpty() == true)
			return Collections.EMPTY_LIST;

		this.lock.writeLock().lock();
		try
		{
			Set<K> puts = new LinkedHashSet<K>();
			for (Entry<K, V> entry : values.entrySet())
			{
				while(this.count >= this.capacity)
				{
					if (this.count > this.capacity)
						throw new IllegalStateException("Count "+this.count+" should never be greater than capacity "+this.capacity);
					
					this.notFull.awaitUninterruptibly();
				}

				if (this.map.put(entry.getKey(), entry.getValue()) == null)
					this.count ++;
				
				puts.add(entry.getKey());
			}

			if (puts.isEmpty() == false)
				this.notEmpty.signalAll();

			return puts;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public boolean offer(final K key, final V value) 
	{
		Objects.requireNonNull(key, "Key to offer is null");
		Objects.requireNonNull(value, "Value to offer is null");

		this.lock.writeLock().lock();
		try
		{
			if (this.count > this.capacity)
				throw new IllegalStateException("Count "+this.count+" should never be greater than capacity "+this.capacity);
			
			if (this.count == this.capacity)
				return false;
			
			if (this.map.put(key, value) == null)
			{
				this.count++;
				this.notEmpty.signal();
			}
			
			return true;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public boolean offer(final K key, final V value, final long timeout, final TimeUnit unit) throws InterruptedException
	{
		Objects.requireNonNull(key, "Key to offer is null");
		Objects.requireNonNull(value, "Value to offer is null");
		Objects.requireNonNull(unit, "Time unit is null");
		Numbers.lessThan(timeout, 1, "Time out is less than 1");
		
		long nanos = unit.toNanos(timeout);
		
		this.lock.writeLock().lockInterruptibly();
		try
		{
			while(this.count >= this.capacity)
			{
				if (this.count > this.capacity)
					throw new IllegalStateException("Count "+this.count+" should never be greater than capacity "+this.capacity);
				
                if (nanos <= 0)
                    return false;
                
                nanos = this.notFull.awaitNanos(nanos);
            }
			
			if (this.map.put(key, value) == null)
			{
				this.count++;
				this.notEmpty.signal();
			}
			
			return true;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public Entry<K,V> peek() 
	{
		this.lock.readLock().lock();
		try
		{
			if (this.count == 0)
				return null;
			
			return this.map.entrySet().iterator().next();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	public Entry<K,V> peek(final long timeout, final TimeUnit unit) throws InterruptedException
	{
		Objects.requireNonNull(unit, "Time unit is null");
		Numbers.lessThan(timeout, 1, "Time out is less than 1");

		long nanos = unit.toNanos(timeout);
		
		// Need writelock as using writelock.conditions
		this.lock.writeLock().lockInterruptibly();
		try
		{
			while (this.count == 0) 
			{
                if (nanos <= 0)
                    return null;

                nanos = this.notEmpty.awaitNanos(nanos);
            }
			
			return this.map.entrySet().iterator().next();
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public Entry<K,V> poll()
	{
		this.lock.writeLock().lock();
		try
		{
			if (this.count == 0)
				return null;

			Entry<K, V> entry = this.map.entrySet().iterator().next();
			V value = this.map.remove(entry.getKey());
			if (value == null || value.equals(entry.getValue()) == false)
				throw new IllegalStateException("Polled value "+entry.getValue().hashCode()+" does not match map value "+value.hashCode());

			this.count--;
			this.notFull.signal();
			return entry;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public Entry<K,V> poll(final long timeout, final TimeUnit unit) throws InterruptedException
	{
		Objects.requireNonNull(unit, "Time unit is null");
		Numbers.lessThan(timeout, 1, "Time out is less than 1");

		long nanos = unit.toNanos(timeout);
		this.lock.writeLock().lockInterruptibly();
		try
		{
			while (this.count == 0) 
			{
                if (nanos <= 0)
                    return null;

                nanos = this.notEmpty.awaitNanos(nanos);
            }
			
			Entry<K, V> entry = this.map.entrySet().iterator().next();
			V value = this.map.remove(entry.getKey());
			if (value == null || value.equals(entry.getValue()) == false)
				throw new IllegalStateException("Polled value "+entry.getValue().hashCode()+" does not match map value "+value.hashCode());
		
			this.count--;
			this.notFull.signal();
			return entry;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public void forEach(final BiConsumer<? super K, ? super V> action)
	{
		Objects.requireNonNull(action, "Biconsumer foreach is null");
		
		this.lock.readLock().lock();
		try
		{
			this.map.forEach(action);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public boolean contains(final K key)
	{
		Objects.requireNonNull(key, "Key to test contains is null");
		
		this.lock.readLock().lock();
		try			
		{
			if (this.map.containsKey(key) == false)
				return false;

			return true;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	public V get(final K key)
	{
		Objects.requireNonNull(key, "Key to get is null");

		this.lock.readLock().lock();
		try			
		{
			return this.map.get(key);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public V remove(final K key) 
	{
		Objects.requireNonNull(key, "Key to remove is null");
		
		this.lock.writeLock().lock();
		try
		{
			V value = this.map.remove(key);
			if (value == null)
				return value;
			
			this.count--;
			this.notFull.signal();
			return value;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public boolean remove(final K key, final V value)
	{
		Objects.requireNonNull(key, "Key to remove is null");
		Objects.requireNonNull(value, "Value to remove is null");
		
		this.lock.writeLock().lock();
		try
		{
			boolean removed = this.map.remove(key, value);
			if (removed == true)
			{
				this.count--;
				this.notFull.signal();
			}
			return removed;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	
	@SuppressWarnings("unchecked")
	public Map<K, V> removeAll(final Collection<K> keys) 
	{
		Objects.requireNonNull(keys, "Key collection to remove is null");
		if (keys.isEmpty() == true)
			return Collections.EMPTY_MAP;
		
		List<Entry<K, V>> removed = new ArrayList<Entry<K, V>>();
		this.lock.writeLock().lock();
		try
		{
			for (K key : keys)
			{
				V value = this.map.remove(key);
				if (value != null)
					removed.add(new AbstractMap.SimpleEntry<>(key, value));
			}

			if (removed.isEmpty() == false)
			{
				this.count -= removed.size();
				this.notFull.signalAll();
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}

		return removed.stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
	}

	@SuppressWarnings("unchecked")
	public Map<K, V> removeAll(final Map<K, V> removals) 
	{
		Objects.requireNonNull(removals, "Map to remove is null");
		if (removals.isEmpty() == true)
			return Collections.EMPTY_MAP;

		List<Entry<K, V>> removed = new ArrayList<Entry<K, V>>();
		this.lock.writeLock().lock();
		try
		{
			for (Entry<K, V> entry : removals.entrySet())
			{
				if (entry.getValue().equals(this.map.get(entry.getKey())) == true)
				{
					V value = this.map.remove(entry.getKey());
					if (value != null)
						removed.add(entry);
					else
						throw new IllegalStateException("Removal precondition of "+entry.getKey()+" satisfied, but produced null result");
				}
			}

			if (removed.isEmpty() == false)
			{
				this.count -= removed.size();
				this.notFull.signalAll();
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
		
		return removed.stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
	}
}
