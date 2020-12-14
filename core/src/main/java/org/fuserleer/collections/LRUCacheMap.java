package org.fuserleer.collections;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCacheMap<K, V> extends LinkedHashMap<K, V> 
{
    /**
	 * 
	 */
	private static final long serialVersionUID = 942954212369300337L;
	
	private int size;

    public LRUCacheMap(int size) 
    {
        super(size, 075f, true);
        this.size = size;
    }

    public LRUCacheMap(int size, boolean LRU) 
    {
        super(size, 075f, LRU);
        this.size = size;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) 
    {
        return size() > size;
    }
}