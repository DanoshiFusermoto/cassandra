package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.Universe;
import org.fuserleer.collections.LRUCacheMap;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.events.SyncBlockEvent;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.Subscribe;
import com.google.common.primitives.Longs;

// TODO Needs persistence methods.  Storing this in memory over long periods will consume a lot of space.  Recovery and rebuilding it from a crash would also be quite the nightmare.
public final class VotePowerHandler implements Service
{
	private static final Logger powerLog = Logging.getLogger("power");

	/** Vote power maturity delay in blocks **/
	public static long VOTE_POWER_MATURITY = 60;	 
	
	private final Context context;
	private final VotePowerStore votePowerStore;
	private final Map<ECPublicKey, Map<Long, Long>> votePower;
	private final Map<Long, VotePowerBloom> votePowerBloomCache;
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

	VotePowerHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.votePower = new HashMap<ECPublicKey, Map<Long, Long>>();
		this.votePowerBloomCache = Collections.synchronizedMap(new LRUCacheMap<Long, VotePowerBloom>(this.context.getConfiguration().get("ledger.vote.bloom.cache", 1<<10)));
		this.votePowerStore = new VotePowerStore(context);
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			this.votePower.clear();
			
			this.votePowerStore.start();
			Collection<ECPublicKey> identities = this.votePowerStore.getIdentities();
			if (identities.isEmpty() == false)
			{
				for (ECPublicKey identity : identities)
				{
					Map<Long, Long> powerMap = this.votePowerStore.get(identity);
					Iterator<Long> powerHeightIterator = powerMap.keySet().iterator();
					while(powerHeightIterator.hasNext() == true)
					{
						long powerMapHeight = powerHeightIterator.next();
						if (powerMapHeight > this.context.getLedger().getHead().getHeight())
							powerHeightIterator.remove();
					}
					
					this.votePower.put(identity, powerMap);
					powerLog.info(this.context.getName()+": Loaded vote power for "+identity+":"+getVotePower(this.context.getLedger().getHead().getHeight(), identity));
				}
			}
			else
			{
				for (ECPublicKey genode : Universe.getDefault().getGenodes())
				{
					powerLog.info(this.context.getName()+": Setting vote power for genesis node "+genode+":"+ShardMapper.toShardGroup(genode, Universe.getDefault().shardGroupCount())+" to "+1);
					Map<Long, Long> powerMap = new HashMap<>();
					powerMap.put(0l, 1l);
					this.votePower.put(genode, powerMap);
					this.votePowerStore.store(genode, powerMap);
				}
			}
			
			this.context.getEvents().register(this.syncBlockListener);
		}
		catch (Exception ex)
		{
			throw new StartupException(ex);
		}
	}

	@Override
	public void stop() throws TerminationException
	{
		this.context.getEvents().unregister(this.syncBlockListener);
	}
	
	public long getVotePower(final long height, final ECPublicKey identity)
	{
		Objects.requireNonNull(identity, "Identity is null");

		this.lock.readLock().lock();
		try
		{
			Map<Long, Long> powerMap = this.votePower.get(identity);
			if (powerMap == null)
				return 0l;
			
			long power = 0;
			if (powerMap.containsKey(height))
				power = powerMap.getOrDefault(Math.max(0, height), 0l);
			else
			{
				// FIXME this section makes sure that an accurate power is returned in the event of missing information by
				//		 returning the highest power seen up to a particular height.  It's very expensive though.
				//		 Evaluate if absolutely required and if so, craft a more efficient method.
				for (Entry<Long, Long> entry : powerMap.entrySet())
					if (entry.getKey() <= Math.max(0, height) && entry.getValue() > power)
						power = entry.getValue();
			}
			
			return power;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	public long getTotalVotePower(final long height, final long shardGroup)
	{
		this.lock.readLock().lock();
		try
		{
			long totalVotePower = 0;
			for (ECPublicKey identity : this.votePower.keySet())
			{
				if (shardGroup != ShardMapper.toShardGroup(identity, this.context.getLedger().numShardGroups(height)))
					continue;

				totalVotePower += getVotePower(height, identity); 
			}
			return totalVotePower;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public long getTotalVotePower(final long height, final Set<Long> shardGroups)
	{
		Objects.requireNonNull(shardGroups, "Shard groups is null");

		this.lock.readLock().lock();
		try
		{
			long totalVotePower = 0;
			for (ECPublicKey identity : this.votePower.keySet())
			{
				if (shardGroups.contains(ShardMapper.toShardGroup(identity, this.context.getLedger().numShardGroups(height))) == false)
					continue;

				totalVotePower += getVotePower(height, identity); 
			}
			return totalVotePower;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	public VotePowerBloom getVotePowerBloom(final Hash block, long shardGroup)
	{
		Objects.requireNonNull(block, "Block hash is null");
		Objects.requireNonNull(shardGroup, "Shard group is null");

		VotePowerBloom votePowerBloom;
		long height = Longs.fromByteArray(block.toByteArray());
		
		votePowerBloom = this.votePowerBloomCache.get(height + shardGroup);
		if (votePowerBloom != null)
			return votePowerBloom;
		
		votePowerBloom = new VotePowerBloom(block, shardGroup);

		this.lock.readLock().lock();
		try
		{
			for (ECPublicKey identity : this.votePower.keySet())
			{
				if (shardGroup != ShardMapper.toShardGroup(identity, this.context.getLedger().numShardGroups(height)))
					continue;

				votePowerBloom.add(identity, getVotePower(height-1, identity));
			}
		}
		finally
		{
			this.lock.readLock().unlock();
		}

		// Only cache blocks that are committed
		// TODO need to flush cache on reorgs
		if (height <= this.context.getLedger().getHead().getHeight())
			this.votePowerBloomCache.putIfAbsent(height + shardGroup, votePowerBloom);

		return votePowerBloom;
	}

	public long getVotePower(final long height, final Set<ECPublicKey> identities)
	{
		Objects.requireNonNull(identities, "Identities is null");

		this.lock.readLock().lock();
		try
		{
			long votePower = 0l;
			for (ECPublicKey identity : identities)
				votePower += getVotePower(height, identity);
			
			return votePower;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public long getVotePowerThreshold(final long height, final long shardGroup)
	{
		return twoFPlusOne(getTotalVotePower(height, shardGroup));
	}
	
	public long getVotePowerThreshold(final long height, final Set<Long> shardGroups)
	{
		Objects.requireNonNull(shardGroups, "Shard groups is null");

		return twoFPlusOne(getTotalVotePower(height, shardGroups));
	}
	
	long addVotePower(final long height, final ECPublicKey identity)
	{
		Objects.requireNonNull(identity, "Identity is null");

		this.lock.writeLock().lock();
		try
		{
			final long prevPower;
			final long prevPowerHeight;
			Map<Long, Long> powerMap = this.votePower.computeIfAbsent(identity, (k) -> new HashMap<Long, Long>());
			if (powerMap.isEmpty() == false)
			{
				long bestPrevPower = -1;
				long bestPrevPowerHeight = -1;
				for (Entry<Long, Long> pmh : powerMap.entrySet())
				{
					if (pmh.getKey() < height && pmh.getKey() > bestPrevPowerHeight)
					{
						bestPrevPowerHeight = pmh.getKey();
						bestPrevPower = pmh.getValue();
					}
				}
				
				prevPower = bestPrevPower;
				prevPowerHeight = bestPrevPowerHeight;
			}
			else
			{
				prevPower = 0;
				prevPowerHeight = 0;
			}

			long bestPower = 0;
			for (long h : powerMap.keySet())
			{
				if (h > height)
				{
					long bp = powerMap.compute(h, (k,v) -> v == null ? prevPower+1l : v+1l);
					if (bp > bestPower)
						bestPower = bp;
				}
			}
			
			for (long h = prevPowerHeight ; h < height ; h++)
				powerMap.computeIfAbsent(h, (k) -> prevPower);

			long bp = powerMap.compute(height, (k,v) -> v == null ? prevPower+1l : v+1l);
			if (bp > bestPower)
				bestPower = bp;

			powerLog.info(this.context.getName()+": Incrementing vote power for "+identity+":"+ShardMapper.toShardGroup(identity, this.context.getLedger().numShardGroups(height))+" to "+bestPower);
			return bestPower;
		}			
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	private long twoFPlusOne(long power)
	{
		long F = Math.max(1, power / 3);
		long T = F * 2;
		return Math.min(power, T + 1);
	}
	
	private Set<ECPublicKey> processVoteBloom(final Collection<ECPublicKey> identities, final VotePowerBloom votePowerBloom)
	{
		Objects.requireNonNull(votePowerBloom, "Vote power bloom is null");
		Objects.requireNonNull(identities, "Identities is null");

		this.lock.writeLock().lock();
		try
		{
			Set<ECPublicKey> updated = new HashSet<ECPublicKey>();
			for (ECPublicKey identity : identities)
			{
				long power = votePowerBloom.power(identity);
				Map<Long, Long> powerMap = this.votePower.computeIfAbsent(identity, (k) -> new HashMap<Long, Long>());
				if (power > powerMap.getOrDefault(Longs.fromByteArray(votePowerBloom.getBlock().toByteArray())-1, 0l))
				{
					powerMap.put(Longs.fromByteArray(votePowerBloom.getBlock().toByteArray())-1, power);
					updated.add(identity);
					powerLog.info(this.context.getName()+": Setting vote power for "+identity+":"+votePowerBloom.getShardGroup()+" to "+power);
				}
			}
			return updated;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	void update(final Block block) throws IOException
	{
		Objects.requireNonNull(block, "Block for vote update is null");

		this.lock.writeLock().lock();
		try
		{
			Set<ECPublicKey> updates = new HashSet<ECPublicKey>();
			addVotePower(block.getHeader().getHeight(), block.getHeader().getOwner());
			updates.add(block.getHeader().getOwner());

			long numShardGroups = this.context.getLedger().numShardGroups(block.getHeader().getHeight());
			long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			for (AtomCertificate atomCertificate : block.getCertificates())
			{
				Multimap<Long, ECPublicKey> shardGroupNodes = HashMultimap.create();
				for (StateCertificate stateCertificate : atomCertificate.getAll())
					shardGroupNodes.putAll(ShardMapper.toShardGroup(stateCertificate.getState().get(), numShardGroups), stateCertificate.getSignatures().getSigners());
				
				for (VotePowerBloom votePowerBloom : atomCertificate.getVotePowers())
				{
					if (votePowerBloom.getShardGroup() != localShardGroup)
						updates.addAll(processVoteBloom(shardGroupNodes.get(votePowerBloom.getShardGroup()), votePowerBloom));
				}
			}
			
			for (ECPublicKey identity : updates)
				this.votePowerStore.store(identity, this.votePower.get(identity));
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	// BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(BlockCommittedEvent event) 
		{
			try
			{
				update(event.getBlock());
			}
			catch (IOException ioex)
			{
				powerLog.error(VotePowerHandler.this.context.getName()+": Failed to update vote powers in block "+event.getBlock().getHeader(), ioex);
			}
		}
		
		@Subscribe
		public void on(final SyncBlockEvent event) 
		{
			try
			{
				update(event.getBlock());
			}
			catch (IOException ioex)
			{
				powerLog.error(VotePowerHandler.this.context.getName()+": Failed to update vote powers in block "+event.getBlock().getHeader(), ioex);
			}
		}
	};
}
