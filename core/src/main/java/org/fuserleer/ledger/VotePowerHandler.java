package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.Universe;
import org.fuserleer.collections.LRUCacheMap;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.DatabaseException;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.events.SyncBlockEvent;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.Numbers;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.Subscribe;

// TODO Needs persistence methods.  Storing this in memory over long periods will consume a lot of space.  Recovery and rebuilding it from a crash would also be quite the nightmare.
public final class VotePowerHandler implements Service
{
	private static final Logger powerLog = Logging.getLogger("power");

	/** Vote power maturity delay in blocks **/
	public static long VOTE_POWER_MATURITY = 60;	 
	
	private final Context context;
	private final VotePowerStore votePowerStore;
	private final Map<Long, VotePowerBloom> votePowerBloomCache;

	/**
	 * Holds a cache of the currently known identities.
	 * 
	 * TODO not a problem now, but may get large in super sized networks (1M+ validators), perhaps hold a subset 
	 */
	private final Set<ECPublicKey> identityCache;
	
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

	VotePowerHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.votePowerBloomCache = Collections.synchronizedMap(new LRUCacheMap<Long, VotePowerBloom>(this.context.getConfiguration().get("ledger.vote.bloom.cache", 1<<10)));
		this.votePowerStore = new VotePowerStore(context);
		this.identityCache = Collections.synchronizedSet(new HashSet<ECPublicKey>());
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			this.votePowerStore.start();
			
			Collection<ECPublicKey> identities = this.votePowerStore.getIdentities();
			if (identities.isEmpty() == true)
			{
				long height = 0;
				long power = 1;
				for (ECPublicKey genode : Universe.getDefault().getGenodes())
				{
					powerLog.info(this.context.getName()+": Setting vote power for genesis node "+genode+":"+ShardMapper.toShardGroup(genode, Universe.getDefault().shardGroupCount())+" to "+1);
					this.votePowerStore.set(genode, height, power);
					if (this.votePowerStore.get(genode, height) != power)
						throw new IllegalStateException("Genesis node "+genode+" should have vote power of "+power+" @ "+height);
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
		this.votePowerStore.stop();
		this.identityCache.clear();
	}
	
	void clean() throws DatabaseException
	{
		this.votePowerStore.clean();
	}
	
	// NOTE Can use the identity cache directly providing that access outside of this function
	// wraps the returned set in a lock or a sync block
	private Set<ECPublicKey> getIdentities() throws DatabaseException
	{
		synchronized(this.identityCache)
		{
			if (this.identityCache.isEmpty())
				this.identityCache.addAll(this.votePowerStore.getIdentities());
			
			return this.identityCache;
		}
	}
	
	private void identitiesAreDirty()
	{
		this.identityCache.clear();
	}

	public long getVotePower(final long height, final ECPublicKey identity) throws DatabaseException
	{
		Objects.requireNonNull(identity, "Identity is null");
		Numbers.notNegative(height, "Height is negative");

		this.lock.readLock().lock();
		try
		{
			return this.votePowerStore.get(identity, height);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	public long getTotalVotePower(final long height, final long shardGroup) throws DatabaseException
	{
		this.lock.readLock().lock();
		try
		{
			long totalVotePower = 0;
			for (ECPublicKey identity : getIdentities())
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

	public long getTotalVotePower(final long height, final Set<Long> shardGroups) throws DatabaseException
	{
		Objects.requireNonNull(shardGroups, "Shard groups is null");

		this.lock.readLock().lock();
		try
		{
			long totalVotePower = 0;
			for (ECPublicKey identity : getIdentities())
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
	
	public VotePowerBloom getVotePowerBloom(final Hash block, long shardGroup) throws DatabaseException
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
			for (ECPublicKey identity : getIdentities())
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

	public long getVotePower(final long height, final Set<ECPublicKey> identities) throws DatabaseException
	{
		Objects.requireNonNull(identities, "Identities is null");

		this.lock.readLock().lock();
		try
		{
			long votePower = 0l;
			for (ECPublicKey identity : getIdentities())
				votePower += getVotePower(height, identity);
			
			return votePower;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public long getVotePowerThreshold(final long height, final long shardGroup) throws DatabaseException
	{
		return twoFPlusOne(getTotalVotePower(height, shardGroup));
	}
	
	public long getVotePowerThreshold(final long height, final Set<Long> shardGroups) throws DatabaseException
	{
		Objects.requireNonNull(shardGroups, "Shard groups is null");

		return twoFPlusOne(getTotalVotePower(height, shardGroups));
	}
	
	void setVotePower(final long height, final ECPublicKey identity, final long power) throws DatabaseException
	{
		Objects.requireNonNull(identity, "Identity is null");

		this.lock.writeLock().lock();
		try
		{
			this.votePowerStore.set(identity, height, power);
		}			
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	long incrementVotePower(final long height, final ECPublicKey identity) throws DatabaseException
	{
		Objects.requireNonNull(identity, "Identity is null");

		this.lock.writeLock().lock();
		try
		{
			this.votePowerStore.increment(identity, height);
			long power = this.votePowerStore.get(identity, height);
			powerLog.info(this.context.getName()+": Incrementing vote power for "+identity+":"+ShardMapper.toShardGroup(identity, this.context.getLedger().numShardGroups(height))+" to "+power+" @ "+height);
			return power;
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
	
	void update(final Block block) throws IOException
	{
		Objects.requireNonNull(block, "Block for vote update is null");

		this.lock.writeLock().lock();
		try
		{
			identitiesAreDirty();
			
			incrementVotePower(block.getHeader().getHeight(), block.getHeader().getOwner());

			long numShardGroups = this.context.getLedger().numShardGroups(block.getHeader().getHeight());
			long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			Multimap<Long, ECPublicKey> shardGroupNodes = HashMultimap.create();
			Map<Long, Long> updates = new HashMap<Long, Long>();

			// Phase 1 process into map to avoid duplicates / overwrites hitting IO efficiency
			for (AtomCertificate atomCertificate : block.getCertificates())
			{
				for (StateCertificate stateCertificate : atomCertificate.getAll())
					shardGroupNodes.putAll(ShardMapper.toShardGroup(stateCertificate.getState().get(), numShardGroups), stateCertificate.getSignatures().getSigners());
				
				for (VotePowerBloom votePowerBloom : atomCertificate.getVotePowers())
				{
					if (votePowerBloom.getShardGroup() != localShardGroup)
					{
						long height = Longs.fromByteArray(votePowerBloom.getBlock().toByteArray())-1;
						for (ECPublicKey identity : shardGroupNodes.get(votePowerBloom.getShardGroup()))
						{
							long power = votePowerBloom.power(identity);
							long key = identity.asLong() * (31l + height);
							if (updates.containsKey(key) == false || updates.get(key) < power)
								updates.put(key, power);
						}

					}
				}
			}
			
			// Phase 2 process the update map and perform IO
			for (AtomCertificate atomCertificate : block.getCertificates())
			{
				for (VotePowerBloom votePowerBloom : atomCertificate.getVotePowers())
				{
					if (votePowerBloom.getShardGroup() != localShardGroup)
					{
						for (ECPublicKey identity : shardGroupNodes.get(votePowerBloom.getShardGroup()))
						{
							long height = Longs.fromByteArray(votePowerBloom.getBlock().toByteArray())-1;
							long key = identity.asLong() * (31l + height);
							if (updates.containsKey(key) == true)
							{
								long power = updates.remove(key);
								if (this.votePowerStore.set(identity, height, power) != power)
									powerLog.info(this.context.getName()+": Setting vote power for "+identity+":"+votePowerBloom.getShardGroup()+" to "+power+" @ "+height);
							}
						}

					}
				}
			}
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
