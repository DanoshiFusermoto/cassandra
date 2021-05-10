package org.fuserleer.ledger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.Universe;
import org.fuserleer.collections.Bloom;
import org.fuserleer.collections.LRUCacheMap;
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.DatabaseException;
import org.fuserleer.events.EventListener;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executor;
import org.fuserleer.executors.ScheduledExecutable;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.events.SyncBlockEvent;
import org.fuserleer.ledger.messages.IdentitiesMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.events.PeerConnectedEvent;
import org.fuserleer.network.peers.filters.StandardPeerFilter;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.Numbers;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public final class ValidatorHandler implements Service
{
	private static final Logger powerLog = Logging.getLogger("power");
	
	static 
	{
		powerLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
//		powerLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
//		powerLog.setLevels(Logging.ERROR | Logging.FATAL);
	}

	/** Vote power maturity delay in blocks **/
	public static long VOTE_POWER_MATURITY = 60;	 
	
	private final Context context;
	private final ValidatorStore validatorStore;
	private final Map<Long, VotePowerBloom> votePowerBloomCache;

	/**
	 * Holds a cache of the currently known identities and those with power.
	 * 
	 * Two are held as we may know that an identity has power, but do not yet know the details of the identity.
	 * 
	 * TODO not a problem now, but may get large in super sized networks (1M+ validators), perhaps hold a subset 
	 */
	private final Set<BLSPublicKey> ownedPowerCache;
	private final Set<BLSPublicKey> identityCache;
	
	private Future<?> houseKeepingTaskFuture = null;
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

	ValidatorHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.votePowerBloomCache = Collections.synchronizedMap(new LRUCacheMap<Long, VotePowerBloom>(this.context.getConfiguration().get("ledger.vote.bloom.cache", 1<<10)));
		this.validatorStore = new ValidatorStore(context);
		this.ownedPowerCache = Collections.synchronizedSet(new HashSet<BLSPublicKey>());
		this.identityCache = Collections.synchronizedSet(new HashSet<BLSPublicKey>());
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			this.validatorStore.start();
			
			Collection<BLSPublicKey> powerOwners = this.validatorStore.getWithPower();
			if (powerOwners.isEmpty() == true)
			{
				long height = 0;
				long power = 1;
				for (BLSPublicKey genode : Universe.getDefault().getGenodes())
				{
					powerLog.info(this.context.getName()+": Setting vote power for genesis node "+genode+":"+ShardMapper.toShardGroup(genode, Universe.getDefault().shardGroupCount())+" to "+power);
					this.validatorStore.set(genode, height, power);
					if (this.validatorStore.get(genode, height) != power)
						throw new IllegalStateException("Genesis node "+genode+" should have vote power of "+power+" @ "+height);
				}
			}
			
			this.validatorStore.store(this.context.getNode().getIdentity());
			
			this.context.getNetwork().getMessaging().register(IdentitiesMessage.class, this.getClass(), new MessageProcessor<IdentitiesMessage> ()
			{
				@Override
				public void process (IdentitiesMessage identitiesMessage, ConnectedPeer peer)
				{
					for (BLSPublicKey identity : identitiesMessage.getIdentities())
					{
						try
						{
							if (ValidatorHandler.this.validatorStore.store(identity) == OperationStatus.SUCCESS)
							{
								if (powerLog.hasLevel(Logging.DEBUG) == true)
									powerLog.debug(ValidatorHandler.this.context.getName()+": Stored identity "+identity);
							}
						}
						catch(IOException ioex)
						{
							powerLog.error(ValidatorHandler.this.context.getName()+": Failed to store identity "+identity, ioex);
						}
					}
					
					identitiesAreDirty();
				}
			});
			
	        // IDENTITIES HOUSEKEEPING //
			this.houseKeepingTaskFuture = Executor.getInstance().scheduleWithFixedDelay(new ScheduledExecutable(60, this.context.getConfiguration().get("network.peers.broadcast.interval", 30), TimeUnit.SECONDS)
			{
				@Override
				public void execute()
				{
 					try
					{
						// Identities refresh to all connected peers
 						Collection<BLSPublicKey> identities = getIdentities();
 						if (identities.isEmpty() == false)
 						{
 							for (ConnectedPeer connectedPeer : ValidatorHandler.this.context.getNetwork().get(StandardPeerFilter.build(ValidatorHandler.this.context).setStates(PeerState.CONNECTED)))
 								ValidatorHandler.this.context.getNetwork().getMessaging().send(new IdentitiesMessage(identities), connectedPeer);
 						}
					}
					catch (Throwable t)
					{
						powerLog.error(ValidatorHandler.this.context.getName()+": Identities update failed", t);
					}
				}
			});

			
			this.context.getEvents().register(this.syncBlockListener);
			this.context.getEvents().register(this.peerListener);
		}
		catch (Exception ex)
		{
			throw new StartupException(ex);
		}
	}

	@Override
	public void stop() throws TerminationException
	{
		if (this.houseKeepingTaskFuture != null)
			this.houseKeepingTaskFuture.cancel(false);

		this.context.getEvents().unregister(this.peerListener);
		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getNetwork().getMessaging().deregisterAll(getClass());
		this.validatorStore.stop();
		this.ownedPowerCache.clear();
		this.identityCache.clear();
	}
	
	void clean() throws DatabaseException
	{
		this.validatorStore.clean();
	}
	
	// NOTE Can use the identity cache directly providing that access outside of this function
	// wraps the returned set in a lock or a sync block
	private Collection<BLSPublicKey> getPowerOwners() throws DatabaseException
	{
		synchronized(this.ownedPowerCache)
		{
			if (this.ownedPowerCache.isEmpty())
				this.ownedPowerCache.addAll(this.validatorStore.getWithPower());
			
			return Collections.unmodifiableCollection(new ArrayList<BLSPublicKey>(this.ownedPowerCache));
		}
	}
	
	private void powerOwnersCacheIsDirty()
	{
		this.ownedPowerCache.clear();
	}
	
	public Collection<Entry<BLSPublicKey, Long>> getVotePowers() throws DatabaseException
	{
		List<Entry<BLSPublicKey, Long>> votePowers = new ArrayList<Entry<BLSPublicKey, Long>>();
		for (BLSPublicKey identity : getPowerOwners())
		{
			long votePower = getVotePower(this.context.getLedger().getHead().getHeight(), identity);
			if (votePower == 0)
				continue;
			
			votePowers.add(new AbstractMap.SimpleEntry<BLSPublicKey, Long>(identity, votePower));
		}
		
		Collections.sort(votePowers, new Comparator<Entry<BLSPublicKey, Long>>() 
		{
			@Override
			public int compare(Entry<BLSPublicKey, Long> arg0, Entry<BLSPublicKey, Long> arg1)
			{
				if (arg0.getValue() > arg1.getValue())
					return -1;
				if (arg0.getValue() < arg1.getValue())
					return 1;
				return 0;
			}
		});
		
		return votePowers;
	}

	// NOTE Can use the identity cache directly providing that access outside of this function
	// wraps the returned set in a lock or a sync block
	public Collection<BLSPublicKey> getIdentities() throws DatabaseException
	{
		synchronized(this.identityCache)
		{
			if (this.identityCache.isEmpty())
				this.identityCache.addAll(this.validatorStore.getIdentities());
			
			return Collections.unmodifiableCollection(new ArrayList<BLSPublicKey>(this.identityCache));
		}
	}
	
	private void identitiesAreDirty()
	{
		this.identityCache.clear();
	}

	public long getVotePower(final long height, final BLSPublicKey owner) throws DatabaseException
	{
		Objects.requireNonNull(owner, "Identity is null");
		Numbers.isNegative(height, "Height is negative");

		this.lock.readLock().lock();
		try
		{
			return this.validatorStore.get(owner, height);
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
			for (BLSPublicKey powerOwner : getPowerOwners())
			{
				if (shardGroup != ShardMapper.toShardGroup(powerOwner, this.context.getLedger().numShardGroups(height)))
					continue;

				totalVotePower += getVotePower(height, powerOwner); 
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
			for (BLSPublicKey powerOwner : getPowerOwners())
			{
				if (shardGroups.contains(ShardMapper.toShardGroup(powerOwner, this.context.getLedger().numShardGroups(height))) == false)
					continue;

				totalVotePower += getVotePower(height, powerOwner); 
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
		
		this.lock.readLock().lock();
		try
		{
			final Collection<BLSPublicKey> allPowerOwners = getPowerOwners();
			final Collection<BLSPublicKey> shardGroupPowerOwners = new HashSet<BLSPublicKey>();
			for (BLSPublicKey powerOwner : allPowerOwners)
			{
				if (shardGroup != ShardMapper.toShardGroup(powerOwner, this.context.getLedger().numShardGroups(height)))
					continue;

				shardGroupPowerOwners.add(powerOwner);
			}
			
			votePowerBloom = new VotePowerBloom(block, shardGroup, shardGroupPowerOwners.size());
			for (BLSPublicKey powerOwner : shardGroupPowerOwners)
				votePowerBloom.add(powerOwner, getVotePower(height-1, powerOwner));
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

	public long getVotePower(final long height, final Bloom owners) throws DatabaseException
	{
		Objects.requireNonNull(owners, "Identities is null");

		this.lock.readLock().lock();
		try
		{
			long votePower = 0l;
			for (BLSPublicKey powerOwner : getPowerOwners())
			{
				if (owners.contains(powerOwner.toByteArray()) == true)
					votePower += getVotePower(height, powerOwner);
			}
			
			return votePower;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public long getVotePower(final long height, final Set<BLSPublicKey> owners) throws DatabaseException
	{
		Objects.requireNonNull(owners, "Identities is null");

		this.lock.readLock().lock();
		try
		{
			long votePower = 0l;
			for (BLSPublicKey powerOwner : getPowerOwners())
				votePower += getVotePower(height, powerOwner);
			
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
	
	void setVotePower(final long height, final BLSPublicKey owner, final long power) throws DatabaseException
	{
		Objects.requireNonNull(owner, "Owner is null");

		this.lock.writeLock().lock();
		try
		{
			this.validatorStore.set(owner, height, power);
		}			
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	long incrementVotePower(final long height, final BLSPublicKey owner) throws DatabaseException
	{
		Objects.requireNonNull(owner, "Power owner is null");

		this.lock.writeLock().lock();
		try
		{
			this.validatorStore.increment(owner, height);
			long power = this.validatorStore.get(owner, height);
			powerLog.info(this.context.getName()+": Incrementing vote power for "+owner+":"+ShardMapper.toShardGroup(owner, this.context.getLedger().numShardGroups(height))+" to "+power+" @ "+height);
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
			powerOwnersCacheIsDirty();
			
			incrementVotePower(block.getHeader().getHeight(), block.getHeader().getOwner());

			long numLocalShardGroups = this.context.getLedger().numShardGroups(block.getHeader().getHeight());
			long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numLocalShardGroups);
			Multimap<Long, BLSPublicKey> shardGroupNodes = HashMultimap.create();
			Map<Long, Long> updates = new HashMap<Long, Long>();

			// Phase 1 process into map to avoid duplicates / overwrites hitting IO efficiency
			for (AtomCertificate atomCertificate : block.getCertificates())
			{
				for (StateCertificate stateCertificate : atomCertificate.getAll())
				{
					long numRemoteShardGroups = this.context.getLedger().numShardGroups(stateCertificate.getHeight());
					long stateShardGroup = ShardMapper.toShardGroup(stateCertificate.getState().get(), numRemoteShardGroups);
					shardGroupNodes.putAll(stateShardGroup, this.validatorStore.get(stateShardGroup, numRemoteShardGroups));
					
					if (localShardGroup != stateShardGroup)
						this.validatorStore.store(stateCertificate.getProducer());
				}
				
				for (VotePowerBloom votePowerBloom : atomCertificate.getVotePowers())
				{
					if (votePowerBloom.getShardGroup() != localShardGroup)
					{
						long height = Longs.fromByteArray(votePowerBloom.getBlock().toByteArray())-1;
						for (BLSPublicKey identity : shardGroupNodes.get(votePowerBloom.getShardGroup()))
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
						for (BLSPublicKey identity : shardGroupNodes.get(votePowerBloom.getShardGroup()))
						{
							long height = Longs.fromByteArray(votePowerBloom.getBlock().toByteArray())-1;
							long key = identity.asLong() * (31l + height);
							if (updates.containsKey(key) == true)
							{
								long power = updates.remove(key);
								if (this.validatorStore.set(identity, height, power) != power)
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
	
	// PEER LISTENER //
	private EventListener peerListener = new EventListener()
	{
		@Subscribe
		public void on(PeerConnectedEvent peerConnectedEvent) 
		{
			try
			{
				if (ValidatorHandler.this.validatorStore.store(peerConnectedEvent.getPeer().getNode().getIdentity()).equals(OperationStatus.SUCCESS) == true)
				{
					if (powerLog.hasLevel(Logging.DEBUG) == true)
						powerLog.debug(ValidatorHandler.this.context.getName()+": Stored identity "+peerConnectedEvent.getPeer().getNode().getIdentity());
					ValidatorHandler.this.identityCache.add(peerConnectedEvent.getPeer().getNode().getIdentity());
				}
			}
			catch (IOException ioex)
			{
				powerLog.error(ValidatorHandler.this.context.getName()+": Failed to store node identity "+peerConnectedEvent.getPeer().getNode().getIdentity(), ioex);
			}
			
			try
			{
				Collection<BLSPublicKey> identities = getIdentities();
				if (identities.isEmpty() == false)
					ValidatorHandler.this.context.getNetwork().getMessaging().send(new IdentitiesMessage(identities), peerConnectedEvent.getPeer());
			}
			catch (IOException ioex)
			{
				powerLog.error(ValidatorHandler.this.context.getName()+": Failed to send node identities to "+peerConnectedEvent.getPeer(), ioex);
			}
		}
	};

	// BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent) 
		{
			try
			{
				update(blockCommittedEvent.getBlock());
			}
			catch (IOException ioex)
			{
				powerLog.error(ValidatorHandler.this.context.getName()+": Failed to update vote powers in block "+blockCommittedEvent.getBlock().getHeader(), ioex);
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
				powerLog.error(ValidatorHandler.this.context.getName()+": Failed to update vote powers in block "+event.getBlock().getHeader(), ioex);
			}
		}
	};
}
