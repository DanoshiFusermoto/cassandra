package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.collections.MappedBlockingQueue;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomAcceptedEvent;
import org.fuserleer.ledger.events.AtomAcceptedTimeoutEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomPersistedEvent;
import org.fuserleer.ledger.events.SyncStatusChangeEvent;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.GossipFetcher;
import org.fuserleer.network.GossipFilter;
import org.fuserleer.network.GossipInventory;
import org.fuserleer.network.GossipReceiver;
import org.fuserleer.time.Time;
import org.fuserleer.utils.CustomInteger;
import org.fuserleer.utils.Longs;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public final class AtomPool implements Service
{
	private static final Logger atomsLog = Logging.getLogger("atoms");

	// TODO temporary should be private
	protected enum AtomVoteStatus
	{
		SUCCESS, FAILED, SKIPPED;
	}

	private final static int 	NUM_BUCKETS = 4096;
	private final static long 	BUCKET_SPAN = -(Long.MIN_VALUE / 2) / (NUM_BUCKETS / 4);

	private Executable voteProcessor = new Executable()
	{
		@Override
		public void execute()
		{
			try 
			{
				while (this.isTerminated() == false)
				{
					try
					{
						synchronized(AtomPool.this.voteProcessor)
						{
							AtomPool.this.voteProcessor.wait(TimeUnit.SECONDS.toMillis(1));
						}
	
						if (AtomPool.this.context.getLedger().isSynced() == false)
							continue;

						Multimap<AtomVote, Long> atomVotesToBroadcast = HashMultimap.create();
						if (AtomPool.this.votesToCountQueue.isEmpty() == false)
						{
							Entry<Hash, AtomVote> atomVote;
							while((atomVote = AtomPool.this.votesToCountQueue.peek()) != null)
							{
								try
								{
									AtomVoteStatus status = process(atomVote.getValue()); 
									if (status.equals(AtomVoteStatus.SUCCESS) == true)
									{
										PendingAtom pendingAtom = AtomPool.this.context.getLedger().getAtomHandler().get(atomVote.getValue().getAtom());
										if (pendingAtom != null)
										{
											if (pendingAtom.getStatus().greaterThan(CommitStatus.NONE) == true)
											{
												Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), AtomPool.this.context.getLedger().numShardGroups());
												atomVotesToBroadcast.putAll(atomVote.getValue(), shardGroups);
											}
										}
										else
											atomsLog.warn(AtomPool.this.context.getName()+": Pending atom "+atomVote.getValue().getAtom()+" is gone after applying vote "+atomVote.getValue()+" from "+atomVote.getValue().getOwner());
									}
									else if (status.equals(AtomVoteStatus.SKIPPED) == true)
									{
										if (atomsLog.hasLevel(Logging.DEBUG) == true)
											atomsLog.debug(AtomPool.this.context.getName()+": Processing of atom vote "+atomVote.getValue().getHash()+" was skipped for atom "+atomVote.getValue().getAtom()+" by "+atomVote.getValue().getOwner());
									}
									else
										atomsLog.warn(AtomPool.this.context.getName()+": Processing of atom vote "+atomVote.getValue().getHash()+" failed for atom "+atomVote.getValue().getAtom()+" by "+atomVote.getValue().getOwner());
								}
								catch (Exception ex)
								{
									atomsLog.error(AtomPool.this.context.getName()+": Error counting vote for "+atomVote.getValue(), ex);
								}
								finally
								{
									if (AtomPool.this.votesToCountQueue.remove(atomVote.getKey(), atomVote.getValue()) == false)
										// FIXME sync state can initially flip/flop between ... annoying, so just throw these as warns for now (theres are in all queue handlers!)
										atomsLog.warn(AtomPool.this.context.getName()+": Atom pool vote peek/remove failed for "+atomVote.getValue());
//										throw new IllegalStateException("Atom pool vote peek/remove failed for "+atomVote.getValue());
								}
							}
						}
						
						if (AtomPool.this.votesToCastQueue.isEmpty() == false)
						{
							PendingAtom pendingAtom;
							while((pendingAtom = AtomPool.this.votesToCastQueue.peek()) != null)
							{
								try
								{
									// Dont vote if we have no power!
									long localVotePower = AtomPool.this.context.getLedger().getValidatorHandler().getVotePower(Math.max(0, AtomPool.this.context.getLedger().getHead().getHeight() - ValidatorHandler.VOTE_POWER_MATURITY), AtomPool.this.context.getNode().getIdentity());
									if (localVotePower > 0)
									{
										AtomVote atomVote = new AtomVote(pendingAtom.getHash(), AtomPool.this.context.getNode().getIdentity());
										atomVote.sign(AtomPool.this.context.getNode().getKeyPair());
	
										AtomVoteStatus status = process(atomVote); 
										if (status.equals(AtomVoteStatus.SUCCESS) == true)
										{
											if (atomsLog.hasLevel(Logging.DEBUG))
												atomsLog.debug(AtomPool.this.context.getName()+": Voted on atom "+atomVote.getAtom()+" with vote "+atomVote.getHash());
										}
										else
											atomsLog.warn(AtomPool.this.context.getName()+": Persistance of local atom vote failed "+atomVote.getHash()+":"+atomVote.getAtom()+" for "+atomVote.getOwner()+" (did out of sync recently happen?)");
									}										
									
									// Broadcast here even if local validator doesn't vote, as may be pending broadcasts from votes counted.
									// Pending broadcasts can not be sent before the Atom is prepared and the shards are known.
									// If the local validator is attempting to vote, then it knows the shards required.
									// TODO this might be causing duplicates in the gossip
									Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), AtomPool.this.context.getLedger().numShardGroups());
									for (AtomVote atomVote : pendingAtom.votes())
										atomVotesToBroadcast.putAll(atomVote, shardGroups);
								}
								catch (Exception ex)
								{
									atomsLog.error(AtomPool.this.context.getName()+": Error processing vote for " + pendingAtom.getHash(), ex);
								}
								finally
								{
									if (pendingAtom.equals(AtomPool.this.votesToCastQueue.poll()) == false)
										// FIXME sync state can initially flip/flop between ... annoying, so just throw these as warns for now (theres are in all queue handlers!)
										atomsLog.warn(AtomPool.this.context.getName()+": Atom pool cast vote peek/remove failed for "+pendingAtom.getHash());
//										throw new IllegalStateException("Atom pool vote cast peek/pool failed for "+pendingAtom.getHash());
								}
							}
						}
						
						try
						{
							if (atomVotesToBroadcast.isEmpty() == false)
							{
								Collection<AtomVote> atomVotes = atomVotesToBroadcast.keySet();
								AtomPool.this.context.getMetaData().increment("ledger.pool.atom.votes", atomVotes.size());
								for (AtomVote atomVote : atomVotes)
									AtomPool.this.context.getNetwork().getGossipHandler().broadcast(atomVote, atomVotesToBroadcast.get(atomVote));
							}
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomPool.this.context.getName()+": Error broadcasting atom votes "+atomVotesToBroadcast, ex);
						}
					} 
					catch (InterruptedException e) 
					{
						// DO NOTHING //
						continue;
					}
				}
			}
			catch (Throwable throwable)
			{
				// TODO want to actually handle this?
				atomsLog.fatal(AtomPool.this.context.getName()+": Error processing atom queue", throwable);
			}
		}
	};
	
	private final Context context;

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	private final Map<Hash, PendingAtom> pending = new HashMap<Hash, PendingAtom>();
	private final Map<Long, Set<PendingAtom>> buckets = new HashMap<Long, Set<PendingAtom>>();

	private final BlockingQueue<PendingAtom> votesToCastQueue;
	private final MappedBlockingQueue<Hash, AtomVote> votesToCountQueue;
	
	public AtomPool(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.votesToCastQueue = new LinkedBlockingQueue<PendingAtom>(this.context.getConfiguration().get("ledger.atom.queue", 1<<16));
		this.votesToCountQueue = new MappedBlockingQueue<Hash, AtomVote>(this.context.getConfiguration().get("ledger.atom.queue", 1<<16));

		long location = Long.MIN_VALUE;
		for (int b = 0 ; b <= NUM_BUCKETS ; b++)
		{
			long bucket = mapToBucket(location);
			this.buckets.put(bucket, new HashSet<PendingAtom>());
			location += BUCKET_SPAN;
		}
	}

	@Override
	public void start() throws StartupException
	{
		this.context.getNetwork().getGossipHandler().register(AtomVote.class, new GossipFilter(this.context) 
		{
			@Override
			public Set<Long> filter(Primitive object) throws Throwable
			{
				Set<Long> shardGroups = new HashSet<Long>();
				PendingAtom pendingAtom = AtomPool.this.context.getLedger().getAtomHandler().get(((AtomVote)object).getAtom());
				shardGroups.addAll(ShardMapper.toShardGroups(pendingAtom.getShards(), AtomPool.this.context.getLedger().numShardGroups()));
				return shardGroups;
			}
		});
		
		this.context.getNetwork().getGossipHandler().register(AtomVote.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(Class<? extends Primitive> type, Collection<Hash> items) throws Throwable
			{
				AtomPool.this.lock.readLock().lock();
				try
				{
					Set<Hash> required = new HashSet<Hash>();
					for (Hash item : items)
					{
						if (AtomPool.this.votesToCountQueue.contains(item) == true || 
							AtomPool.this.context.getLedger().getLedgerStore().has(item) == true)
							continue;
						
						required.add(item);
					}
					return required;
				}
				finally
				{
					AtomPool.this.lock.readLock().unlock();
				}
			}
		});

		this.context.getNetwork().getGossipHandler().register(AtomVote.class, new GossipReceiver() 
		{
			@Override
			public void receive(Primitive object) throws IOException, CryptoException
			{
				AtomVote vote = (AtomVote) object;
				
				if (atomsLog.hasLevel(Logging.DEBUG) == true)
					atomsLog.debug(AtomPool.this.context.getName()+": Atom vote received "+vote.getHash()+":"+vote.getAtom()+" for "+vote.getOwner());
				
				// Check existence of AtomVote ... primary cause of this evaluating to true is that 
				// the received AtomVote is the local nodes.
				// Syncing from a clean slate may result in the local node voting for an Atom in 
				// the pool, not knowing it already voted previously until it receives the vote from
				// a sync peer.  The duplicate will get caught in the votesToCountQueue processor
				// outputting a lot of warnings which is undesirable.
				if (AtomPool.this.votesToCountQueue.contains(vote.getHash()) == true || 
					AtomPool.this.context.getLedger().getLedgerStore().has(vote.getHash()) == true)
					return;

				PendingAtom pendingAtom = AtomPool.this.context.getLedger().getAtomHandler().get(vote.getAtom());
				if (pendingAtom != null)
				{
					AtomPool.this.votesToCountQueue.put(vote.getHash(), vote);
					synchronized(AtomPool.this.voteProcessor)
					{
						AtomPool.this.voteProcessor.notify();
					}
				}
			}
		});

		this.context.getNetwork().getGossipHandler().register(AtomVote.class, new GossipFetcher() 
		{
			@Override
			public Collection<AtomVote> fetch(Collection<Hash> items) throws IOException
			{
				AtomPool.this.lock.readLock().lock();
				try
				{
					Set<AtomVote> fetched = new HashSet<AtomVote>();
					for (Hash item : items)
					{
						AtomVote atomVote = AtomPool.this.votesToCountQueue.get(item);
						if (atomVote == null)
							atomVote = AtomPool.this.context.getLedger().getLedgerStore().get(item, AtomVote.class);
						
						if (atomVote == null)
						{
							atomsLog.error(AtomPool.this.context.getName()+": Requested atom vote "+item+" not found");
							continue;
						}
						
						fetched.add(atomVote);
					}
					return fetched;
				}
				finally
				{
					AtomPool.this.lock.readLock().unlock();
				}
			}
		});
		
		// SYNC //

		Thread voteProcessorThread = new Thread(this.voteProcessor);
		voteProcessorThread.setDaemon(true);
		voteProcessorThread.setName(this.context.getName()+" Atom Vote Processor");
		voteProcessorThread.start();
		
		this.context.getEvents().register(this.syncChangeListener);
		this.context.getEvents().register(this.atomEventListener);
	}

	@Override
	public void stop() throws TerminationException
	{
		this.voteProcessor.terminate(true);
		this.context.getEvents().unregister(this.syncChangeListener);
		this.context.getEvents().unregister(this.atomEventListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	private void reset()
	{
		this.lock.writeLock().lock();
		try
		{
			this.buckets.forEach((b, s) -> s.clear());
			this.pending.clear();
			this.votesToCastQueue.clear();
			this.votesToCountQueue.clear();
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	AtomVoteStatus process(final AtomVote atomVote) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(atomVote, "Atom vote is null");
		
		if (AtomPool.this.context.getLedger().getLedgerStore().store(AtomPool.this.context.getLedger().getHead().getHeight(), atomVote).equals(OperationStatus.SUCCESS) == false)
		{
			atomsLog.warn(AtomPool.this.context.getName()+": Received already seen atom vote "+atomVote+":"+atomVote.getAtom()+" for "+atomVote.getOwner());
			return AtomVoteStatus.SKIPPED;
		}

		AtomPool.this.lock.writeLock().lock();
		AtomVoteStatus response = AtomVoteStatus.FAILED;
		PendingAtom pendingAtom = null;
		try
		{
			pendingAtom = AtomPool.this.context.getLedger().getAtomHandler().get(atomVote.getAtom());
			if (pendingAtom == null)
			{
				// Pending atom null is likely the atom has already been committed / timedout
				// Check for a commit status, and if true silently accept else return false
				Commit commit = this.context.getLedger().getLedgerStore().search(new StateAddress(Atom.class, atomVote.getAtom()));
				if (commit == null)
				{
					if (atomsLog.hasLevel(Logging.WARN) == true) 
						atomsLog.warn(AtomPool.this.context.getName()+": Pending atom "+atomVote.getAtom()+" not found for vote "+atomVote+" for "+atomVote.getOwner());

					return AtomVoteStatus.FAILED;
				}
	
				return AtomVoteStatus.SKIPPED;
			}
					
			long votePower = AtomPool.this.context.getLedger().getValidatorHandler().getVotePower(Math.max(0, AtomPool.this.context.getLedger().getHead().getHeight() - ValidatorHandler.VOTE_POWER_MATURITY), atomVote.getOwner());
			if (votePower > 0)
			{
				if (pendingAtom.vote(atomVote, votePower) == true)
					response = AtomVoteStatus.SUCCESS;
			}
			else
				response = AtomVoteStatus.SKIPPED;
		}
		finally
		{
			AtomPool.this.lock.writeLock().unlock();
		}
		
		// See if threshold vote power is met, verify aggregate signatures 
		if (response.equals(AtomVoteStatus.SUCCESS))
		{
			// Don't build certificates from cast votes received until executed locally
			if (pendingAtom.getStatus().greaterThan(CommitStatus.NONE) == false)
				return response;

			if (pendingAtom.isPreverified() == true)
				return response;
			
			if (pendingAtom.preverify() == false)
				return response;
			
			if (pendingAtom.isVerified() == true)
				return response;
			
			if (pendingAtom.verify() == false)
				return response;

			if (atomsLog.hasLevel(Logging.DEBUG) == true)
			{
				Set<Long> shardGroups = ShardMapper.toShardGroups(pendingAtom.getShards(), this.context.getLedger().numShardGroups());
				atomsLog.debug(AtomPool.this.context.getName()+": Atom "+pendingAtom.getHash()+" has agreement with "+pendingAtom.voteWeight()+"/"+AtomPool.this.context.getLedger().getValidatorHandler().getTotalVotePower(Math.max(0, AtomPool.this.context.getLedger().getHead().getHeight() - ValidatorHandler.VOTE_POWER_MATURITY), shardGroups));
			}
			
			AtomPool.this.context.getMetaData().increment("ledger.pool.atoms.agreed");
		}
			
		return response;
	}

	public List<PendingAtom> getAll()
	{
		this.lock.readLock().lock();
		try
		{
			List<PendingAtom> pendingAtoms = this.pending.values().stream().collect(Collectors.toList());
			Collections.sort(pendingAtoms, new Comparator<PendingAtom>() 
			{
				@Override
				public int compare(PendingAtom pa1, PendingAtom pa2) 
				{
					return pa1.getHash().compareTo(pa2.getHash());
				}
			});
			return pendingAtoms;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	PendingAtom get(final Hash atom)
	{
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is ZERO");
		
		this.lock.readLock().lock();
		try
		{
			PendingAtom pendingAtom = this.pending.get(atom);
			if (pendingAtom != null)
				return pendingAtom;
			
			return null;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	boolean add(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom);
		
		if (pendingAtom.getAtom() == null)
			throw new IllegalStateException("Atom "+pendingAtom.getHash()+" must be prepared to be added to AtomPool");

		this.lock.writeLock().lock();
		try
		{
			if (this.pending.containsKey(pendingAtom.getHash()) == true)
				return false;
			
			this.pending.put(pendingAtom.getHash(), pendingAtom);

			long location = Longs.fromByteArray(pendingAtom.getHash().toByteArray());
			long bucket = mapToBucket(location);
			this.buckets.get(bucket).add(pendingAtom);
			AtomPool.this.context.getMetaData().increment("ledger.pool.atoms.added");

			if (atomsLog.hasLevel(Logging.DEBUG) == true) 
				atomsLog.debug(AtomPool.this.context.getName()+": "+pendingAtom.toString()+" added to pending pool, size is now "+this.pending.size());

			this.votesToCastQueue.add(pendingAtom);
			synchronized(AtomPool.this.voteProcessor)
			{
				AtomPool.this.voteProcessor.notify();
			}
			
			return true;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	PendingAtom remove(final Hash atom)
	{
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is ZERO");

		this.lock.writeLock().lock();
		try
		{
			PendingAtom pendingAtom = this.pending.remove(atom); 
			if (pendingAtom == null)
			{
				atomsLog.warn(AtomPool.this.context.getName()+": Atom "+atom+" can not be found for removal");
				return null;
			}

			long location = Longs.fromByteArray(atom.toByteArray());
			long bucket = mapToBucket(location);
			this.buckets.get(bucket).remove(pendingAtom);
			AtomPool.this.context.getMetaData().increment("ledger.pool.atoms.removed");

			if (atomsLog.hasLevel(Logging.DEBUG) == true)
				atomsLog.debug(AtomPool.this.context.getName()+": Atom "+atom+" was removed");
			
			return pendingAtom;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public long size()
	{
		this.lock.readLock().lock();
		try
		{
			return this.pending.size();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	public boolean isEmpty()
	{
		this.lock.readLock().lock();
		try
		{
			return this.pending.isEmpty();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	private long mapToBucket(long location)
	{
		long bucket = (location / AtomPool.BUCKET_SPAN);
		return bucket;
	}
	
	private long bucketToLocation(long bucket)
	{
		long location = (bucket * AtomPool.BUCKET_SPAN);
		return location;
	}

	List<PendingAtom> get(final long location, final long target, final long range, final int limit, final Collection<Hash> exclusions)
	{
		return get(location, target, range, limit, exclusions, true);
	}
	
	List<PendingAtom> get(final long location, final long target, final long range, final int limit, final Collection<Hash> exclusions, boolean synced)
	{
		final List<PendingAtom> atoms = new ArrayList<PendingAtom>();
		final BlockHeader head = AtomPool.this.context.getLedger().getHead(); // TODO make this an argument
		final long localShardGroup = ShardMapper.toShardGroup(AtomPool.this.context.getNode().getIdentity(), AtomPool.this.context.getLedger().numShardGroups());
		final long systemTime = Time.getSystemTime();
		
		final Predicate<PendingAtom> filter = new Predicate<PendingAtom>()
		{
			@Override
			public boolean test(PendingAtom pa)
			{
				if (pa.getStatus().lessThan(CommitStatus.PREPARED) == true)
				{
					atomsLog.error(AtomPool.this.context.getName()+": Atom not in prepared state "+pa.getHash());
					return false;
				}

				if (systemTime < pa.getWitnessedAt() + pa.getInclusionDelay())
					return false;

				if (systemTime > pa.getInclusionTimeout())
					return false;
				
				if (pa.isVerified() == false)
					return false;

				if (exclusions.contains(pa.getHash()) == true)
					return false;

				if (synced == true && pa.voteWeight() < pa.voteThreshold())
				{
					return false;
					
					// TODO left here for reference on per shard group threshold tallying
					// Check local atom agreement first
					// FIXME if atom is latent making it into a block, the vote weight tallied is historic and may not pass the threshold at the current head height
					//		 recalulating vote weight if historic (easily detected) probably solves this
/*					long voteThresold = AtomPool.this.context.getLedger().getVoteRegulator().getVotePowerThreshold(head.getHeight() - VoteRegulator.VOTE_POWER_MATURITY, localShardGroup);
					if (pa.voteWeight() < voteThresold)
					{
						pa.setInclusionDelay(pa.getInclusionDelay() + TimeUnit.SECONDS.toMillis(1));
						return false;
					}

					// Check remote atom agreements
					Set<Long> shardGroups = AtomPool.this.context.getLedger().getShardGroups(pa.getShards());
					shardGroups.remove(localShardGroup);
					for (long shardGroup : shardGroups)
					{
						voteThresold = AtomPool.this.context.getLedger().getVoteRegulator().getVotePowerThreshold(head.getHeight() - VoteRegulator.VOTE_POWER_MATURITY, shardGroup);
						if (pa.voteWeight() < voteThresold)
						{
							pa.setInclusionDelay(pa.getInclusionDelay() + TimeUnit.SECONDS.toMillis(1));
							return false;
						}
					}*/
				}

				return true;
			}
		};

		this.lock.readLock().lock();
		try
		{
			boolean visitedRight = false;
			boolean visitedLeft = false;
			long bucket = mapToBucket(location);

			for (PendingAtom pendingAtom : this.buckets.get(bucket))
			{
				if (filter.test(pendingAtom) == false)
					continue;
				
				atoms.add(pendingAtom);
			}
			
			CustomInteger leftBucket = new CustomInteger(bucket, AtomPool.NUM_BUCKETS / 2);
			leftBucket.decrement();
			CustomInteger rightBucket = new CustomInteger(bucket, AtomPool.NUM_BUCKETS / 2);
			rightBucket.increment();
			
			do
			{
				if (visitedLeft == false && leftBucket.get() != bucket)
				{
					if (this.buckets.get(leftBucket.get()).isEmpty() == false)
					{
						Set<PendingAtom> leftBucketAtoms = this.buckets.get(leftBucket.get());
						
						if (leftBucketAtoms.isEmpty() == false)
						{
							for (PendingAtom pendingAtom : leftBucketAtoms)
							{
								if (filter.test(pendingAtom) == false)
									continue;
								
								atoms.add(pendingAtom);
							}
						}
					}

					leftBucket.decrement();
					if (BlockHandler.withinRange(bucketToLocation(leftBucket.get()), target, range) == false)
						visitedLeft = true;
				}
				
				if (visitedRight == false && rightBucket.get() != bucket)
				{
					if (this.buckets.get(rightBucket.get()).isEmpty() == false)
					{
						Set<PendingAtom> rightBucketAtoms = this.buckets.get(rightBucket.get());
						
						if (rightBucketAtoms.isEmpty() == false)
						{
							for (PendingAtom pendingAtom : rightBucketAtoms)
							{
								if (filter.test(pendingAtom) == false)
									continue;
								
								atoms.add(pendingAtom);
							}
						}
					}
					
					rightBucket.increment();
					if (BlockHandler.withinRange(bucketToLocation(rightBucket.get()), target, range) == false)
						visitedRight = true;
				}
			}
			while ((visitedRight == false || visitedLeft == false) && leftBucket.get() != rightBucket.get() && atoms.size() < limit);
		}
		finally
		{
			this.lock.readLock().unlock();
		}

		return atoms;
	}

	Set<PendingAtom> get(final Collection<Hash> atoms)
	{
		Objects.requireNonNull(atoms, "Atoms hash collection is null");
		
		this.lock.writeLock().lock();
		try
		{
			Set<PendingAtom> known = new HashSet<PendingAtom>();
			atoms.stream().forEach(a -> {
				PendingAtom pendingAtom = AtomPool.this.pending.get(a);
				if (pendingAtom != null)
					known.add(pendingAtom);
			});

			return known;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
						
	private SynchronousEventListener atomEventListener = new SynchronousEventListener() 
	{
		@Subscribe
		public void on(final AtomPersistedEvent event) 
		{
			if (AtomPool.this.add(event.getPendingAtom()) == false)
				atomsLog.error(AtomPool.this.context.getName()+": Atom "+event.getAtom().getHash()+" not added to atom pool");
		}
		
		@Subscribe
		public void on(AtomAcceptedEvent event)
		{
			AtomPool.this.remove(event.getPendingAtom().getHash());
		}

		@Subscribe
		public void on(AtomExceptionEvent event)
		{
			if (event.getPendingAtom().getStatus().equals(CommitStatus.PREPARED) == true)
				AtomPool.this.remove(event.getPendingAtom().getHash());
		}

		@Subscribe
		public void on(AtomAcceptedTimeoutEvent event)
		{
			AtomPool.this.remove(event.getPendingAtom().getHash());
		}

		@Subscribe
		public void on(AtomCommitTimeoutEvent event)
		{
			AtomPool.this.remove(event.getPendingAtom().getHash());
		}
	};

	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncStatusChangeEvent event) 
		{
			AtomPool.this.lock.writeLock().lock();
			try
			{
				if (event.isSynced() == false)
				{
					atomsLog.info(AtomPool.this.context.getName()+": Sync status changed to "+event.isSynced()+", flushing atom pool");
					reset();
				}
			}
			finally
			{
				AtomPool.this.lock.writeLock().unlock();
			}
		}
	};
	
	public Map<Integer, Integer> distribution()
	{
		this.lock.readLock().lock();
		try
		{
			Map<Integer, Integer> distribution = new LinkedHashMap<Integer, Integer>();
			for (long bucket : this.buckets.keySet())
				if (this.buckets.containsKey(bucket) == true && this.buckets.get(bucket).size() > 0)
					distribution.put((int) bucket, this.buckets.get(bucket).size());
			
			return distribution;
		}
		finally
		{
			AtomPool.this.lock.readLock().unlock();
		}
	}
}
