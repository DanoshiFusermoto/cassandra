package org.fuserleer.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.collections.Bloom;
import org.fuserleer.collections.MappedBlockingQueue;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hashable;
import org.fuserleer.database.Indexable;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.DependencyNotFoundException;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.executors.Executable;
import org.fuserleer.executors.Executor;
import org.fuserleer.executors.ScheduledExecutable;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.events.AtomDiscardedEvent;
import org.fuserleer.ledger.events.AtomErrorEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.messages.AtomBroadcastMessage;
import org.fuserleer.ledger.messages.AtomPoolVoteInventoryMessage;
import org.fuserleer.ledger.messages.AtomPoolVoteMessage;
import org.fuserleer.ledger.messages.GetAtomPoolMessage;
import org.fuserleer.ledger.messages.GetAtomPoolVoteMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.messaging.MessageProcessor;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.time.Time;
import org.fuserleer.utils.CustomInteger;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.UInt128;
import org.fuserleer.utils.UInt256;

import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public class AtomPool implements Service
{
	private static final Logger atomsLog = Logging.getLogger("atoms");

	private final static int 	NUM_BUCKETS = 4096;
	private final static long 	BUCKET_SPAN = -(Long.MIN_VALUE / 2) / (NUM_BUCKETS / 4);
	
	private class PendingAtom implements Hashable
	{
		private final Atom 	atom;
		private	final long 	witnessed;
		private long 		delayed;

		private UInt256		voteWeight;
		private final Map<ECPublicKey, UInt128> votes;

		public PendingAtom(Atom atom)
		{
			this.atom = Objects.requireNonNull(atom);
			this.witnessed = Time.getLedgerTimeMS();
			this.delayed = 0;
			this.voteWeight = UInt256.ZERO;
			this.votes = Collections.synchronizedMap(new HashMap<ECPublicKey, UInt128>());
		}
		
		@Override
		public Hash getHash()
		{
			return this.atom.getHash();
		}
		
		public Atom getAtom()
		{
			return this.atom;
		}
				
		public long getDelayed()
		{
			return this.delayed;
		}
				
		void setDelayed(long delayed)
		{
			this.delayed = delayed;
		}

		@Override
		public int hashCode()
		{
			return this.atom.getHash().hashCode();
		}

		@Override
		public boolean equals(Object obj)
		{
			if (obj == null)
				return false;
			
			if (obj != this)
				return false;
			
			return true;
		}

		@Override
		public String toString()
		{
			return this.atom.getHash()+" @ "+this.witnessed;
		}
		
		public long getWitnessed()
		{
			return this.witnessed;
		}
		
		public boolean voted(ECPublicKey identity)
		{
			return this.votes.containsKey(Objects.requireNonNull(identity, "Vote identity is null"));
		}

		public UInt256 vote(ECPublicKey identity, UInt128 weight)
		{
			synchronized(this.votes)
			{
				if (this.votes.containsKey(Objects.requireNonNull(identity, "Vote identity is null")) == false)
				{
					this.votes.put(identity, Objects.requireNonNull(weight, "Vote weight is null"));
					this.voteWeight = this.voteWeight.add(weight);
				}
				else
					atomsLog.warn(AtomPool.this.context.getName()+": "+identity+" has already cast a vote for "+this.atom.getHash());
				
				return this.voteWeight;
			}
		}
		
		public UInt256 votes()
		{
			return this.voteWeight;
		}
	}
	
	private Executable voteProcessor = new Executable()
	{
		@Override
		public void execute()
		{
			Bloom voteBloom = new Bloom(0.000000001, AtomPoolVoteMessage.MAX_VOTES);
			try 
			{
				long lastBroadcast = System.currentTimeMillis();
				while (this.isTerminated() == false)
				{
					try
					{
						Entry<Hash, PendingAtom> pendingAtom = AtomPool.this.voteQueue.poll(1, TimeUnit.SECONDS);
						if (pendingAtom != null)
						{
							if (atomsLog.hasLevel(Logging.DEBUG))
								atomsLog.debug(AtomPool.this.context.getName()+": Voting on atom "+pendingAtom.getValue().getHash());

							try
							{
								// Dont vote if we have no power!
								UInt128 localVotePower = AtomPool.this.voteRegulator.getVotePower(AtomPool.this.context.getNode().getIdentity(), Long.MAX_VALUE);
								if (localVotePower.compareTo(UInt128.ZERO) > 0)
								{
									pendingAtom.getValue().vote(AtomPool.this.context.getNode().getIdentity(), AtomPool.this.voteRegulator.getVotePower(AtomPool.this.context.getNode().getIdentity(), Long.MAX_VALUE));
									voteBloom.add(pendingAtom.getValue().getHash().toByteArray());
								}
							}
							catch (Exception ex)
							{
								atomsLog.error(AtomPool.this.context.getName()+": Error processing vote for " + pendingAtom.getValue().getHash(), ex);
							}
						}
						
						try
						{
							if (voteBloom.count() == AtomPoolVoteMessage.MAX_VOTES ||
								(System.currentTimeMillis() - lastBroadcast > TimeUnit.SECONDS.toMillis(1) && voteBloom.count() > 0))
							{
								if (atomsLog.hasLevel(Logging.DEBUG))
									atomsLog.debug(AtomPool.this.context.getName()+": Broadcasting "+voteBloom.count()+" votes");
								
								lastBroadcast = System.currentTimeMillis();
								broadcast(voteBloom);
								voteBloom = new Bloom(0.000000001, AtomPoolVoteMessage.MAX_VOTES);
							}
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomPool.this.context.getName()+": Error broadcasting vote for "+voteBloom.count()+" atoms", ex);
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
		
		private void broadcast(Bloom voteBloom) throws IOException, CryptoException
		{
			AtomPoolVote atomPoolVote = new AtomPoolVote(voteBloom, AtomPool.this.context.getLedger().getHead().getHeight(), AtomPool.this.context.getNode().getIdentity());
			atomPoolVote.sign(AtomPool.this.context.getNode().getKey());
			AtomPool.this.context.getLedger().getLedgerStore().store(atomPoolVote);
			
			AtomPoolVoteMessage atomPoolVoteMessage = new AtomPoolVoteMessage(atomPoolVote);
			for (ConnectedPeer connectedPeer : AtomPool.this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED))
			{
				if (AtomPool.this.context.getNode().isInSyncWith(connectedPeer.getNode()) == false)
					continue;
				
				try
				{
					AtomPool.this.context.getNetwork().getMessaging().send(atomPoolVoteMessage, connectedPeer);
				}
				catch (IOException ex)
				{
					atomsLog.error(AtomPool.this.context.getName()+": Unable to send AtomPoolVoteMessage for "+voteBloom.count()+" atoms to "+connectedPeer, ex);
				}
			}
		}
	};
	
	private final Context context;
	private final long commitTimeout;
	private final long dependencyTimeout;

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Map<Hash, PendingAtom> pending = new HashMap<Hash, PendingAtom>();
	private final Map<Hash, Hash> indexables = new HashMap<Hash, Hash>();
	private final Map<Long, Set<PendingAtom>> buckets = new HashMap<Long, Set<PendingAtom>>();
	private final VoteRegulator voteRegulator;
	private final MappedBlockingQueue<Hash, PendingAtom> voteQueue;

	// Atom vote broadcast batching
	private final Set<Hash> atomPoolVoteBatch = new HashSet<Hash>();
	private final ScheduledExecutable atomPoolVoteBatchProcessor = new ScheduledExecutable(1000, 250, TimeUnit.MILLISECONDS) 
	{
		@Override
		public void execute()
		{
			try
			{
				AtomPoolVoteInventoryMessage atomPoolVoteInventoryMessage;
				synchronized(AtomPool.this.atomPoolVoteBatch)
				{
					if (AtomPool.this.atomPoolVoteBatch.isEmpty() == true)
						return;
					
					atomPoolVoteInventoryMessage = new AtomPoolVoteInventoryMessage(AtomPool.this.atomPoolVoteBatch);
					AtomPool.this.atomPoolVoteBatch.clear();
				}
	
				for (ConnectedPeer connectedPeer : AtomPool.this.context.getNetwork().get(Protocol.TCP, PeerState.CONNECTED))
				{
					if (AtomPool.this.context.getNode().isInSyncWith(connectedPeer.getNode()) == false)
						continue;
						
					try
					{
						AtomPool.this.context.getNetwork().getMessaging().send(atomPoolVoteInventoryMessage, connectedPeer);
					}
					catch (IOException ex)
					{
						atomsLog.error(AtomPool.this.context.getName()+": Unable to send AtomPoolVoteInventoryMessage of "+atomPoolVoteInventoryMessage.getInventory().size()+" atom votes to "+connectedPeer, ex);
					}
				}
			}
			catch (Exception ex)
			{
				atomsLog.error(AtomPool.this.context.getName()+": Processing of atom pool vote batch failed", ex);
			}
		}
	};

	public AtomPool(Context context, VoteRegulator voteRegulator)
	{
		this(context, voteRegulator, TimeUnit.SECONDS.toMillis(context.getConfiguration().get("ledger.pool.atom.timeout", 3600*24)), TimeUnit.SECONDS.toMillis(context.getConfiguration().get("ledger.pool.dependency.timeout", 60)));
	}
	
	public AtomPool(Context context, VoteRegulator voteRegulator, long commitTimeout, long dependencyTimeout)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.voteRegulator = Objects.requireNonNull(voteRegulator, "Vote regulator is null");
		this.commitTimeout = commitTimeout;
		this.dependencyTimeout = dependencyTimeout;
		this.voteQueue = new MappedBlockingQueue<Hash, PendingAtom>(this.context.getConfiguration().get("ledger.atom.queue", 1<<16));

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
		this.context.getNetwork().getMessaging().register(GetAtomPoolMessage.class, this.getClass(), new MessageProcessor<GetAtomPoolMessage>()
		{
			@Override
			public void process(final GetAtomPoolMessage getAtomPoolMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomPool.this.context.getName()+": Atom pool request from "+peer);

							// TODO will cause problems when pool is BIG
							// TODO what about the actual votes?
							Collection<Atom> atoms = AtomPool.this.get();
							List<Hash> atomsToBroadcast = new ArrayList<Hash>();
							for (Atom atom : atoms)
							{
								atomsToBroadcast.add(atom.getHash());

								if (atomsToBroadcast.size() == AtomBroadcastMessage.MAX_ATOMS)
								{
									if (atomsLog.hasLevel(Logging.DEBUG) == true)
										atomsLog.debug(AtomPool.this.context.getName()+": Broadcasting about "+atomsToBroadcast.size()+" atoms to "+peer);
									
									AtomBroadcastMessage atomBroadcastMessage = new AtomBroadcastMessage(atomsToBroadcast);
									AtomPool.this.context.getNetwork().getMessaging().send(atomBroadcastMessage, peer);
									atomsToBroadcast.clear();
								}
							}
							
							if (atomsToBroadcast.isEmpty() == false)
							{
								if (atomsLog.hasLevel(Logging.DEBUG) == true)
									atomsLog.debug(AtomPool.this.context.getName()+": Broadcasting about "+atomsToBroadcast.size()+" atoms to "+peer);
								
								AtomBroadcastMessage atomBroadcastMessage = new AtomBroadcastMessage(atomsToBroadcast);
								AtomPool.this.context.getNetwork().getMessaging().send(atomBroadcastMessage, peer);
								atomsToBroadcast.clear();
							}
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomPool.this.context.getName()+": ledger.messages.atom.get.pool " + peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(AtomPoolVoteInventoryMessage.class, this.getClass(), new MessageProcessor<AtomPoolVoteInventoryMessage>()
		{
			@Override
			public void process(final AtomPoolVoteInventoryMessage atomPoolVoteInventoryMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomPool.this.context.getName()+": Atom votes inventory for "+atomPoolVoteInventoryMessage.getInventory().size()+" vote blooms from " + peer);

							if (atomPoolVoteInventoryMessage.getInventory().size() == 0)
							{
								atomsLog.error(AtomPool.this.context.getName()+": Received empty atom votes inventory from " + peer);
								// TODO disconnect and ban
								return;
							}

							// TODO proper request process that doesn't produce duplicate requests  see AtomHandler
							List<Hash> atomPoolVoteInventoryRequired = new ArrayList<Hash>();
							for (Hash atomPoolVoteHash : atomPoolVoteInventoryMessage.getInventory())
							{
								if (AtomPool.this.context.getLedger().getLedgerStore().has(atomPoolVoteHash) == true)
									continue;
								
								atomPoolVoteInventoryRequired.add(atomPoolVoteHash);
							}
							
							if (atomPoolVoteInventoryRequired.isEmpty() == true)
								return;
							
							AtomPool.this.context.getNetwork().getMessaging().send(new GetAtomPoolVoteMessage(atomPoolVoteInventoryRequired), peer);
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomPool.this.context.getName()+": Unable to send ledger.messages.atom.pool.vote.inv for "+atomPoolVoteInventoryMessage.getInventory().size()+" atoms to "+peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(GetAtomPoolVoteMessage.class, this.getClass(), new MessageProcessor<GetAtomPoolVoteMessage>()
		{
			@Override
			public void process(final GetAtomPoolVoteMessage getAtomPoolVoteMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomPool.this.context.getName()+": Atom votes request for "+getAtomPoolVoteMessage.getInventory().size()+" vote blooms from " + peer);
							
							if (getAtomPoolVoteMessage.getInventory().size() == 0)
							{
								atomsLog.error(AtomPool.this.context.getName()+": Received empty atom votes request from " + peer);
								// TODO disconnect and ban
								return;
							}
							
							for (Hash atomPoolVoteHash : getAtomPoolVoteMessage.getInventory())
							{
								AtomPoolVote atomPoolVoteBloom = AtomPool.this.context.getLedger().getLedgerStore().get(atomPoolVoteHash, AtomPoolVote.class);
								if (atomPoolVoteBloom == null)
								{
									if (atomsLog.hasLevel(Logging.DEBUG) == true)
										atomsLog.debug(AtomPool.this.context.getName()+": Requested atom vote bloom not found "+atomPoolVoteHash+" for " + peer);
									
									continue;
								}

								try
								{
									AtomPool.this.context.getNetwork().getMessaging().send(new AtomPoolVoteMessage(atomPoolVoteBloom), peer);
								}
								catch (IOException ex)
								{
									atomsLog.error(AtomPool.this.context.getName()+": Unable to send AtomPoolVoteMessage for "+atomPoolVoteHash+" of "+atomPoolVoteBloom.getObject().count()+" atoms to "+peer, ex);
								}
							}
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomPool.this.context.getName()+": ledger.messages.atom.pool.vote.get " + peer, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(AtomPoolVoteMessage.class, this.getClass(), new MessageProcessor<AtomPoolVoteMessage>()
		{
			@Override
			public void process(final AtomPoolVoteMessage atomPoolVoteMessage, final ConnectedPeer peer)
			{
				Executor.getInstance().schedule(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomPool.this.context.getName()+": Atom pool votes of "+atomPoolVoteMessage.getVotes().getObject().count()+" for "+atomPoolVoteMessage.getVotes().getOwner()+" from " + peer);
							
							if (atomPoolVoteMessage.getVotes().getObject().count() == 0)
							{
								atomsLog.error(AtomPool.this.context.getName()+": Received empty atom pool votes for "+atomPoolVoteMessage.getVotes().getOwner()+" from " + peer);
								// TODO disconnect and ban
								return;
							}

							if (atomPoolVoteMessage.getVotes().verify(atomPoolVoteMessage.getVotes().getOwner()) == false)
							{
								atomsLog.error(AtomPool.this.context.getName()+": Atom pool votes failed verification for "+atomPoolVoteMessage.getVotes().getOwner()+" from " + peer);
								return;
							}
							
							if (OperationStatus.KEYEXIST.equals(AtomPool.this.context.getLedger().getLedgerStore().store(atomPoolVoteMessage.getVotes())) == false)
							{
								// TODO optimise vote count, will get slow with big mem pools
								AtomPool.this.lock.readLock().lock();
								try
								{
									for (PendingAtom pendingAtom : AtomPool.this.pending.values())
									{
										if (pendingAtom.voted(atomPoolVoteMessage.getVotes().getOwner()) == true)
											continue;
										
										if (atomPoolVoteMessage.getVotes().getObject().contains(pendingAtom.getHash().toByteArray()) == false)
											continue;
										
										pendingAtom.vote(atomPoolVoteMessage.getVotes().getOwner(), AtomPool.this.voteRegulator.getVotePower(atomPoolVoteMessage.getVotes().getOwner(), Long.MAX_VALUE));
										
										if (atomsLog.hasLevel(Logging.DEBUG) == true)
										{
											UInt256 voteThresold = AtomPool.this.voteRegulator.getVotePowerThreshold(Long.MAX_VALUE);
											if (pendingAtom.votes().compareTo(voteThresold) >= 0)
												atomsLog.debug(AtomPool.this.context.getName()+": Atom "+pendingAtom.getHash()+" has agreement with "+pendingAtom.votes()+"/"+AtomPool.this.voteRegulator.getTotalVotePower(Long.MAX_VALUE));
										}
									}
								}
								finally
								{
									AtomPool.this.lock.readLock().unlock();
								}
								
								// Independent so doesn't need to sit in a lock
								AtomPool.this.atomPoolVoteBatch.add(atomPoolVoteMessage.getVotes().getHash());
							}
							else
								atomsLog.warn(AtomPool.this.context.getName()+": Received already seen atom pool votes of "+atomPoolVoteMessage.getVotes().getObject().count()+" for "+atomPoolVoteMessage.getVotes().getOwner()+" from " + peer);
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomPool.this.context.getName()+": ledger.messages.atom.pool.vote " + peer, ex);
						}
					}
				}, 1, TimeUnit.SECONDS); // FIXME Adding delay to process to mitigate issues missing votes when the Atoms this vote set may refer to are not in the pool yet due to latency.
			}
		});
		
		Executor.getInstance().scheduleWithFixedDelay(this.atomPoolVoteBatchProcessor);
		
		Thread voteProcessorThread = new Thread(this.voteProcessor);
		voteProcessorThread.setDaemon(true);
		voteProcessorThread.setName(this.context.getName()+" Vote Processor");
		voteProcessorThread.start();
		
		this.context.getEvents().register(this.eventListener);
	}

	@Override
	public void stop() throws TerminationException
	{
		this.atomPoolVoteBatchProcessor.terminate(true);
		this.voteProcessor.terminate(true);
		this.context.getEvents().unregister(this.eventListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	public void clear()
	{
		this.lock.writeLock().lock();
		try
		{
			this.pending.clear();
			this.indexables.clear();
			for (Set<PendingAtom> pendingAtom : this.buckets.values())
				pendingAtom.clear();
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public List<Atom> get()
	{
		this.lock.readLock().lock();
		try
		{
			List<Atom> atoms = this.pending.values().stream().filter(pa -> pa.getAtom() != null).map(pa -> pa.getAtom()).collect(Collectors.toList());
			return atoms;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public Atom get(Hash hash)
	{
		this.lock.readLock().lock();
		try
		{
			PendingAtom pendingAtom = this.pending.get(hash);
			if (pendingAtom != null)
				return pendingAtom.getAtom();
			
			return null;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public boolean has(Hash atom)
	{
		this.lock.readLock().lock();
		try
		{
			return this.pending.containsKey(atom);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public int addAll(Collection<Atom> atoms)
	{
		Objects.requireNonNull(atoms);

		this.lock.writeLock().lock();
		try
		{
			int added = 0;
			for (Atom atom : atoms)
			{
				if (add(atom) == true)
					added++;
			}
			return added;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public boolean add(Atom atom)
	{
		Objects.requireNonNull(atom);

		this.lock.writeLock().lock();
		try
		{
			PendingAtom pendingAtom = this.pending.get(atom.getHash());
			if (pendingAtom == null)
			{
				// TODO want to allow multiple indexable definitions in pool?
				for (Indexable indexable : atom.getIndexables())
				{
					if (this.indexables.containsKey(indexable.getHash()) == true)
					{
						atomsLog.debug(AtomPool.this.context.getName()+": Indexable "+indexable+" defined by "+atom.getHash()+" already defined in pending pool");
						return false;
					}
				}

				pendingAtom = new PendingAtom(atom);
				this.pending.put(atom.getHash(), pendingAtom);

				long location = Longs.fromByteArray(atom.getHash().toByteArray());
				long bucket = mapToBucket(location);
				this.buckets.get(bucket).add(pendingAtom);
				
				for (Indexable indexable : atom.getIndexables())
					this.indexables.put(indexable.getHash(), atom.getHash());
				
				this.voteQueue.put(pendingAtom.getHash(), pendingAtom);

				if (atomsLog.hasLevel(Logging.DEBUG) == true) 
					atomsLog.debug(AtomPool.this.context.getName()+": "+pendingAtom.toString()+" added to pending pool, size is now "+this.pending.size());
				
				return true;
			}

			return false;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public boolean remove(Atom atom)
	{
		Objects.requireNonNull(atom);

		this.lock.writeLock().lock();
		try
		{
			for (Indexable indexable : atom.getIndexables())
			{
				if (this.indexables.remove(indexable.getHash(), atom.getHash()) == false)
					atomsLog.debug(AtomPool.this.context.getName()+": Indexable "+indexable+" defined by "+atom.getHash()+" not found");
			}

			PendingAtom pendingAtom = this.pending.remove(atom.getHash()); 
			if (pendingAtom == null)
				return false;
			
			long location = Longs.fromByteArray(atom.getHash().toByteArray());
			long bucket = mapToBucket(location);
			if (pendingAtom.getAtom() != null)
				this.buckets.get(bucket).remove(pendingAtom);
			
			return true;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public long witnessedAt(Hash atom)
	{
		this.lock.readLock().lock();
		try
		{
			PendingAtom pendingAtom = this.pending.get(atom);
			if (pendingAtom != null)
				return pendingAtom.getWitnessed();
			
			return 0;
		}
		finally
		{
			this.lock.readLock().unlock();
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
	
	public List<Atom> get(final long location, final Set<Atom> exclusions)
	{
		return get(location, 1, exclusions);
	}
	
	public List<Atom> get(final long location, final int limit, final Set<Atom> exclusions)
	{
		final List<Atom> atoms = new ArrayList<Atom>();
		final List<AtomDiscardedEvent> removals = new ArrayList<AtomDiscardedEvent>();
		final long ledgerTime = Time.getLedgerTimeMS();

		final Predicate<PendingAtom> filter = new Predicate<PendingAtom>()
		{
			@Override
			public boolean test(PendingAtom pa)
			{
				if (ledgerTime > pa.witnessed + AtomPool.this.commitTimeout)
				{
					removals.add(new AtomDiscardedEvent(pa.atom, "Timed out"));
					return false;
				}
				
				UInt256 voteThresold = AtomPool.this.voteRegulator.getVotePowerThreshold(Long.MAX_VALUE);
				if (pa.votes().compareTo(voteThresold) < 0)
					return false;

				if (exclusions.contains(pa.atom) == false && ledgerTime > pa.delayed && ledgerTime < pa.witnessed + AtomPool.this.commitTimeout)
					return true;
				
				return false;
			}
		};

		this.lock.readLock().lock();
		try
		{
			int visitedRight = limit;
			int visitedLeft = limit;
			long bucket = mapToBucket(location);

			for (PendingAtom pendingAtom : this.buckets.get(bucket))
			{
				if (filter.test(pendingAtom) == false)
					continue;
				
				atoms.add(pendingAtom.atom);
			}
			
			CustomInteger leftBucket = new CustomInteger(bucket, AtomPool.NUM_BUCKETS / 2);
			leftBucket.decrement();
			CustomInteger rightBucket = new CustomInteger(bucket, AtomPool.NUM_BUCKETS / 2);
			rightBucket.increment();
			
			do
			{
				if (visitedLeft > 0 && leftBucket.get() != bucket)
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
								
								atoms.add(pendingAtom.atom);
							}
							
							visitedLeft--;
						}
					}
					leftBucket.decrement();
				}
				
				if (visitedRight > 0 && rightBucket.get() != bucket)
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
								
								atoms.add(pendingAtom.atom);
							}
							
							visitedRight--;
						}
					}
					rightBucket.increment();
				}
			}
			while ((visitedRight > 0 || visitedLeft > 0) && leftBucket.get() != rightBucket.get() && atoms.size() < limit);
		}
		finally
		{
			this.lock.readLock().unlock();
		}

		if (removals.isEmpty() == false)
			removals.forEach(a -> {
				if (remove(a.getAtom()) == true)
					this.context.getEvents().post(a);	// TODO ensure no synchronous event processing happens on this!
			});
		
		return atoms;
	}

	public Set<Atom> get(Collection<Hash> atoms)
	{
		Objects.requireNonNull(atoms);
		
		this.lock.writeLock().lock();
		try
		{
			Set<Atom> known = new HashSet<Atom>();
			atoms.stream().forEach(a -> {
				PendingAtom pendingAtom = AtomPool.this.pending.get(a);
				if (pendingAtom != null && pendingAtom.getAtom() != null)
					known.add(pendingAtom.getAtom());
			});

			return known;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	private SynchronousEventListener eventListener = new SynchronousEventListener() 
	{
		@Subscribe
		public void on(AtomErrorEvent event)
		{
			AtomPool.this.lock.writeLock().lock();
			try
			{
				if (event.getError() instanceof DependencyNotFoundException)
				{
					PendingAtom pendingAtom = AtomPool.this.pending.get(event.getAtom().getHash());
					if (pendingAtom == null)
						return;
					
					pendingAtom.delayed = Time.getLedgerTimeMS() + TimeUnit.SECONDS.toMillis(10);
					
					// Check if the dependency is also pending.  If the atom is recently witnessed, it may be dependent 
					// on an atom that is also recent and hasn't been seen by the local node yet.  Allow some "maturity" 
					// time before pruning, enabling any recent dependent atoms to be seen.
					if (AtomPool.this.indexables.containsKey(((DependencyNotFoundException)event.getError()).getDependency()) == false &&
						Time.getLedgerTimeMS() - pendingAtom.witnessed > AtomPool.this.dependencyTimeout)
					{
						if (AtomPool.this.remove(event.getAtom()) == true)
							AtomPool.this.context.getEvents().post(new AtomDiscardedEvent(event.getAtom(), event.getError().getMessage()));
						
						return;
					}
				}
				else
					AtomPool.this.remove(event.getAtom());
			}
			finally
			{
				AtomPool.this.lock.writeLock().unlock();
			}
		}
		
		@Subscribe
		public void on(AtomExceptionEvent event)
		{
			AtomPool.this.remove(event.getAtom());
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

	// FIXME temporary for block commit ... probability of removing incorrect Atoms!
	public void remove(final Bloom bloom)
	{
		AtomPool.this.lock.writeLock().lock();
		try
		{
			List<PendingAtom> toRemove = new ArrayList<PendingAtom>(); 
			Iterator<PendingAtom> pendingAtomIterator = AtomPool.this.pending.values().iterator();
			while(pendingAtomIterator.hasNext() == true)
			{
				PendingAtom pendingAtom = pendingAtomIterator.next();
				if (bloom.contains(pendingAtom.getAtom().getHash().toByteArray()) == true)
					toRemove.add(pendingAtom);
			}
			
			for (PendingAtom pendingAtom : toRemove)
				remove(pendingAtom.getAtom());
		}
		finally
		{
			AtomPool.this.lock.writeLock().unlock();
		}
	}
}
