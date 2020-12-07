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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.events.AtomDiscardedEvent;
import org.fuserleer.ledger.events.AtomErrorEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.messages.AtomBroadcastMessage;
import org.fuserleer.ledger.messages.AtomPoolVoteInventoryMessage;
import org.fuserleer.ledger.messages.AtomPoolVoteMessage;
import org.fuserleer.ledger.messages.GetAtomPoolMessage;
import org.fuserleer.ledger.messages.GetAtomPoolVoteMessage;
import org.fuserleer.ledger.messages.InventoryMessage;
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
		private final Hash	hash;
		private	final long 	seen;
		private	final long 	witnessed;
		private Atom 		atom;
		private long 		delayed;

		private UInt256		voteWeight;
		private final Map<ECPublicKey, UInt128> votes;

		public PendingAtom(Hash atom)
		{
			this.hash = Objects.requireNonNull(atom);
			this.witnessed = Time.getSystemTime();
			this.seen = AtomPool.this.context.getLedger().getHead().getHeight();
			this.delayed = 0;
			this.voteWeight = UInt256.ZERO;
			this.votes = Collections.synchronizedMap(new HashMap<ECPublicKey, UInt128>());
		}

		public PendingAtom(Atom atom)
		{
			this.hash = Objects.requireNonNull(atom).getHash();
			this.atom = atom;
			this.witnessed = Time.getSystemTime();
			this.seen = AtomPool.this.context.getLedger().getHead().getHeight();
			this.delayed = 0;
			this.voteWeight = UInt256.ZERO;
			this.votes = Collections.synchronizedMap(new HashMap<ECPublicKey, UInt128>());
		}
		
		@Override
		public Hash getHash()
		{
			return this.hash;
		}
		
		public long getSeen()
		{
			return this.seen;
		}

		public long getWitnessed()
		{
			return this.witnessed;
		}

		public Atom getAtom()
		{
			return this.atom;
		}
				
		void setAtom(Atom atom)
		{
			if (Objects.requireNonNull(atom).getHash().equals(this.hash) == false)
				throw new IllegalArgumentException("Atom does not match hash "+this.hash+" "+atom);
			
			this.atom = atom;
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
			return this.hash.hashCode();
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
			return this.hash+" @ "+this.witnessed;
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
					atomsLog.warn(AtomPool.this.context.getName()+": "+identity+" has already cast a vote for "+this.hash);
				
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
			List<Hash> votes = new ArrayList<Hash>();
			try 
			{
				long lastBroadcast = System.currentTimeMillis();
				while (this.isTerminated() == false)
				{
					try
					{
						Entry<Hash, PendingAtom> pendingAtom = AtomPool.this.voteQueue.poll(1, TimeUnit.SECONDS);
						if (pendingAtom != null && pendingAtom.getValue().getAtom() != null)
						{
							if (atomsLog.hasLevel(Logging.DEBUG))
								atomsLog.debug(AtomPool.this.context.getName()+": Voting on atom "+pendingAtom.getValue().getHash());

							try
							{
								// Dont vote if we have no power!
								UInt128 localVotePower = AtomPool.this.voteRegulator.getVotePower(AtomPool.this.context.getNode().getIdentity(), pendingAtom.getValue().getSeen());
								if (localVotePower.compareTo(UInt128.ZERO) > 0)
								{
									pendingAtom.getValue().vote(AtomPool.this.context.getNode().getIdentity(), localVotePower);
									votes.add(pendingAtom.getValue().getHash());
								}
							}
							catch (Exception ex)
							{
								atomsLog.error(AtomPool.this.context.getName()+": Error processing vote for " + pendingAtom.getValue().getHash(), ex);
							}
						}
						
						try
						{
							if (votes.size() == AtomPoolVoteMessage.MAX_VOTES ||
								(System.currentTimeMillis() - lastBroadcast > TimeUnit.SECONDS.toMillis(1) && votes.size() > 0))
							{
								if (atomsLog.hasLevel(Logging.DEBUG))
									atomsLog.debug(AtomPool.this.context.getName()+": Broadcasting "+votes.size()+" votes");
								
								lastBroadcast = System.currentTimeMillis();
								broadcast(votes);
								votes = new ArrayList<Hash>();
							}
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomPool.this.context.getName()+": Error broadcasting vote for "+votes.size()+" atoms", ex);
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
		
		private void broadcast(List<Hash> votes) throws IOException, CryptoException
		{
			AtomPoolVote atomPoolVote = new AtomPoolVote(votes, AtomPool.this.context.getLedger().getHead().getHeight(), AtomPool.this.context.getNode().getIdentity());
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
					atomsLog.error(AtomPool.this.context.getName()+": Unable to send AtomPoolVoteMessage for "+votes.size()+" atoms to "+connectedPeer, ex);
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
	private final BlockingQueue<Hash> atomVoteInventory = new LinkedBlockingQueue<Hash>();
	private final Executable atomVoteInventoryProcessor = new Executable() 
	{
		@Override
		public void execute()
		{
			while(isTerminated() == false)
			{
				try
				{
					Hash hash = AtomPool.this.atomVoteInventory.poll(1, TimeUnit.SECONDS);
					if (hash == null)
						continue;
					
					List<Hash> atomVoteInventory = new ArrayList<Hash>();
					atomVoteInventory.add(hash);
					AtomPool.this.atomVoteInventory.drainTo(atomVoteInventory, InventoryMessage.MAX_INVENTORY-1);
	
					AtomPoolVoteInventoryMessage atomPoolVoteInventoryMessage = new AtomPoolVoteInventoryMessage(atomVoteInventory);
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
					atomsLog.error(AtomPool.this.context.getName()+": Processing of atom vote inventory failed", ex);
				}
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

		atomsLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN | Logging.WARN);
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
								AtomPoolVote atomPoolVote = AtomPool.this.context.getLedger().getLedgerStore().get(atomPoolVoteHash, AtomPoolVote.class);
								if (atomPoolVote == null)
								{
									if (atomsLog.hasLevel(Logging.DEBUG) == true)
										atomsLog.debug(AtomPool.this.context.getName()+": Requested atom vote not found "+atomPoolVoteHash+" for " + peer);
									
									continue;
								}

								try
								{
									AtomPool.this.context.getNetwork().getMessaging().send(new AtomPoolVoteMessage(atomPoolVote), peer);
								}
								catch (IOException ex)
								{
									atomsLog.error(AtomPool.this.context.getName()+": Unable to send AtomPoolVoteMessage for "+atomPoolVoteHash+" of "+atomPoolVote.getObject().size()+" atoms to "+peer, ex);
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
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (atomsLog.hasLevel(Logging.DEBUG) == true)
								atomsLog.debug(AtomPool.this.context.getName()+": Atom pool votes of "+atomPoolVoteMessage.getVotes().getObject().size()+" for "+atomPoolVoteMessage.getVotes().getOwner()+" from " + peer);
							
							if (atomPoolVoteMessage.getVotes().getObject().size() == 0)
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
								// Creating pending atom objects from vote if atom not seen
								AtomPool.this.lock.writeLock().lock();
								try
								{
									for (Hash atom : atomPoolVoteMessage.getVotes().getObject())
									{
										PendingAtom pendingAtom = AtomPool.this.pending.get(atom);
										if (pendingAtom == null) // TODO check if atom is in state &&
										{
											pendingAtom = new PendingAtom(atom);
											add(pendingAtom);
										}
									}
								}
								finally
								{
									AtomPool.this.lock.writeLock().unlock();
								}
								
								// TODO optimise vote count, will get slow with big mem pools
								AtomPool.this.lock.readLock().lock();
								try
								{
									for (Hash atom : atomPoolVoteMessage.getVotes().getObject())
									{
										PendingAtom pendingAtom = AtomPool.this.pending.get(atom);
										if (pendingAtom == null)
											continue;
											
										if (pendingAtom.voted(atomPoolVoteMessage.getVotes().getOwner()) == true)
											continue;
										
										pendingAtom.vote(atomPoolVoteMessage.getVotes().getOwner(), AtomPool.this.voteRegulator.getVotePower(atomPoolVoteMessage.getVotes().getOwner(), pendingAtom.getSeen()));
										
										UInt256 voteThresold = AtomPool.this.voteRegulator.getVotePowerThreshold(pendingAtom.getSeen());
										if (pendingAtom.votes().compareTo(voteThresold) >= 0)
											atomsLog.info(AtomPool.this.context.getName()+": Atom "+pendingAtom.getHash()+" has agreement with "+pendingAtom.votes()+"/"+AtomPool.this.voteRegulator.getTotalVotePower(pendingAtom.getSeen()));
									}
								}
								finally
								{
									AtomPool.this.lock.readLock().unlock();
								}
								
								// Independent so doesn't need to sit in a lock
								AtomPool.this.atomVoteInventory.add(atomPoolVoteMessage.getVotes().getHash());
							}
							else
								atomsLog.warn(AtomPool.this.context.getName()+": Received already seen atom pool votes of "+atomPoolVoteMessage.getVotes().getObject().size()+" for "+atomPoolVoteMessage.getVotes().getOwner()+" from " + peer);
						}
						catch (Exception ex)
						{
							atomsLog.error(AtomPool.this.context.getName()+": ledger.messages.atom.pool.vote " + peer, ex);
						}
					}
				});
			}
		});
		
		Thread atomVoteInventoryProcessorThread = new Thread(this.atomVoteInventoryProcessor);
		atomVoteInventoryProcessorThread.setDaemon(true);
		atomVoteInventoryProcessorThread.setName(this.context.getName()+" Atom Vote Inventory Processor");
		atomVoteInventoryProcessorThread.start();

		Thread voteProcessorThread = new Thread(this.voteProcessor);
		voteProcessorThread.setDaemon(true);
		voteProcessorThread.setName(this.context.getName()+" Vote Processor");
		voteProcessorThread.start();
		
		this.context.getEvents().register(this.eventListener);
	}

	@Override
	public void stop() throws TerminationException
	{
		this.atomVoteInventoryProcessor.terminate(true);
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
			if (pendingAtom != null && pendingAtom.getAtom() != null)
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
				pendingAtom = new PendingAtom(atom);
				add(pendingAtom);
				this.voteQueue.put(pendingAtom.getHash(), pendingAtom);
				return true;
			}
			else if (pendingAtom.getAtom() == null)
			{
				pendingAtom.setAtom(atom);
				this.voteQueue.put(pendingAtom.getHash(), pendingAtom);
				return true;
			}
			
			return false;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	private void add(PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom);

		this.lock.writeLock().lock();
		try
		{
			// TODO want to allow multiple indexable definitions in pool?
			this.pending.put(pendingAtom.getHash(), pendingAtom);

			long location = Longs.fromByteArray(pendingAtom.getHash().toByteArray());
			long bucket = mapToBucket(location);
			this.buckets.get(bucket).add(pendingAtom);
			AtomPool.this.context.getMetaData().increment("ledger.pool.atoms.added");

			if (atomsLog.hasLevel(Logging.DEBUG) == true) 
				atomsLog.debug(AtomPool.this.context.getName()+": "+pendingAtom.toString()+" added to pending pool, size is now "+this.pending.size());
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public boolean remove(Hash atom)
	{
		Objects.requireNonNull(atom);

		this.lock.writeLock().lock();
		try
		{
			PendingAtom pendingAtom = this.pending.remove(atom); 
			if (pendingAtom == null)
				return false;
			
			if (pendingAtom.getAtom() != null)
			{
				for (Indexable indexable : pendingAtom.getAtom().getIndexables())
				{
					if (this.indexables.remove(indexable.getHash(), atom) == false)
						atomsLog.debug(AtomPool.this.context.getName()+": Indexable "+indexable+" defined by "+atom+" not found");
				}
			}

			long location = Longs.fromByteArray(atom.toByteArray());
			long bucket = mapToBucket(location);
			this.buckets.get(bucket).remove(pendingAtom);
			AtomPool.this.context.getMetaData().increment("ledger.pool.atoms.removed");
			
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
	
	public List<Atom> get(final long location, final Collection<Bloom> exclusions)
	{
		return get(location, 1, exclusions);
	}
	
	public List<Atom> get(final long location, final int limit, final Collection<Bloom> exclusions)
	{
		final List<Atom> atoms = new ArrayList<Atom>();
		final List<AtomDiscardedEvent> removals = new ArrayList<AtomDiscardedEvent>();
		final long systemTime = Time.getSystemTime();

		final Predicate<PendingAtom> filter = new Predicate<PendingAtom>()
		{
			@Override
			public boolean test(PendingAtom pa)
			{
				if (pa.atom == null)
					return false;
				
				if (systemTime > pa.getWitnessed() + AtomPool.this.commitTimeout)
				{
					removals.add(new AtomDiscardedEvent(pa.atom, "Timed out"));
					return false;
				}
				
				UInt256 voteThresold = AtomPool.this.voteRegulator.getVotePowerThreshold(pa.getSeen());
				if (pa.votes().compareTo(voteThresold) < 0)
					return false;

				if (systemTime > pa.delayed && systemTime < pa.getWitnessed() + AtomPool.this.commitTimeout)
				{
					for (Bloom exclusion : exclusions)
						if (exclusion.contains(pa.hash.toByteArray()) == true)
							return false;
					
					return true;
				}
				
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
				if (remove(a.getAtom().getHash()) == true)
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
					
					pendingAtom.delayed = Time.getSystemTime() + TimeUnit.SECONDS.toMillis(10);
					
					// Check if the dependency is also pending.  If the atom is recently witnessed, it may be dependent 
					// on an atom that is also recent and hasn't been seen by the local node yet.  Allow some "maturity" 
					// time before pruning, enabling any recent dependent atoms to be seen.
					if (AtomPool.this.indexables.containsKey(((DependencyNotFoundException)event.getError()).getDependency()) == false &&
						Time.getSystemTime() - pendingAtom.getWitnessed() > AtomPool.this.dependencyTimeout)
					{
						if (AtomPool.this.remove(event.getAtom().getHash()) == true)
							AtomPool.this.context.getEvents().post(new AtomDiscardedEvent(event.getAtom(), event.getError().getMessage()));
						
						return;
					}
				}
				else
					AtomPool.this.remove(event.getAtom().getHash());
			}
			finally
			{
				AtomPool.this.lock.writeLock().unlock();
			}
		}
		
		@Subscribe
		public void on(AtomExceptionEvent event)
		{
			AtomPool.this.remove(event.getAtom().getHash());
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
				if (bloom.contains(pendingAtom.getHash().toByteArray()) == true)
					toRemove.add(pendingAtom);
			}
			
			for (PendingAtom pendingAtom : toRemove)
				remove(pendingAtom.getHash());
		}
		finally
		{
			AtomPool.this.lock.writeLock().unlock();
		}
	}
}
