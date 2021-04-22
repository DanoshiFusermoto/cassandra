package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.Universe;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.events.EventListener;
import org.fuserleer.events.SynchronousEventListener;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.BlockHeader.InventoryType;
import org.fuserleer.ledger.CommitOperation.Type;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.AtomCertificate;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.events.AtomAcceptedEvent;
import org.fuserleer.ledger.events.AtomCommitEvent;
import org.fuserleer.ledger.events.AtomCommitTimeoutEvent;
import org.fuserleer.ledger.events.AtomDiscardedEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomRejectedEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.events.SyncBlockEvent;
import org.fuserleer.ledger.events.SyncStatusChangeEvent;
import org.fuserleer.ledger.messages.SyncAcquiredMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.peers.events.PeerConnectedEvent;
import org.fuserleer.node.Node;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.Subscribe;

public final class Ledger implements Service, LedgerInterface
{
	private static final Logger ledgerLog = Logging.getLogger("ledger");

	private final Context 	context;
	
	private final AtomPool 		atomPool;
	private final AtomHandler 	atomHandler;

	private final StatePool 	statePool;
	private final StateHandler 	stateHandler;
	private final StateAccumulator	stateAccumulator;

	private final BlockHandler 	blockHandler;
	private final SyncHandler 	syncHandler;

	private final LedgerStore 	ledgerStore;
	private final LedgerSearch	ledgerSearch;
	private final ValidatorHandler validatorHandler;
	
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
	private final transient AtomicReference<BlockHeader> head;
	
	public Ledger(Context context)
	{
		this.context = Objects.requireNonNull(context);

		this.ledgerStore = new LedgerStore(this.context);

		this.validatorHandler = new ValidatorHandler(this.context);
		this.blockHandler = new BlockHandler(this.context);
		this.syncHandler = new SyncHandler(this.context);
		this.atomPool = new AtomPool(this.context);
		this.atomHandler = new AtomHandler(this.context);
		this.stateAccumulator = new StateAccumulator(this.context, "ledger", this.ledgerStore);
		this.stateHandler = new StateHandler(this.context);
		this.statePool = new StatePool(this.context);
		
		this.ledgerSearch = new LedgerSearch(this.context);

		this.head = new AtomicReference<BlockHeader>(this.context.getNode().getHead());

		ledgerLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			// Stuff required for integrity check
			this.ledgerStore.start();
			this.validatorHandler.start();
			
			integrity();
			
			this.context.getEvents().register(this.syncChangeListener);
			this.context.getEvents().register(this.syncBlockListener);
			this.context.getEvents().register(this.asyncAtomListener);
			this.context.getEvents().register(this.syncAtomListener);
			this.context.getEvents().register(this.peerListener);

			// IMPORTANT Order dependent!
			this.blockHandler.start();
			this.atomHandler.start();
			this.atomPool.start();
			this.stateAccumulator.reset();
			this.stateHandler.start();
			this.statePool.start();
			this.syncHandler.start();
			
			this.ledgerSearch.start();
		}
		catch (Exception ex)
		{
			throw new StartupException(ex);
		}			
	}

	@Override
	public void stop() throws TerminationException
	{
		this.context.getEvents().unregister(this.peerListener);
		this.context.getEvents().unregister(this.asyncAtomListener);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getEvents().unregister(this.syncChangeListener);

		this.ledgerSearch.stop();
		
		this.syncHandler.stop();
		this.statePool.stop();
		this.stateHandler.stop();
		this.atomHandler.stop();
		this.atomPool.stop();
		this.blockHandler.stop();
		this.validatorHandler.stop();
		this.ledgerStore.stop();
	}
	
	public void clean() throws IOException
	{
		this.ledgerStore.clean();
		this.validatorHandler.clean();
	}
	
	private void integrity() throws IOException, ValidationException, StateLockedException
	{
		this.lock.writeLock().lock();
		try
		{
			Hash headHash = this.ledgerStore.head();
			
			Hash genesis = this.ledgerStore.getSyncBlock(0);
			if (genesis != null)
			{
				if (Universe.getDefault().getGenesis().getHeader().getHash().equals(genesis) == false)
					throw new RuntimeException("You didn't clean your database dumbass!");
			}
	
			// Check if this is just a new ledger store and doesn't need integrity or recovery
			if (headHash.equals(Universe.getDefault().getGenesis().getHeader().getHash()) == true && this.ledgerStore.has(headHash) == false)
			{
				// Store the genesis block primitive
				this.ledgerStore.store(Universe.getDefault().getGenesis());
	
				// Commit the genesis block
				// Some simple manual actions here with regard to provisioning as don't want to complicate the flow with a special case for genesis
				this.ledgerStore.commit(Universe.getDefault().getGenesis());
				for (Atom atom : Universe.getDefault().getGenesis().getAtoms())
				{
					PendingAtom pendingAtom = PendingAtom.create(this.context, atom);
					pendingAtom.prepare();
					pendingAtom.accepted();
					pendingAtom.provision(Universe.getDefault().getGenesis().getHeader());
					pendingAtom.setStatus(CommitStatus.PROVISIONED);
					pendingAtom.execute();
					this.ledgerStore.commit(Collections.singletonList(pendingAtom.getCommitOperation(Type.ACCEPT)));
				}
				
				return;
			}
			else
			{
				// TODO block header is known but is it the strongest head that represents state?
				BlockHeader header = this.ledgerStore.get(headHash, BlockHeader.class);
				
				if (header != null)
					this.head.set(header);
				else
				{
					// TODO recover to the best head with committed state
					ledgerLog.error(Ledger.this.context.getName()+": Local block header "+headHash+" not found in store");
					throw new UnsupportedOperationException("Integrity recovery not implemented");
				}
				
				// TODO clean up vote power if needed after recovery as could be in a compromised state 
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public AtomHandler getAtomHandler()
	{
		return this.atomHandler;
	}

	public ValidatorHandler getValidatorHandler()
	{
		return this.validatorHandler;
	}

	public BlockHandler getBlockHandler()
	{
		return this.blockHandler;
	}

	public AtomPool getAtomPool()
	{
		return this.atomPool;
	}

	public StateHandler getStateHandler()
	{
		return this.stateHandler;
	}

	public StatePool getStatePool()
	{
		return this.statePool;
	}

	@VisibleForTesting
	public StateAccumulator getStateAccumulator()
	{
		this.lock.readLock().lock();
		try
		{
			return this.stateAccumulator;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
			
	}

	LedgerStore getLedgerStore()
	{
		return this.ledgerStore;
	}
	
	@JsonGetter("head")
	public BlockHeader getHead()
	{
		return this.head.get();
	}

	private void update(final Block block) throws StateLockedException, IOException
	{
		Objects.requireNonNull(block, "Block is null");
		
		this.lock.writeLock().lock();
		try
		{
			Set<PendingAtom> acceptedPendingAtoms = new HashSet<PendingAtom>();
			for (Atom atom : block.getAtoms())
			{
				PendingAtom pendingAtom = getAtomHandler().get(atom.getHash(), CommitStatus.NONE);
				if (pendingAtom == null)
					throw new IllegalStateException("Pending atom "+atom.getHash()+" state appears invalid.");
				
				acceptedPendingAtoms.add(pendingAtom);
			}
			
			this.stateAccumulator.lock(acceptedPendingAtoms);
			acceptedPendingAtoms.forEach(pa -> pa.provision(block.getHeader()));
			
			Set<PendingAtom> committedPendingAtoms = new HashSet<PendingAtom>();
			for (AtomCertificate atomCertificate : block.getCertificates())
			{
				PendingAtom pendingAtom = getAtomHandler().get(atomCertificate.getAtom(), CommitStatus.ACCEPTED);
				if (pendingAtom == null)
					throw new IllegalStateException("Pending atom certificate "+atomCertificate.getAtom()+" state appears invalid.");
				
				committedPendingAtoms.add(pendingAtom);
			}

			if (committedPendingAtoms.isEmpty() == false)
			{
				this.ledgerStore.commit(committedPendingAtoms.stream().map(cpa -> cpa.getCommitOperation()).collect(Collectors.toList()));
				this.stateAccumulator.unlock(committedPendingAtoms);
			}
			
			this.head.set(block.getHeader());
			this.context.getNode().setHead(block.getHeader());
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public <T extends Primitive> T get(Hash hash, Class<T> primitive) throws IOException
	{
		return this.ledgerStore.get(hash, primitive);
	}
	
	public Block getBlock(long height) throws IOException
	{
		Hash committedBlockHash = this.ledgerStore.getSyncBlock(height);
		Block block = this.ledgerStore.get(committedBlockHash, Block.class);
		return block;
	}

	public boolean submit(Atom atom) throws InterruptedException
	{
		Objects.requireNonNull(atom);
		
		return this.atomHandler.submit(atom);
	}
	
	@Override
	public <T extends Primitive> Future<AssociationSearchResponse> get(final AssociationSearchQuery query, final Spin spin)
	{
		return this.ledgerSearch.get(query, spin);
	}

	@Override
	public Future<SearchResult> get(StateSearchQuery query)
	{
		return this.ledgerSearch.get(query);
	}
	
	public boolean isSynced()
	{
		return this.context.getNode().isSynced();
	}
	
	// SHARD GROUP FUNCTIONS //
	public long numShardGroups()
	{
		return numShardGroups(this.getHead().getHeight());
	}

	public long numShardGroups(final long height)
	{
		// TODO dynamic shard group count from height / epoch
		return Universe.getDefault().shardGroupCount();
	}

	// ASYNC ATOM LISTENER //
	private EventListener asyncAtomListener = new EventListener()
	{
		@Subscribe
		public void on(AtomExceptionEvent event)
		{
			if (event.getException() instanceof StateLockedException)
				return;
		}
		
		@Subscribe
		public void on(AtomDiscardedEvent event) 
		{
			Exception ex = new ValidationException("Atom "+event.getPendingAtom().getHash()+" was discarded due to: "+event.getMessage());
			ledgerLog.warn(Ledger.this.context.getName()+": "+ex.getMessage());
		}

		@Subscribe
		public void on(AtomCommitTimeoutEvent event)
		{
		}
	};
	
	// SYNCHRONOUS ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomCommitEvent event) 
		{
			try
			{
				if (event.getPendingAtom().getCertificate().getDecision().equals(StateDecision.POSITIVE))
					Ledger.this.context.getEvents().post(new AtomAcceptedEvent(event.getPendingAtom()));
				if (event.getPendingAtom().getCertificate().getDecision().equals(StateDecision.NEGATIVE))
					Ledger.this.context.getEvents().post(new AtomRejectedEvent(event.getPendingAtom()));
			}
			catch (Exception ex)
			{
				ledgerLog.fatal(Ledger.this.context.getName()+": Atom "+event.getAtom().getHash()+" commit failed", ex);
				Ledger.this.context.getEvents().post(new AtomExceptionEvent(event.getPendingAtom(), ex));
			}
		}

		@Subscribe
		public void on(final AtomRejectedEvent event) 
		{
			ledgerLog.error(Ledger.this.context.getName()+": Atom "+event.getAtom().getHash()+" rejected", event.getPendingAtom().thrown());
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event) throws StateLockedException 
		{
			ledgerLog.error(Ledger.this.context.getName()+": Atom "+event.getAtom().getHash()+" commit aborted due to timeout");
			Ledger.this.stateAccumulator.unlock(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomExceptionEvent event) throws StateLockedException 
		{
			// FIXME not a fan of isolating the StateLockedException in this way
			if (event.getException() instanceof StateLockedException)
				return;
			
			ledgerLog.error(Ledger.this.context.getName()+": Atom "+event.getAtom().getHash()+" aborted", event.getException());
			Ledger.this.stateAccumulator.unlock(event.getPendingAtom());
		}
	};
	
	// SYNCHRONOUS BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		private long lastThroughputUpdate = System.currentTimeMillis();
		private long lastThroughputReset = System.currentTimeMillis();
		private long lastThroughputPersistedAtoms = 0;
		private long lastThroughputPersistedParticles = 0;
		private long lastThroughputCommittedAtoms = 0;
		private long lastThroughputRejectedAtoms = 0;
		private long lastThroughputShardsTouched = 0;

		@Subscribe
		public void on(final BlockCommittedEvent event) throws StateLockedException, IOException 
		{
			update(event.getBlock());
		}
		
		@Subscribe
		public void on(final SyncBlockEvent event) throws StateLockedException, IOException 
		{
			sync(event.getBlock());
		}

		private void update(final Block block) throws StateLockedException, IOException
		{
			if (block.getHeader().getPrevious().equals(Ledger.this.getHead().getHash()) == false)
			{
				ledgerLog.error(Ledger.this.context.getName()+": Committed block "+block.getHeader()+" does not attach to current head "+Ledger.this.getHead());
				return;
			}
			
			Ledger.this.update(block);
			ledgerLog.info(Ledger.this.context.getName()+": Committed block with "+block.getHeader().getInventory(InventoryType.ATOMS).size()+" atoms and "+block.getHeader().getInventory(InventoryType.CERTIFICATES).size()+" certificates "+block.getHeader());
			stats(block);
		}
			
		private void sync(final Block block) throws StateLockedException, IOException
		{
			if (block.getHeader().getPrevious().equals(Ledger.this.getHead().getHash()) == false)
			{
				ledgerLog.error(Ledger.this.context.getName()+": Synced block "+block.getHeader()+" does not attach to current head "+Ledger.this.getHead());
				return;
			}
			
			Ledger.this.head.set(block.getHeader());
			Ledger.this.context.getNode().setHead(block.getHeader());
			ledgerLog.info(Ledger.this.context.getName()+": Synced block with "+block.getHeader().getInventory(InventoryType.ATOMS).size()+" atoms and "+block.getHeader().getInventory(InventoryType.CERTIFICATES).size()+" certificates "+block.getHeader());
			stats(block);
		}

		private void stats(final Block block) throws StateLockedException, IOException
		{
			Collection<Hash> stateAccumulatorLocked = context.getLedger().getStateAccumulator().locked();
			ledgerLog.info(Ledger.this.context.getName()+": "+stateAccumulatorLocked.size()+" locked in accumulator "+stateAccumulatorLocked.stream().reduce((a, b) -> Hash.from(a,b)));

			long numShardGroups = Ledger.this.numShardGroups(block.getHeader().getHeight());
			Set<Long> shardGroupsTouched = new HashSet<Long>();
			for (AtomCertificate atomCertificate : block.getCertificates())
			{
				shardGroupsTouched.clear();
				for (StateCertificate stateCertificate : atomCertificate.getAll())
					shardGroupsTouched.add(ShardMapper.toShardGroup(stateCertificate.getState().get(), numShardGroups));
					
				this.lastThroughputShardsTouched += shardGroupsTouched.size();
				
				if (atomCertificate.getDecision().equals(StateDecision.POSITIVE) == true)
				{
					this.lastThroughputCommittedAtoms++;
					Ledger.this.context.getMetaData().increment("ledger.commits.certificates.accept");
				}
				else if (atomCertificate.getDecision().equals(StateDecision.NEGATIVE) == true)
				{
					this.lastThroughputRejectedAtoms++;
					Ledger.this.context.getMetaData().increment("ledger.commits.certificates.reject");
				}
				
				Ledger.this.context.getMetaData().increment("ledger.commits.certificates");
			}

			for (Atom atom : block.getAtoms())
			{
				if (ledgerLog.hasLevel(Logging.DEBUG))
					ledgerLog.debug(Ledger.this.context.getName()+": Pre-committed atom "+atom.getHash()+" in "+block.getHeader());
				
				Ledger.this.context.getMetaData().increment("ledger.processed.atoms.local");
				this.lastThroughputPersistedAtoms++;
				this.lastThroughputPersistedParticles += atom.getParticles().size();

				if (this.lastThroughputShardsTouched > 0 && (this.lastThroughputCommittedAtoms > 0 || this.lastThroughputRejectedAtoms > 0))
					Ledger.this.context.getMetaData().increment("ledger.processed.atoms.total", (numShardGroups / (this.lastThroughputShardsTouched / (this.lastThroughputCommittedAtoms + this.lastThroughputRejectedAtoms))));
			}
			
			if (System.currentTimeMillis() - this.lastThroughputUpdate > TimeUnit.SECONDS.toMillis(10))
			{
				int seconds = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - this.lastThroughputReset);
				Ledger.this.context.getMetaData().put("ledger.throughput.atoms.local", (this.lastThroughputPersistedAtoms / seconds));
				Ledger.this.context.getMetaData().put("ledger.throughput.particles", (this.lastThroughputPersistedParticles / seconds));
				if (this.lastThroughputShardsTouched > 0 && this.lastThroughputCommittedAtoms > 0)
				{
					Ledger.this.context.getMetaData().put("ledger.throughput.shards.touched", (this.lastThroughputShardsTouched / (this.lastThroughputCommittedAtoms + this.lastThroughputRejectedAtoms)));
					Ledger.this.context.getMetaData().put("ledger.throughput.atoms.total", (this.lastThroughputPersistedAtoms / seconds) * (numShardGroups / (this.lastThroughputShardsTouched / (this.lastThroughputCommittedAtoms + this.lastThroughputRejectedAtoms))));
				}
				
				this.lastThroughputUpdate = System.currentTimeMillis();

				if (System.currentTimeMillis() - this.lastThroughputReset > TimeUnit.SECONDS.toMillis(60))
				{
					this.lastThroughputReset = System.currentTimeMillis();
					this.lastThroughputPersistedAtoms = this.lastThroughputPersistedParticles = 0; 
					this.lastThroughputCommittedAtoms = this.lastThroughputRejectedAtoms = this.lastThroughputShardsTouched = 0;
				}
			}
		}
	};
	
	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncStatusChangeEvent event) 
		{
			// Only flush state accumulator on sync > false ... on sync > true it will contain the remnants of the sync process
			// TODO can push directly to accumulator from the SyncHandler in the same was as the Atom/StateHandler injections?
			if (event.isSynced() == false)
			{
				ledgerLog.info(Ledger.this.context.getName()+": Sync status changed to "+event.isSynced()+", flushing state accumulator");
				Ledger.this.stateAccumulator.reset();
			}
		}
	};
	
	// PEER LISTENER //
	private EventListener peerListener = new EventListener()
	{
    	@Subscribe
		public void on(final PeerConnectedEvent event)
		{
    		try
    		{
    			long localShardGroup = ShardMapper.toShardGroup(Ledger.this.context.getNode().getIdentity(), Ledger.this.numShardGroups());
    			long remoteShardGroup = ShardMapper.toShardGroup(event.getPeer().getNode().getIdentity(), Ledger.this.numShardGroups());
    			if (localShardGroup != remoteShardGroup)
    				return;
    			
    			// Reasons NOT to ask for all the pool inventories
    			if (Ledger.this.isSynced() == false || 
    				//event.getPeer().getNode().isSynced() == false || 
    				Ledger.this.context.getNode().isInSyncWith(event.getPeer().getNode(), Node.OOS_TRIGGER_LIMIT) == false)
    				return;
    			
   				Ledger.this.context.getNetwork().getMessaging().send(new SyncAcquiredMessage(Ledger.this.getHead()), event.getPeer());
   				ledgerLog.info(Ledger.this.context.getName()+": Requesting full inventory from "+event.getPeer());
    		}
    		catch (IOException ioex)
    		{
    			ledgerLog.error(Ledger.this.context.getName()+": Failed to request sync connected items from "+event.getPeer(), ioex);
    		}
		}
	};
}
 