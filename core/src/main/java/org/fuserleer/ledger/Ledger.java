package org.fuserleer.ledger;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
import org.fuserleer.ledger.messages.GetAtomPoolInventoryMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.peers.ConnectedPeer;
import org.fuserleer.network.peers.PeerState;
import org.fuserleer.network.peers.events.PeerConnectedEvent;
import org.fuserleer.network.peers.filters.StandardPeerFilter;
import org.fuserleer.node.Node;
import org.fuserleer.time.Time;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.google.common.eventbus.AllowConcurrentEvents;
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
	private final VoteRegulator voteRegulator;
	
	private final transient AtomicReference<BlockHeader> head;
	
	public Ledger(Context context)
	{
		this.context = Objects.requireNonNull(context);

		this.ledgerStore = new LedgerStore(this.context);

		this.voteRegulator = new VoteRegulator(this.context);
		this.blockHandler = new BlockHandler(this.context, this.voteRegulator);
		this.syncHandler = new SyncHandler(this.context);
		this.atomPool = new AtomPool(this.context);
		this.atomHandler = new AtomHandler(this.context);
		this.stateAccumulator = new StateAccumulator(this.context, "ledger", this.ledgerStore);
		this.stateHandler = new StateHandler(this.context);
		this.statePool = new StatePool(this.context);
		
		this.ledgerSearch = new LedgerSearch(this.context);

		this.head = new AtomicReference<BlockHeader>(this.context.getNode().getHead());

		ledgerLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN | Logging.WARN);
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			this.ledgerStore.start();
			
			integrity();
			
			this.context.getEvents().register(this.syncBlockListener);
			this.context.getEvents().register(this.asyncAtomListener);
			this.context.getEvents().register(this.syncAtomListener);
			this.context.getEvents().register(this.peerListener);

			this.voteRegulator.start();
			this.syncHandler.start();
			this.blockHandler.start();
			this.atomPool.start();
			this.atomHandler.start();
			this.stateAccumulator.reset();
			this.statePool.start();
			this.stateHandler.start();
			
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

		this.ledgerSearch.stop();
		
		this.statePool.stop();
		this.stateHandler.stop();
		this.atomHandler.stop();
		this.atomPool.stop();
		this.blockHandler.stop();
		this.syncHandler.stop();
		this.voteRegulator.stop();
		this.ledgerStore.stop();
	}
	
	public void clean() throws IOException
	{
		this.ledgerStore.clean();
	}
	
	private void integrity() throws IOException, ValidationException, StateLockedException
	{
		BlockHeader nodeBlockHeader = this.context.getNode().getHead();
		// Check if this is just a new ledger store and doesn't need integrity or recovery
		if (nodeBlockHeader.equals(Universe.getDefault().getGenesis().getHeader()) == true && this.ledgerStore.has(nodeBlockHeader.getHash()) == false)
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
				pendingAtom.lock();
				pendingAtom.provision(Universe.getDefault().getGenesis().getHeader());
				pendingAtom.setStatus(CommitStatus.PROVISIONED);
				pendingAtom.execute();
				this.ledgerStore.commit(pendingAtom.getCommitOperation(Type.ACCEPT));
			}
			
			return;
		}
		else if (this.ledgerStore.has(nodeBlockHeader.getHash()) == false)
		{
			// TODO recover to the best head with committed state
			ledgerLog.error(Ledger.this.context.getName()+": Local node block header "+nodeBlockHeader+" not found in store");
			throw new UnsupportedOperationException("Integrity recovery not implemented");
		}
		else
		{
			// TODO block header is known but is it the strongest head that represents state?
			setHead(nodeBlockHeader);
		}
	}
	
	AtomHandler getAtomHandler()
	{
		return this.atomHandler;
	}

	VoteRegulator getVoteRegulator()
	{
		return this.voteRegulator;
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

	StateAccumulator getStateAccumulator()
	{
		return this.stateAccumulator;
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

	void setHead(BlockHeader head)
	{
		this.head.set(Objects.requireNonNull(head));
	}

	public <T extends Primitive> T get(Hash hash, Class<T> primitive) throws IOException
	{
		return this.ledgerStore.get(hash, primitive);
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
	
	private long nextSyncStatusCheck = 0;
	public boolean isSynced()
	{
		return isSynced(false);
	}
	
	synchronized boolean isSynced(boolean check)
	{
		if (this.context.getConfiguration().has("singleton") == true)
		{
			this.context.getNode().setSynced(true);
			return true;
		}
		
		// No point doing this multiple times per second as is quite expensive
		if (Time.getSystemTime() > this.nextSyncStatusCheck || check == true)
		{
			this.nextSyncStatusCheck = Time.getSystemTime()+TimeUnit.SECONDS.toMillis(1);
			
			List<ConnectedPeer> syncPeers = this.context.getNetwork().get(StandardPeerFilter.build(this.context).setShardGroup(ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups())).setStates(PeerState.CONNECTED)); 
			if (syncPeers.isEmpty() == true)
				return false;
	
			// Out of sync?
			PendingBranch bestBranch = this.getBlockHandler().getBestBranch();
			int OOSTrigger = bestBranch == null ? Node.OOS_TRIGGER_LIMIT : Node.OOS_TRIGGER_LIMIT + bestBranch.size(); 
			// TODO need to deal with forks that don't converge with local chain due to safety break
			if (this.context.getNode().isSynced() == true && syncPeers.stream().anyMatch(cp -> cp.getNode().isAheadOf(this.context.getNode(), OOSTrigger)) == true)
			{
				this.context.getNode().setSynced(false);
				ledgerLog.info(this.context.getName()+": Out of sync state detected with OOS_TRIGGER limit of "+OOSTrigger);
			}
			else if (this.context.getNode().isSynced() == false)
			{
				ConnectedPeer strongestPeer = null;
				for (ConnectedPeer syncPeer : syncPeers)
				{
					if (strongestPeer == null || syncPeer.getNode().getHead().getHeight() > strongestPeer.getNode().getHead().getHeight())
						strongestPeer = syncPeer;
				}
				
				if (strongestPeer != null && 
					(strongestPeer.getNode().isInSyncWith(this.context.getNode(), 0) == true || this.context.getNode().isAheadOf(strongestPeer.getNode(), 0) == true))
				{
					ledgerLog.info(this.context.getName()+": Synced state reaquired");
					this.context.getNode().setSynced(true);
				}
			}
		}

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
				Ledger.this.stateAccumulator.commit(event.getPendingAtom());
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
		public void on(final AtomCommitTimeoutEvent event) 
		{
			ledgerLog.error(Ledger.this.context.getName()+": Atom "+event.getAtom().getHash()+" aborted due to timeout");
			Ledger.this.stateAccumulator.abort(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomExceptionEvent event) 
		{
			// FIXME not a fan of isolating the StateLockedException in this way
			if (event.getException() instanceof StateLockedException)
				return;
			
			ledgerLog.error(Ledger.this.context.getName()+": Atom "+event.getAtom().getHash()+" aborted", event.getException());
			Ledger.this.stateAccumulator.abort(event.getPendingAtom());
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
		public void on(final BlockCommittedEvent event) 
		{
			process(event.getBlock());
		}
		
		@Subscribe
		public void on(final SyncBlockEvent event) 
		{
			process(event.getBlock());
		}

		private void process(final Block block)
		{
			if (block.getHeader().getPrevious().equals(Ledger.this.getHead().getHash()) == false)
			{
				ledgerLog.error(Ledger.this.context.getName()+": Committed block "+block.getHeader()+" does not attach to current head "+Ledger.this.getHead());
				return;
			}
			
			// Clone it to make sure to extract the header
			Ledger.this.setHead(block.getHeader());
			Ledger.this.context.getNode().setHead(block.getHeader());
			ledgerLog.info(Ledger.this.context.getName()+": Committed block with "+block.getHeader().getInventory(InventoryType.ATOMS).size()+" atoms and "+block.getHeader().getInventory(InventoryType.CERTIFICATES).size()+" certificates "+block.getHeader());
			Ledger.this.context.getMetaData().increment("ledger.commits.atoms.local", block.getHeader().getInventory(InventoryType.ATOMS).size());
			Ledger.this.context.getMetaData().increment("ledger.commits.certificates", block.getHeader().getInventory(InventoryType.CERTIFICATES).size());
			
			long numShardGroups = Ledger.this.numShardGroups(block.getHeader().getHeight());
			Set<Long> shardGroupsTouched = new HashSet<Long>();
			for (AtomCertificate atomCertificate : block.getCertificates())
			{
				shardGroupsTouched.clear();
				for (StateCertificate stateCertificate : atomCertificate.getAll())
					shardGroupsTouched.add(ShardMapper.toShardGroup(stateCertificate.getState().get(), numShardGroups));
					
				this.lastThroughputShardsTouched += shardGroupsTouched.size();
				
				if (atomCertificate.getDecision().equals(StateDecision.POSITIVE) == true)
					this.lastThroughputCommittedAtoms++;
				else if (atomCertificate.getDecision().equals(StateDecision.POSITIVE) == true)
					this.lastThroughputRejectedAtoms++;
			}

			for (Atom atom : block.getAtoms())
			{
				if (ledgerLog.hasLevel(Logging.DEBUG))
					ledgerLog.debug(Ledger.this.context.getName()+": Pre-committed atom "+atom.getHash()+" in "+block.getHeader());
				
				Ledger.this.context.getMetaData().increment("ledger.processed.atoms.local");
				this.lastThroughputPersistedAtoms++;
				this.lastThroughputPersistedParticles += atom.getParticles().size();

				if (this.lastThroughputShardsTouched > 0 && this.lastThroughputCommittedAtoms > 0)
					Ledger.this.context.getMetaData().increment("ledger.processed.atoms.total", (numShardGroups / (this.lastThroughputShardsTouched / this.lastThroughputCommittedAtoms)));
			}
			
			if (System.currentTimeMillis() - this.lastThroughputUpdate > TimeUnit.SECONDS.toMillis(10))
			{
				int seconds = (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - this.lastThroughputReset);
				Ledger.this.context.getMetaData().put("ledger.throughput.atoms.local", (this.lastThroughputPersistedAtoms / seconds));
				Ledger.this.context.getMetaData().put("ledger.throughput.particles", (this.lastThroughputPersistedParticles / seconds));
				if (this.lastThroughputShardsTouched > 0 && this.lastThroughputCommittedAtoms > 0)
				{
					Ledger.this.context.getMetaData().put("ledger.throughput.shards.touched", (this.lastThroughputShardsTouched / this.lastThroughputCommittedAtoms));
					Ledger.this.context.getMetaData().put("ledger.throughput.atoms.total", (this.lastThroughputPersistedAtoms / seconds) * (numShardGroups / (this.lastThroughputShardsTouched / this.lastThroughputCommittedAtoms)));
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
	
	// PEER LISTENER //
	private EventListener peerListener = new EventListener()
	{
    	@Subscribe
		public void on(PeerConnectedEvent event)
		{
/*    		try
    		{
    			// TODO needs requesting on connect from synced nodes only
    			if (event.getPeer().getProtocol().equals(Protocol.TCP) == true)
    				Ledger.this.context.getNetwork().getMessaging().send(new GetAtomPoolInventoryMessage(), event.getPeer());
    		}
    		catch (IOException ioex)
    		{
    			ledgerLog.error("Failed to request atom pool items from "+event.getPeer(), ioex);
    		}*/
		}
	};
}
 