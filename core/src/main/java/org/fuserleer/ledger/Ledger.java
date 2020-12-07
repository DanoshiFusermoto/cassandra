package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
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
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.events.AtomCommittedEvent;
import org.fuserleer.ledger.events.AtomDiscardedEvent;
import org.fuserleer.ledger.events.AtomErrorEvent;
import org.fuserleer.ledger.events.AtomExceptionEvent;
import org.fuserleer.ledger.events.AtomPersistedEvent;
import org.fuserleer.ledger.events.BlockCommittedEvent;
import org.fuserleer.ledger.messages.GetAtomPoolMessage;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.network.Protocol;
import org.fuserleer.network.peers.events.PeerConnectedEvent;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;

public final class Ledger implements Service
{
	private static final Logger ledgerLog = Logging.getLogger("ledger");

	private final Context context;
	
	private final AtomPool atomPool;
	private final AtomHandler atomHandler;
	
	private final BlockHandler blockHandler;

	private final LedgerStore ledgerStore;
	private final VoteRegulator voteRegulator;
	
	private final transient AtomicReference<BlockHeader> head;
	
	private final Map<Hash, AtomFuture> atomFutures = Collections.synchronizedMap(new LinkedHashMap<Hash, AtomFuture>());

	public Ledger(Context context)
	{
		this.context = Objects.requireNonNull(context);

		this.ledgerStore = new LedgerStore(this.context);

		this.voteRegulator = new VoteRegulator(this.context);
		this.blockHandler = new BlockHandler(this.context, this.voteRegulator);
		this.atomPool = new AtomPool(this.context, this.voteRegulator);
		this.atomHandler = new AtomHandler(this.context);
		
		this.head = new AtomicReference<BlockHeader>(this.context.getNode().getHead());
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

			this.blockHandler.start();
			this.atomPool.start();
			this.atomHandler.start();
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

		this.atomHandler.stop();
		this.atomPool.stop();
		this.blockHandler.stop();
		this.ledgerStore.stop();
	}
	
	public void clean() throws IOException
	{
		this.ledgerStore.clean();
	}
	
	private void integrity() throws IOException, ValidationException
	{
		BlockHeader nodeBlockHeader = this.context.getNode().getHead();
		// Check if this is just a new ledger store and doesn't need integrity or recovery
		if (nodeBlockHeader.equals(Universe.getDefault().getGenesis()) == true && this.ledgerStore.has(nodeBlockHeader.getHash()) == false)
		{
			// Store the genesis block primitive
			this.ledgerStore.store(Universe.getDefault().getGenesis());

			// TODO need to commit the state here but components are not ready yet
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

	public BlockHandler getBlockHandler()
	{
		return this.blockHandler;
	}

	public AtomPool getAtomPool()
	{
		return this.atomPool;
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
	
	public Future<BlockHeader> submit(Atom atom) throws InterruptedException
	{
		Objects.requireNonNull(atom);
		
		synchronized(this.atomFutures)
		{
			AtomFuture atomFuture = this.atomFutures.get(atom.getHash());
			if (atomFuture == null)
			{
				atomFuture = new AtomFuture(atom);
				this.atomFutures.put(atom.getHash(), atomFuture);
				
				try
				{
					// TODO what happens on a false return?
					if (this.atomHandler.submit(atom) == false)
						throw new RejectedExecutionException();
				}
				catch(Throwable t)
				{
					this.atomFutures.remove(atom.getHash());
					atomFuture.completeExceptionally(t);
					throw t;
				}
			}

			return atomFuture;
		}
	}
	
	// ASYNC ATOM LISTENER //
	private EventListener asyncAtomListener = new EventListener()
	{
		@Subscribe
		@AllowConcurrentEvents
		public void on(AtomCommittedEvent atomCommittedEvent) 
		{
			AtomFuture atomFuture = Ledger.this.atomFutures.remove(atomCommittedEvent.getAtom().getHash());
			if (atomFuture != null)
				atomFuture.complete(atomCommittedEvent.getBlockHeader());
		}

		@Subscribe
		public void on(AtomErrorEvent atomErrorEvent)
		{
			AtomFuture atomFuture = Ledger.this.atomFutures.remove(atomErrorEvent.getAtom().getHash());
			if (atomFuture != null)
				atomFuture.completeExceptionally(atomErrorEvent.getError());
		}
		
		@Subscribe
		public void on(AtomExceptionEvent atomExceptionEvent)
		{
			AtomFuture atomFuture = Ledger.this.atomFutures.remove(atomExceptionEvent.getAtom().getHash());
			if (atomFuture != null)
				atomFuture.completeExceptionally(atomExceptionEvent.getException());
		}
		
		@Subscribe
		public void on(AtomDiscardedEvent atomDiscardedEvent) 
		{
			Exception ex = new ValidationException("Atom "+atomDiscardedEvent.getAtom().getHash()+" was discarded due to: "+atomDiscardedEvent.getMessage());
			ledgerLog.warn(Ledger.this.context.getName()+": "+ex.getMessage());

			AtomFuture atomFuture = Ledger.this.atomFutures.remove(atomDiscardedEvent.getAtom().getHash());
			if (atomFuture != null)
				atomFuture.completeExceptionally(ex);
		}

		// TODO want to float up to listeners registered here about onTimeout, onVerified etc for application domain?
	};
	
	// SYNCHRONOUS ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(AtomPersistedEvent actionPersistedEvent) 
		{
			if (Ledger.this.atomPool.add(actionPersistedEvent.getAtom()) == false)
				ledgerLog.error(Ledger.this.context.getName()+": Atom "+actionPersistedEvent.getAtom().getHash()+" not added to atom pool");
		}

		@Subscribe
		public void on(AtomCommittedEvent actionCommittedEvent) 
		{
			Ledger.this.atomPool.remove(actionCommittedEvent.getAtom().getHash());
			
			if (ledgerLog.hasLevel(Logging.DEBUG))
				ledgerLog.debug(Ledger.this.context.getName()+": Committed atom "+actionCommittedEvent.getAtom().getHash());
		}
	};
	
	// SYNCHRONOUS BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent) 
		{
			if (blockCommittedEvent.getBlockHeader().getPrevious().equals(Ledger.this.getHead().getHash()) == false)
			{
				ledgerLog.error(Ledger.this.context.getName()+": Committed block "+blockCommittedEvent.getBlockHeader()+" does not attach to current head "+Ledger.this.getHead());
				return;
			}
			
			// Clone it to make sure to extract the header
			BlockHeader blockHeader = blockCommittedEvent.getBlockHeader().clone();
			Ledger.this.setHead(blockHeader);
			ledgerLog.info(Ledger.this.context.getName()+": Committed block with "+blockHeader.getBloom().count()+" atoms "+blockHeader);
			Ledger.this.context.getMetaData().increment("ledger.commits.atoms", blockHeader.getBloom().count());
			
			Ledger.this.voteRegulator.addVotePower(blockCommittedEvent.getBlockHeader().getOwner(), blockCommittedEvent.getBlockHeader().getHeight());
			
			// TODO this will be different when sharded as need to wait for QCs from other shards and leave atom locked
			Ledger.this.atomPool.remove(blockHeader.getBloom());
		}
	};
	
	// PEER LISTENER //
	private EventListener peerListener = new EventListener()
	{
    	@Subscribe
		public void on(PeerConnectedEvent event)
		{
    		try
    		{
    			// TODO needs requesting on connect from synced nodes only
    			if (event.getPeer().getProtocol().equals(Protocol.TCP) == true)
    				Ledger.this.context.getNetwork().getMessaging().send(new GetAtomPoolMessage(), event.getPeer());
    		}
    		catch (IOException ioex)
    		{
    			ledgerLog.error("Failed to request atom pool items from "+event.getPeer(), ioex);
    		}
		}
	};
}
 