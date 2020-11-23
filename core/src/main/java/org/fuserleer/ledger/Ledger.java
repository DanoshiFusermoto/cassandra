package org.fuserleer.ledger;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.fuserleer.Context;
import org.fuserleer.Service;
import org.fuserleer.Universe;
import org.fuserleer.exceptions.StartupException;
import org.fuserleer.exceptions.TerminationException;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

import com.fasterxml.jackson.annotation.JsonGetter;

public final class Ledger implements Service
{
	private static final Logger ledgerLog = Logging.getLogger("ledger");

	private final Context context;
	
	private final LedgerStore ledgerStore;

	private final transient AtomicReference<BlockHeader> head;

	public Ledger(Context context)
	{
		this.context = Objects.requireNonNull(context);

		this.ledgerStore = new LedgerStore(this.context);
		this.head = new AtomicReference<BlockHeader>(this.context.getNode().getHead());
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			this.ledgerStore.start();
			
			integrity();
		}
		catch (Exception ex)
		{
			throw new StartupException(ex);
		}			
	}

	@Override
	public void stop() throws TerminationException
	{
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

}
