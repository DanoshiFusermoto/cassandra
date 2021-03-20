package org.fuserleer.ledger.events;

import org.fuserleer.events.Event;

final public class SyncStatusChangeEvent implements Event
{
	private final boolean synced;
	
	public SyncStatusChangeEvent(final boolean synced)
	{
		super();
		
		this.synced = synced;
	}
	
	public boolean isSynced()
	{
		return this.synced;
	}
}
