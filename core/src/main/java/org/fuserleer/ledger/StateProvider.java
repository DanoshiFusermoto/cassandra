package org.fuserleer.ledger;

import java.io.IOException;

import org.fuserleer.database.Indexable;

public interface StateProvider
{
	public CommitState state(final Indexable indexable) throws IOException; 
}
