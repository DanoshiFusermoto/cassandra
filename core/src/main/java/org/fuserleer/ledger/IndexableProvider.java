package org.fuserleer.ledger;

import java.io.IOException;

import org.fuserleer.database.Indexable;

interface IndexableProvider
{
	CommitState has(Indexable indexable) throws IOException;
}
