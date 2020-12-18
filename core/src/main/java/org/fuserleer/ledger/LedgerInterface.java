package org.fuserleer.ledger;

import java.io.IOException;

import org.fuserleer.common.Primitive;
import org.fuserleer.database.Indexable;
import org.fuserleer.ledger.atoms.Particle.Spin;

interface LedgerInterface
{
	public <T extends Primitive> T get(final Indexable indexable, final Class<T> container) throws IOException;
	public <T extends Primitive> SearchResponse<T> get(final SearchQuery query, final Class<T> type, final Spin spin) throws IOException;
	public boolean state(final Indexable indexable) throws IOException; 
}