package org.fuserleer.ledger;

import java.io.IOException;

import org.fuserleer.common.Primitive;
import org.fuserleer.database.Indexable;
import org.fuserleer.ledger.atoms.Particle.Spin;

interface LedgerInterface
{
	/**
	 * 
	 * Get a indexable from the ledger
	 * 
	 * @param indexable
	 * @return
	 * @throws IOException
	 */
	public <T extends Primitive> T get(final Indexable indexable) throws IOException;
	
	/**
	 * Get an indexable from the ledger wrapped in a parent container if possible.
	 * 
	 * @param indexable
	 * @param container
	 * @return
	 * @throws IOException
	 */
	public <T extends Primitive> T get(final Indexable indexable, final Class<T> container) throws IOException;
	public <T extends Primitive> SearchResponse<T> get(final SearchQuery query, final Class<T> type, final Spin spin) throws IOException;
}