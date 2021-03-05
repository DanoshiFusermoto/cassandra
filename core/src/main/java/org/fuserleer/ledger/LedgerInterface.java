package org.fuserleer.ledger;

import java.io.IOException;
import java.util.concurrent.Future;

import org.fuserleer.common.Primitive;
import org.fuserleer.ledger.atoms.Particle.Spin;

interface LedgerInterface
{
	/**
	 * 
	 * Get a primitive containing the state from the ledger
	 * 
	 * @param indexable
	 * @return
	 * @throws IOException
	 */
	public <T extends Primitive> Future<SearchResult> get(final StateSearchQuery query); //StateKey<?, ?> key);
	
	public <T extends Primitive> Future<AssociationSearchResponse> get(final AssociationSearchQuery query, final Spin spin);
}