package org.fuserleer.ledger;

import java.io.IOException;

import org.fuserleer.utils.UInt256;

interface StateProvider
{
	CommitStatus has(final StateKey<?, ?> key) throws IOException;
	UInt256 get(final StateKey<?, ?> key) throws IOException; 
}
