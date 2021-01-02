package org.fuserleer.ledger;

import java.io.IOException;

interface StateProvider
{
	StateOpResult<?> evaluate(final StateOp stateOp) throws IOException; 
}
