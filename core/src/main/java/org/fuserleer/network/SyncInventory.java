package org.fuserleer.network;

import java.util.Collection;

import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;

public interface SyncInventory
{
	public Collection<Hash> process(final Class<? extends Primitive> type, final Collection<Hash> items) throws Throwable;
}
