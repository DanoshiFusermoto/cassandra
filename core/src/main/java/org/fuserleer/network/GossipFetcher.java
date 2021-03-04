package org.fuserleer.network;

import java.util.Collection;

import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;

public interface GossipFetcher
{
	public Collection<? extends Primitive> fetch(Collection<Hash> items) throws Throwable;
}
