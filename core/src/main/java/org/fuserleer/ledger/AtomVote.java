package org.fuserleer.ledger;

import java.util.List;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.vote.atom.pool")
public final class AtomVote extends Vote<List<Hash>>
{
	private AtomVote()
	{
		// SERIALIZER
	}
	
	public AtomVote(final List<Hash> object, final ECPublicKey owner)
	{
		super(object, true, owner);
	}
	
	public List<Hash> getAtoms()
	{
		return getObject();
	}
}
