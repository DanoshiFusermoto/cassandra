package org.fuserleer.ledger;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.atom.vote")
public final class AtomVote extends Vote<Hash>
{
	@SuppressWarnings("unused")
	private AtomVote()
	{
		// SERIALIZER
	}
	
	public AtomVote(final Hash atom, final ECPublicKey owner)
	{
		super(atom, StateDecision.POSITIVE, owner);
	}
	
	public Hash getAtom()
	{
		return getObject();
	}
}
