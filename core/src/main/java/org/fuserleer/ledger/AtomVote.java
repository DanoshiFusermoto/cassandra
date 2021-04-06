package org.fuserleer.ledger;

import org.fuserleer.crypto.BLSKeyPair;
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.crypto.BLSSignature;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.SerializerId2;

@SerializerId2("ledger.atom.vote")
public final class AtomVote extends Vote<Hash, BLSKeyPair, BLSPublicKey, BLSSignature>
{
	@SuppressWarnings("unused")
	private AtomVote()
	{
		// SERIALIZER
	}
	
	public AtomVote(final Hash atom, final BLSPublicKey owner)
	{
		super(atom, StateDecision.POSITIVE, owner);
	}
	
	public Hash getAtom()
	{
		return getObject();
	}
}
