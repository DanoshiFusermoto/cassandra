package org.fuserleer.ledger.atoms;

import org.fuserleer.crypto.ComputeKey;
import org.fuserleer.crypto.Identity;
import org.fuserleer.ledger.StateAutomata;

public abstract class AutomataParticle extends SignedParticle implements StateAutomata
{
	protected AutomataParticle()
	{
		super();
	}
	
	protected AutomataParticle(Spin spin, Identity owner)
	{
		super(spin, owner);
	}
	
	public final Identity getIdentity()
	{
		return new Identity(Identity.COMPUTE, ComputeKey.from(getHash().toByteArray()));
	}
}
