package org.fuserleer.ledger;

public enum CommitState
{
	NONE(0), LOCKED(1), PRECOMMITTED(2), COMMITTED(3), ABORTED(10);
	
	private final int index;
	
	CommitState(int index)
	{
		this.index = index;
	}
	
	public int index()
	{
		return this.index;
	}
}
