package org.fuserleer.ledger;

public enum CommitStatus
{
	NONE(0), PREPARED(1), LOCKED(2), PROVISIONING(3), PROVISIONED(4), EXECUTED(5), COMMITTED(6), ABORTED(10);
	
	private final int index;
	
	CommitStatus(int index)
	{
		this.index = index;
	}
	
	public int index()
	{
		return this.index;
	}
	
	public boolean lessThan(CommitStatus status)
	{
		return this.index < status.index;
	}

	public boolean greaterThan(CommitStatus status)
	{
		return this.index > status.index;
	}
}
