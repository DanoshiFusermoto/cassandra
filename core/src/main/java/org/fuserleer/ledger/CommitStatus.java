package org.fuserleer.ledger;

public enum CommitStatus
{
	NONE(0), PREPARED(1), ACCEPTED(2), PROVISIONING(3), PROVISIONED(4), AUTOMATA(5), EXECUTED(6), COMPLETED(7), ABORTED(10);
	
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
