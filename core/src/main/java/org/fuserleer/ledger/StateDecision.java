package org.fuserleer.ledger;

import com.fasterxml.jackson.annotation.JsonValue;

public enum StateDecision
{
	POSITIVE, NEGATIVE, UNKNOWN, ERROR;
	
	@JsonValue
	@Override
	public String toString() 
	{
		return this.name();
	}
}
