package org.fuserleer.common;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Direction 
{
	OUTBOUND,
	INBOUND;

	@JsonValue
	@Override
	public String toString() 
	{
		return this.name();
	}
}