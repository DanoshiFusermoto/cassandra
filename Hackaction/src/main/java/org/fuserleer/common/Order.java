package org.fuserleer.common;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Order
{
	ASCENDING,
	DESCENDING;

	@JsonValue
	@Override
	public String toString() 
	{
		return this.name();
	}
}
