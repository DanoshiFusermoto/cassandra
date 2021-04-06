package org.fuserleer.crypto;

import java.util.Objects;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.ledger.StateDecision;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("crypto.certificate")
public abstract class Certificate extends BasicObject implements Primitive
{
	@JsonProperty("decision")
	@DsonOutput(Output.ALL)
	private StateDecision decision;

	protected Certificate()
	{
		// For serializer
	}
	
	protected Certificate(final StateDecision decision)
	{
		Objects.requireNonNull(decision, "Decision is null");

		this.decision = decision;
		if (decision.equals(StateDecision.ERROR) == true || decision.equals(StateDecision.UNKNOWN) == true)
			throw new IllegalArgumentException("Decision "+decision+" is unsupported at present");
	}

	public final StateDecision getDecision()
	{
		return this.decision;
	}

	public abstract <T> T getObject();
}
