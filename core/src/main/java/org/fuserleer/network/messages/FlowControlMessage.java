package org.fuserleer.network.messages;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.network.messaging.Message;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("network.message.flow.control")
public final class FlowControlMessage extends Message
{
	@JsonProperty("sequences")
	@DsonOutput(Output.ALL)
	private Set<Long> sequences;
	
	FlowControlMessage()
	{
		super();
	}
	
	public FlowControlMessage(Collection<Long> sequences)
	{
		super();
		
		if (Objects.requireNonNull(sequences).isEmpty() == true)
			throw new IllegalArgumentException("Sequences is empty");
		
		this.sequences = new HashSet<Long>(sequences);
	}

	public Set<Long> getSequences()
	{
		return sequences;
	}
}
