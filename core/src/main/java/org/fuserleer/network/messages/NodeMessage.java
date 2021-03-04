package org.fuserleer.network.messages;

import java.util.Objects;

import org.fuserleer.network.messaging.Message;
import org.fuserleer.node.Node;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 * TODO need a secure way to sign the Node object so that it may be passed around the network in peer updates and used in discovery processes
 * 
 */
@SerializerId2("message.node")
public class NodeMessage extends Message
{
	@JsonProperty("node")
	@DsonOutput(Output.ALL)
	private Node node;

	protected NodeMessage()
	{
		super();
	}
	
	public NodeMessage(final Node node)
	{
		super();

		this.node = new Node(Objects.requireNonNull(node, "Node is null"));
	}

	public Node getNode()
	{
		return this.node;
	}
}
