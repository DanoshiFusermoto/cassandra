package org.fuserleer.ledger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.Numbers;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.state.inputs")
class StateInputs implements Primitive
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;

	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;
		
	@JsonProperty("inputs")
	@JsonInclude(Include.ALWAYS)
	@DsonOutput(Output.ALL)
	private Map<Hash, Optional<UInt256>> inputs;
	
	
	@SuppressWarnings("unused")
	private StateInputs()
	{
		// FOR SERIALIZER
	}
	
	StateInputs(final Hash block, final Hash atom, final Map<StateKey<?, ?>, Optional<UInt256>> inputs)
	{
		Objects.requireNonNull(inputs, "State inputs is null");
		Numbers.isZero(inputs.size(), "State inputs is empty");
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom hash is zero");
		Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block hash is zero");

		this.atom = atom;
		this.block = block;
		this.inputs = inputs.entrySet().stream().collect(Collectors.toMap(e -> e.getKey().get(), e -> e.getValue()));
	}
	
	@Override
	public Hash getHash()
	{
		return Hash.from(this.block, this.atom);
	}

	public Hash getBlock()
	{
		return this.block;
	}

	public Hash getAtom()
	{
		return this.atom;
	}
	
	public Optional<UInt256> getInput(final StateKey<?, ?> stateKey)
	{
		Objects.requireNonNull(stateKey, "State key is null");
		return this.inputs.getOrDefault(stateKey.get(), null);
	}
}