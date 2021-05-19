package org.fuserleer.ledger.atoms;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.ledger.AtomVote;
import org.fuserleer.ledger.StateInput;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;
import org.fuserleer.utils.UInt256;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.automataext")
public final class AutomataExtension extends BasicObject implements Primitive
{
	@JsonProperty("Atom")
	@DsonOutput(Output.ALL)
	private Atom atom;

	@JsonProperty("particles")
	@DsonOutput(Output.ALL)
	private List<Particle> particles;
	
	@JsonProperty("votes")
	@DsonOutput(Output.ALL)
	private List<AtomVote> votes;

	@JsonProperty("shards")
	@JsonDeserialize(as=LinkedHashSet.class)
	@DsonOutput(Output.ALL)
	private Set<UInt256> shards;

	@JsonProperty("inputs")
	@JsonDeserialize(as=LinkedHashSet.class)
	@DsonOutput(Output.ALL)
	private Set<StateInput> inputs;

	public AutomataExtension()
	{
		super();
	}

	public AutomataExtension(final Atom atom, final Collection<StateInput> inputs, final Collection<UInt256> shards, final Collection<AtomVote> votes)
	{
		this();
		
		Objects.requireNonNull(atom, "Atom is null");
		Numbers.isZero(atom.getAutomata().size(), "Atom has no automata particles");
		Objects.requireNonNull(votes, "Votes is null");
		Numbers.isZero(votes.size(), "Votes is empty");
		Objects.requireNonNull(shards, "Shards is null");
		Numbers.isZero(shards.size(), "Shards is empty");
		Numbers.isZero(atom.getAutomata().size(), "Particles is empty");
		
		this.atom = new Atom(atom.getParticles());
		this.votes = votes.stream().sorted().collect(Collectors.toList());
		this.particles = atom.getAutomata().stream().sorted().collect(Collectors.toList());
		this.shards = new LinkedHashSet<UInt256>(shards);
		this.inputs = new LinkedHashSet<StateInput>(inputs.stream().sorted().collect(Collectors.toList()));
	}

	public Atom getAtom()
	{
		return this.atom;
	}

	public Set<StateInput> getInputs()
	{
		return Collections.unmodifiableSet(this.inputs);
	}

	public Set<UInt256> getShards()
	{
		return Collections.unmodifiableSet(this.shards);
	}

	public List<Particle> getParticles()
	{
		return Collections.unmodifiableList(this.particles);
	}

	public List<AtomVote> getVotes()
	{
		return Collections.unmodifiableList(this.votes);
	}
}
