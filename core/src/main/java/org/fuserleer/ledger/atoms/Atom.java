package org.fuserleer.ledger.atoms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.ledger.StateObject;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.atom")
public final class Atom extends BasicObject implements Primitive
{
	@JsonProperty("particles")
	@DsonOutput(Output.ALL)
	private List<Particle> particles;
	
	@JsonProperty("automata")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private List<Particle> automata;

	@JsonProperty("states")
	@DsonOutput(value = {Output.API, Output.PERSIST})
	@JsonDeserialize(as=LinkedHashMap.class)
	private Map<Hash, StateObject> states;

	public Atom()
	{
		super();
	}

	public Atom(final Particle particle)
	{
		this();
		
		this.particles = new ArrayList<Particle>();
		this.particles.add(Objects.requireNonNull(particle, "Particle is null"));
	}

	public Atom(final Particle ... particles)
	{
		this();
		
		Objects.requireNonNull(particles, "Particles is null");
		Numbers.isZero(particles.length, "Particles is empty");
		
		Set<Particle> verifiedNonDuplicates = new LinkedHashSet<Particle>();
		for (int p = 0 ; p < particles.length ; p++)
		{
			if (verifiedNonDuplicates.add(particles[p]) == false)
				throw new IllegalArgumentException("Particle "+particles[p].getHash()+" is duplicated");
		}

		this.particles = new ArrayList<Particle>(verifiedNonDuplicates);
	}

	public Atom(final Collection<? extends Particle> particles)
	{
		this();
		
		Objects.requireNonNull(particles, "Particles is null");
		Numbers.isZero(particles.size(), "Particles is empty");
		
		Set<Particle> verifiedNonDuplicates = new LinkedHashSet<Particle>();
		for (Particle particle : particles)
		{
			if (verifiedNonDuplicates.add(particle) == false)
				throw new IllegalArgumentException("Particle "+particle.getHash()+" is duplicated");
		}
		
		this.particles = new ArrayList<Particle>(verifiedNonDuplicates);
	}
	
	public Atom(final Collection<? extends Particle> particles, final Collection<? extends Particle> automata)
	{
		this();
		
		Objects.requireNonNull(automata, "Automata is null");
		Objects.requireNonNull(particles, "Particles is null");
		Numbers.isZero(particles.size(), "Particles is empty");
		
		Set<Particle> verifiedNonDuplicates = new LinkedHashSet<Particle>();
		for (Particle particle : particles)
		{
			if (verifiedNonDuplicates.add(particle) == false)
				throw new IllegalArgumentException("Particle "+particle.getHash()+" is duplicated");
		}
		for (Particle particle : automata)
		{
			if (verifiedNonDuplicates.add(particle) == false)
				throw new IllegalArgumentException("Automata particle "+particle.getHash()+" is duplicated");
		}

		this.particles = new ArrayList<Particle>(particles);
		this.automata = new ArrayList<Particle>(automata);
	}

	public Collection<Particle> getAutomata()
	{
		if (this.automata == null)
			return Collections.emptyList();
		
		return Collections.unmodifiableList(this.automata);
	}


	public void clearAutomata()
	{
		if (this.automata != null)
			this.automata.clear();
	}

/*	public boolean addAutomata(final Collection<Particle> particles)
	{
		Objects.requireNonNull(particles, "Automata particles is null");
		Numbers.isZero(particles.size(), "Automata particles is empty");
		if (this.automata == null)
			this.automata = Collections.synchronizedList(new ArrayList<Particle>());

		for (Particle particle : particles)
		{
			if (this.particles.contains(particle) == true)
				throw new IllegalArgumentException("Automata particle "+particle.getHash()+" is internal atom particle");

			if (this.automata.contains(particle) == true)
				throw new IllegalArgumentException("Automata particle "+particle.getHash()+" is duplicated");
				
			this.automata.add(particle);
		}
		
		return true;
	}*/

	public boolean addAutomata(final Particle particle)
	{
		Objects.requireNonNull(particle, "Automata output particle is null");
		if (this.automata == null)
			this.automata = Collections.synchronizedList(new ArrayList<Particle>());
		
		if (this.automata.contains(particle) == true)
			throw new IllegalArgumentException("Automata output particle "+particle.getHash()+" is duplicated");
			
		return this.automata.add(particle);
	}
	
	public Collection<Particle> getParticles()
	{
		return Collections.unmodifiableList(this.particles);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getParticle(final Hash hash) 
	{
		Objects.requireNonNull(hash);
		
		for (Particle particle : this.particles)
			if (particle.getHash().equals(hash) == true)
				return (T)particle;
		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Particle> List<T> getParticles(final Class<T> type)
	{
		List<T> particles = new ArrayList<T>();
		this.particles.stream().filter(p -> type.isAssignableFrom(p.getClass())).forEach(p -> particles.add((T) p));
		return particles;
	}

	public boolean hasParticle(final Class<? extends Particle> type)
	{
		for (Particle particle : this.particles)
			if (type.isAssignableFrom(particle.getClass()))
				return true;
		
		return false;
	}
	
	public boolean hasParticle(final Hash hash) 
	{
		for (Particle particle : this.particles)
			if (particle.getHash().equals(hash) == true)
				return true;
		
		return false;
	}
	
	public Map<Hash, StateObject> getStates()
	{
		if (this.states == null)
			return Collections.emptyMap();
		
		return this.states;
	}

	public boolean addState(StateObject stateObject)
	{
		Objects.requireNonNull(stateObject, "State object for atom "+getHash()+" is null");
		if (this.states == null)
			this.states = new LinkedHashMap<Hash, StateObject>();
		
		return this.states.putIfAbsent(stateObject.getKey().get(), stateObject) == null ? true : false;
	}

	/**
	 * Returns the values of the states touched in this atom.
	 * <br><br>
	 * Beware!  If a state is not native to a shard served by a node, the value will represent the last state change that was seen and may have changed since.
	 * <br><br>
	 * Only states native to the nodes shard group will be the current committed value of that state. 
	 * <br>
	 * @param states
	 */
	// Also would really like this to be package-private but accessible from parent-package.  But Java ... meh
	public void setStates(Map<Hash, StateObject> states)
	{
		Objects.requireNonNull(states, "States for atom "+getHash()+" is null");
		this.states = new LinkedHashMap<Hash, StateObject>(states);
	}
}
