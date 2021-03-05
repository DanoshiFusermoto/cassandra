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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.atom")
public final class Atom extends BasicObject implements Primitive // TODO not sure if this needs to be state primitive as its really an envelope for particle state primitives
{
	@JsonProperty("particles")
	@DsonOutput(Output.ALL)
	private List<Particle> particles;
	
	@JsonProperty("states")
	@DsonOutput(value = {Output.API, Output.PERSIST})
	@JsonDeserialize(as=LinkedHashMap.class)
	private Map<Hash, StateObject> states;

	public Atom()
	{
		super();
	}

	public Atom(Particle particle)
	{
		this();
		
		this.particles = new ArrayList<Particle>();
		this.particles.add(Objects.requireNonNull(particle));
	}

	public Atom(Particle ... particles)
	{
		this();
		
		Objects.requireNonNull(particles, "Particles is null");
		if (particles.length == 0)
			throw new IllegalArgumentException("Particles is empty");
		
		Set<Particle> verifiedNonDuplicates = new LinkedHashSet<Particle>();
		for (int p = 0 ; p < Objects.requireNonNull(particles).length ; p++)
		{
			if (verifiedNonDuplicates.add(particles[p]) == false)
				throw new IllegalArgumentException("Particle "+particles[p].getHash()+" is duplicated");
		}

		this.particles = new ArrayList<Particle>(verifiedNonDuplicates);
	}

	public Atom(Collection<? extends Particle> particles)
	{
		this();
		
		Objects.requireNonNull(particles, "Particles is null");
		if (particles.isEmpty() == true)
			throw new IllegalArgumentException("Particles is empty");
		
		Set<Particle> verifiedNonDuplicates = new LinkedHashSet<Particle>();
		for (Particle particle : particles)
		{
			if (verifiedNonDuplicates.add(particle) == false)
				throw new IllegalArgumentException("Particle "+particle.getHash()+" is duplicated");
		}
		
		this.particles = new ArrayList<Particle>(verifiedNonDuplicates);
	}
	
	public Collection<Particle> getParticles()
	{
		return Collections.unmodifiableList(this.particles);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getParticle(Hash hash) 
	{
		Objects.requireNonNull(hash);
		
		for (Particle particle : this.particles)
			if (particle.getHash().equals(hash) == true)
				return (T)particle;
		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Particle> List<T> getParticles(Class<T> type)
	{
		List<T> particles = new ArrayList<T>();
		this.particles.stream().filter(p -> type.isAssignableFrom(p.getClass())).forEach(p -> particles.add((T) p));
		return particles;
	}

	public boolean hasParticle(Class<? extends Particle> type)
	{
		for (Particle particle : this.particles)
			if (type.isAssignableFrom(particle.getClass()))
				return true;
		
		return false;
	}
	
	public boolean hasParticle(Hash hash) 
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
