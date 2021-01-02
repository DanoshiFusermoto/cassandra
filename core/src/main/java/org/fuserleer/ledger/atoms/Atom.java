package org.fuserleer.ledger.atoms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.fuserleer.BasicObject;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.Identifier;
import org.fuserleer.database.Indexable;
import org.fuserleer.database.IndexablePrimitive;
import org.fuserleer.ledger.StateOp;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atom")
public final class Atom extends BasicObject implements IndexablePrimitive // TODO not sure if this needs to be state primitive as its really an envelope for particle state primitives
{
	@JsonProperty("particles")
	@DsonOutput(Output.ALL)
	private List<Particle> particles;
	
	private transient Set<StateOp> stateops = null;
	private transient Set<Indexable> indexables = null;

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
		
		if (Objects.requireNonNull(particles).isEmpty() == true)
			throw new IllegalArgumentException("Particles is empty");
		
		Set<Particle> verifiedNonDuplicates = new LinkedHashSet<Particle>();
		for (Particle particle : particles)
		{
			if (verifiedNonDuplicates.add(particle) == false)
				throw new IllegalArgumentException("Particle "+particle.getHash()+" is duplicated");
		}
		
		this.particles = new ArrayList<Particle>(verifiedNonDuplicates);
	}
	
	public Set<Hash> getStates()
	{
		Set<Hash> states = new LinkedHashSet<Hash>();
		this.particles.forEach(p -> states.addAll(p.getStates()));
		return states;
	}

	public synchronized Set<StateOp> getStateOps()
	{
		if (this.stateops == null)
		{
			final Set<StateOp> stateops = new LinkedHashSet<StateOp>();
			this.particles.forEach(p -> stateops.addAll(p.getStateOps()));
			this.stateops = Collections.unmodifiableSet(stateops);
		}
		
		return this.stateops;
	}

	public synchronized Set<Indexable> getIndexables()
	{
		if (this.indexables == null)
		{
			Set<Indexable> indexables = new HashSet<Indexable>();
			indexables.addAll(this.particles.stream().map(p -> Indexable.from(p.getHash(), Particle.class)).collect(Collectors.toList()));
			indexables.addAll(this.particles.stream().map(p -> Indexable.from(p.getHash(), p.getClass())).collect(Collectors.toList()));
			this.particles.stream().forEach(p -> indexables.addAll(p.getIndexables()));
			this.indexables = Collections.unmodifiableSet(indexables);
		}
		
		return this.indexables;
	}

	public Set<Identifier> getIdentifiers()
	{
		Set<Identifier> identifiers = new HashSet<Identifier>();
		this.particles.stream().forEach(p -> identifiers.addAll(p.getIdentifiers()));
		return identifiers;
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
	public <T> T getParticle(Indexable indexable) 
	{
		Objects.requireNonNull(indexable);
		
		for (Particle particle : this.particles)
			if (particle.getHash().equals(indexable.getKey()) == true || particle.hasIndexable(indexable) == true)
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
	
	@JsonProperty("shards")
	@DsonOutput(Output.API)
	public Set<UInt256> getShards()
	{
		Set<UInt256> shards = new HashSet<UInt256>();
		for (Particle particle : this.particles)
			shards.addAll(particle.getShards());
		return shards;
	}
}
