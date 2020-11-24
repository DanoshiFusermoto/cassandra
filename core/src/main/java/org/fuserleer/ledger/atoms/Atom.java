package org.fuserleer.ledger.atoms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.BasicObject;
import org.fuserleer.common.Primitive;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.SerializerId2;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atom")
public final class Atom extends BasicObject implements Primitive // StatePrimitive TODO not sure if this needs to be state primitive as its really an envelope for particle state primitives
{
	@JsonProperty("particles")
	@DsonOutput(Output.ALL)
	private List<Particle> particles;

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
}
