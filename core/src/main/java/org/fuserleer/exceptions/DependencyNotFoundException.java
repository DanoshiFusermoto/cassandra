package org.fuserleer.exceptions;

import java.util.Objects;

import org.fuserleer.crypto.Hash;

public class DependencyNotFoundException extends ValidationException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 110356083142939115L;
	
	private final Hash dependency;
	private final Hash dependent;
	
	public DependencyNotFoundException(String message, Hash dependent, Hash dependency)
	{
		super(message);

		if (dependent.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Dependent hash is ZERO");

		if (dependency.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Dependency hash is ZERO");

		if (dependent.equals(dependency) == true)
			throw new IllegalArgumentException("Dependency and dependent are the same");
		
		this.dependency = dependency;
		this.dependent = dependent;
	}

	public DependencyNotFoundException(Hash dependent, Hash dependency)
	{
		this("The depdendency "+Objects.requireNonNull(dependency)+" is not found for dependent "+Objects.requireNonNull(dependent), dependent, dependency);
	}

	public Hash getDependency() 
	{
		return this.dependency;
	}

	public Hash getDependent() 
	{
		return this.dependent;
	}
}
