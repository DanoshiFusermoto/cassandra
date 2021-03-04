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
	
	public DependencyNotFoundException(final String message, final Hash dependent, final Hash dependency)
	{
		super(message);

		Hash.notZero(dependent, "Dependent hash is ZERO");
		Hash.notZero(dependency, "Dependency hash is ZERO");

		if (dependent.equals(dependency) == true)
			throw new IllegalArgumentException("Dependency and dependent are the same");
		
		this.dependency = dependency;
		this.dependent = dependent;
	}

	public DependencyNotFoundException(final Hash dependent, final Hash dependency)
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
