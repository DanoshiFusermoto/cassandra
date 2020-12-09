package org.fuserleer.ledger.atoms;

import java.util.Objects;

import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;

public class AtomNotFoundException extends ValidationException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6964432881299046502L;
	
	private final Hash atom;
	
	public AtomNotFoundException(String message, Hash atom)
	{
		super(message);

		if (atom.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Atom hash is ZERO");

		this.atom = atom;
	}

	public AtomNotFoundException(Hash atom)
	{
		this("The atom "+Objects.requireNonNull(atom)+" is not found", atom);
	}

	public Hash getAtom() 
	{
		return this.atom;
	}
}
