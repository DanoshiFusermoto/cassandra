package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.crypto.Hash;

public class StateLockedException extends Exception
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 110356083142939115L;
	
	private final StateKey<?, ?> state;
	private final Hash dependent;
	
	public StateLockedException(String message, StateKey<?, ?> state, Hash dependent)
	{
		super(message);

		Objects.requireNonNull(message, "Message is null");
		Objects.requireNonNull(state, "State key is null");
		Objects.requireNonNull(dependent, "Dependent is null");
		Hash.notZero(dependent, "Dependent hash is ZERO");

		if (dependent.equals(state.get()) == true)
			throw new IllegalArgumentException("State and dependent are the same");
		
		this.state = state;
		this.dependent = dependent;
	}

	public StateLockedException(StateKey<?, ?> state, Hash dependent)
	{
		this("State "+state+" required by "+Objects.requireNonNull(dependent)+" is locked", state, dependent);
	}

	public StateKey<?, ?> getState() 
	{
		return this.state;
	}

	public Hash getDependent() 
	{
		return this.dependent;
	}
}
