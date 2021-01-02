package org.fuserleer.ledger;

import java.util.Objects;

import org.fuserleer.exceptions.ValidationException;

public final class StateOpResult<T>
{
	private final StateOp 		stateOp;
	private final T				output;
	private final CommitState 	commitState;
	private final ValidationException exception;
	
	StateOpResult(final StateOp stateOp, final ValidationException exception)
	{
		this.stateOp = Objects.requireNonNull(stateOp, "StateOp is null");
		this.exception = Objects.requireNonNull(exception, "Exception is null");
		this.output = null;
		this.commitState = null;
	}

	StateOpResult(final StateOp stateOp, final CommitState commitState, final ValidationException exception)
	{
		this.stateOp = Objects.requireNonNull(stateOp, "StateOp is null");
		this.exception = Objects.requireNonNull(exception, "Exception is null");
		this.commitState = Objects.requireNonNull(commitState, "CommitState is null");
		this.output = null;
	}

	StateOpResult(final StateOp stateOp, final CommitState commitState)
	{
		this.stateOp = Objects.requireNonNull(stateOp, "StateOp is null");
		this.commitState = Objects.requireNonNull(commitState, "CommitState is null");
		this.output = null;
		this.exception = null;
	}

	StateOpResult(final StateOp stateOp, final T output, final CommitState commitState)
	{
		this.stateOp = Objects.requireNonNull(stateOp, "StateOp is null");
		this.output = Objects.requireNonNull(output, "Output is null");
		this.commitState = Objects.requireNonNull(commitState, "CommitState is null");
		this.exception = null;
	}

	public StateOp getStateOp()
	{
		return this.stateOp;
	}

	public T getOutput() throws ValidationException
	{
		if (this.exception != null)
			thrown();
		
		return this.output;
	}

	public CommitState getCommitState() throws ValidationException
	{
		if (this.commitState == null)
			thrown();
		
		return this.commitState;
	}
	
	public ValidationException getException()
	{
		return this.exception;
	}

	public void thrown() throws ValidationException
	{
		if (this.exception != null)
			throw this.exception;
	}
}
