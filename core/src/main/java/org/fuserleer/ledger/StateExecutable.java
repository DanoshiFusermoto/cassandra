package org.fuserleer.ledger;

import java.io.IOException;

import org.fuserleer.exceptions.ValidationException;

public interface StateExecutable
{
	public void prepare(final StateMachine stateMachine, final Object ... arguments) throws ValidationException, IOException;
	public void execute(final StateMachine stateMachine, final Object ... arguments) throws ValidationException, IOException;
}
