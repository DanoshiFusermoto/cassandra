package org.fuserleer.ledger;

import java.io.IOException;

import org.fuserleer.exceptions.ValidationException;

public interface StateAutomata
{
	public void prepare(final StateMachine stateMachine, final String method, final Object ... arguments) throws ValidationException, IOException;
	public void execute(final StateMachine stateMachine, final String method, final Object ... arguments) throws ValidationException, IOException;
}
