package org.fuserleer.ledger;

import java.io.IOException;

import org.fuserleer.exceptions.ValidationException;

public interface StateInstruction
{
	public void prepare(final StateMachine stateMachine) throws ValidationException, IOException;
	public void execute(final StateMachine stateMachine) throws ValidationException, IOException;
}
