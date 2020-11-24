package org.fuserleer.ledger;

import java.io.IOException;

import org.fuserleer.exceptions.ValidationException;

public interface StateExecutor
{
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException;
	public boolean isPrepared();
	public void execute(StateMachine stateMachine) throws ValidationException, IOException;
	public boolean isExecuted();
	public void unexecute(StateMachine stateMachine) throws ValidationException, IOException;
	public boolean isUnexecuted();
}
