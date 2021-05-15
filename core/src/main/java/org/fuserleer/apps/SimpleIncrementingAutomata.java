package org.fuserleer.apps;

import java.io.IOException;

import org.fuserleer.crypto.Identity;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateField;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.StateOp;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.ledger.atoms.AutomataParticle;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.UInt256;

@SerializerId2("apps.simpleincrementer")
public final class SimpleIncrementingAutomata extends AutomataParticle
{
	public static final String INCREMENT = "increment";
	public static final String DECREMENT = "increment";

	@SuppressWarnings("unused")
	private SimpleIncrementingAutomata()
	{
		super();
	}
	
	public SimpleIncrementingAutomata(Identity owner)
	{
		super(Spin.UP, owner);
	}
	
	@Override
	public boolean isConsumable()
	{
		return false;
	}
	
	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException
	{
		stateMachine.sop(new StateOp(new StateField(getHash(), "value"), Instruction.GET), this);
		stateMachine.sop(new StateOp(new StateField(getHash(), "value"), Instruction.NOT_EXISTS), this);
		super.prepare(stateMachine);
	}

	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException
	{
		stateMachine.sop(new StateOp(new StateField(getHash(), "value"), UInt256.ZERO, Instruction.SET), this);
		super.execute(stateMachine);
	}

	@Override
	public void prepare(StateMachine stateMachine, String method, Object ... arguments) throws ValidationException, IOException
	{
		if (method.compareToIgnoreCase(INCREMENT) == 0)
		{
			stateMachine.sop(new StateOp(new StateField(getHash(), "value"), Instruction.GET), this);
		}
		else if (method.compareToIgnoreCase(DECREMENT) == 0)
		{
			stateMachine.sop(new StateOp(new StateField(getHash(), "value"), Instruction.GET), this);
		}
		else
			throw new ValidationException("Method "+method+" not supported");
	}

	@Override
	public void execute(StateMachine stateMachine, String method, Object ... arguments) throws ValidationException, IOException
	{
		if (method.compareToIgnoreCase(INCREMENT) == 0)
		{
			UInt256 value = stateMachine.getInput(new StateField(getHash(), "value")).get();
			value = value.increment();
			stateMachine.sop(new StateOp(new StateField(getHash(), "value"), value, Instruction.SET), this);
		}
		else if (method.compareToIgnoreCase(DECREMENT) == 0)
		{
			UInt256 value = stateMachine.getInput(new StateField(getHash(), "value")).get();
			value = value.decrement();
			stateMachine.sop(new StateOp(new StateField(getHash(), "value"), value, Instruction.SET), this);
		}
		else
			throw new ValidationException("Method "+method+" not supported");
	}
}
