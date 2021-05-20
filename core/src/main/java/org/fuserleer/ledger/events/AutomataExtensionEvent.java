package org.fuserleer.ledger.events;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.ledger.PendingAtom;
import org.fuserleer.ledger.StateOp;
import org.fuserleer.ledger.atoms.AutomataExtension;
import org.fuserleer.utils.Numbers;

public final class AutomataExtensionEvent extends AtomEvent
{
	private final AutomataExtension automataExt;
	private final Set<StateOp> stateOps;
	
	public AutomataExtensionEvent(final PendingAtom pendingAtom, final Collection<StateOp> stateOps, final AutomataExtension automataExt)
	{
		super(pendingAtom);
		Objects.requireNonNull(automataExt, "Automata extension is null");
		Objects.requireNonNull(stateOps, "State ops for automata extension is null");
		Numbers.isZero(stateOps.size(), "State ops for automata extension is empty");
		
		this.automataExt = automataExt;
		this.stateOps = Collections.unmodifiableSet(new HashSet<StateOp>(stateOps));
	}

	public AutomataExtension getAutomataExtension()
	{
		return this.automataExt;
	}

	public Set<StateOp> getStateOps()
	{
		return this.stateOps;
	}
}
