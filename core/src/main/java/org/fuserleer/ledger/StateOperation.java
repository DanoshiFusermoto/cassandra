package org.fuserleer.ledger;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Objects;

import org.fuserleer.database.Fields;
import org.fuserleer.database.Identifier;
import org.fuserleer.database.Indexable;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.time.Time;

public class StateOperation
{
	private final BlockHeader	head;
	private final Atom			atom;
	private final long 			timestamp;
	
	private transient CommitState		state;
	private transient Set<Indexable> 	indexables = null;
	private transient Set<Identifier> 	identifiers = null;

	StateOperation(BlockHeader head, Atom atom, CommitState state)
	{
		this.head = Objects.requireNonNull(head);
		this.atom = Objects.requireNonNull(atom);
		this.timestamp = Time.getLedgerTimeMS();
		this.state = Objects.requireNonNull(state);
	}

	public CommitState getState() 
	{
		return this.state;
	}

	void setState(CommitState state) 
	{
		this.state = Objects.requireNonNull(state);
	}

	public long getTimestamp() 
	{
		return this.timestamp;
	}

	public BlockHeader getHead() 
	{
		return this.head;
	}

	public Atom getAtom() 
	{
		return this.atom;
	}

	public synchronized Set<Indexable> getIndexables() 
	{
		if (this.indexables == null)
		{
			Set<Indexable> indexables = new HashSet<Indexable>(this.atom.getIndexables()); 
			this.indexables = Collections.unmodifiableSet(indexables);
		}
		
		return this.indexables;
	}

	public Set<Identifier> getIdentifiers() 
	{
		if (this.identifiers == null)
		{
			Set<Identifier> identifiers = new HashSet<Identifier>(this.atom.getIdentifiers()); 
			identifiers.add(Identifier.from(this.head.getHash()));
			this.identifiers = Collections.unmodifiableSet(identifiers);
		}
		
		return this.identifiers;
	}

	public Fields getFields() 
	{
		return this.atom.getFields();
	}

	@Override
	public int hashCode() 
	{
		return this.head.hashCode();
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof StateOperation)
		{
			if (this.head.equals(((StateOperation)object).getHead()) == false)
				return false;
			
			if (this.atom.equals(((StateOperation)object).getAtom()) == false)
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.state+" "+this.head.getHash()+" "+this.atom.getHash()+" ["+getIndexables().toString()+"] ("+(getIdentifiers() == null ? "" : getIdentifiers())+")";
	}

}
