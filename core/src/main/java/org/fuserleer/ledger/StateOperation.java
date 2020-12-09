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
	public static enum Type 
	{
		NOP, STORE, DELETE, PRUNE;
	}
	
	private final Type 			type;
	private final BlockHeader	head;
	private final Atom			atom;
	private final long 			timestamp;
	
	private transient Set<Indexable> indexables = null;
	private transient Set<Identifier> identifiers = null;

	StateOperation(Type type, BlockHeader head, Atom atom)
	{
		this.type = Objects.requireNonNull(type);
		this.head = Objects.requireNonNull(head);
		this.atom = Objects.requireNonNull(atom);
		this.timestamp = Time.getLedgerTimeMS();
	}

	public Type getType() 
	{
		return this.type;
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
			indexables.add(Indexable.from(this.atom.getHash(), Atom.class));
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
			if (this.type.equals(((StateOperation)object).getType()) == false)
				return false;

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
		return this.head.getHash()+" "+this.atom.getHash()+" ["+getIndexables().toString()+"] ("+(getIdentifiers() == null ? "" : getIdentifiers())+")";
	}

}
