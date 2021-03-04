package org.fuserleer.ledger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.time.Time;

public class CommitOperation
{
	public enum Type
	{
		ACCEPT, REJECT;
	}
	
	private final Type				type;
	private final long 				timestamp;
	private final BlockHeader		head;
	private final Atom				atom;
	private final List<StateOp> 	stateOps;
	private final List<Path> 		statePaths;
	private final List<Path> 		associationPaths;
	
	CommitOperation(Type type, BlockHeader head, Atom atom, Collection<StateOp> stateOps, Collection<Path> statePaths, Collection<Path> associationPaths)
	{
		this.type = Objects.requireNonNull(type, "Type is null");
		this.head = Objects.requireNonNull(head, "Head is null");
		this.atom = Objects.requireNonNull(atom, "Atom is null");
		this.timestamp = Time.getLedgerTimeMS();

		if (this.type.equals(Type.REJECT) == true)
		{
			if (Objects.requireNonNull(stateOps, "State operations is null").isEmpty() == false)
				throw new IllegalArgumentException("Rejections must have not have state operations for atom "+atom.getHash());
			if (Objects.requireNonNull(stateOps, "State paths is null").isEmpty() == false)
				throw new IllegalArgumentException("Rejections must have not have state paths for atom "+atom.getHash());
			if (Objects.requireNonNull(associationPaths, "Associations is null").isEmpty() == false)
				throw new IllegalArgumentException("Rejections must have not have association paths for atom "+atom.getHash());

			this.stateOps = Collections.emptyList();
			this.statePaths = Collections.emptyList();
			this.associationPaths = Collections.emptyList();
		}
		else
		{
			if (Objects.requireNonNull(stateOps, "State operations is null").isEmpty())
				throw new IllegalArgumentException("State operations is empty for atom "+atom.getHash());
		
			if (Objects.requireNonNull(stateOps, "State paths is null").isEmpty())
				throw new IllegalArgumentException("State paths is empty for atom "+atom.getHash());

			Objects.requireNonNull(associationPaths, "Associations is null");

			this.stateOps = Collections.unmodifiableList(new ArrayList<StateOp>(stateOps));
			this.statePaths = Collections.unmodifiableList(new ArrayList<Path>(statePaths));
			this.associationPaths = Collections.unmodifiableList(new ArrayList<Path>(associationPaths));

			for (Path path : this.statePaths)
				path.validate();
			for (Path path : this.associationPaths)
				path.validate();
		}
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
	
	public Collection<StateOp> getStateOps() 
	{
		return this.stateOps;
	}
	
	public Collection<Path> getStatePaths() 
	{
		return this.statePaths;
	}

	public Collection<Path> getAssociationPaths() 
	{
		return this.associationPaths;
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
		
		if (object instanceof CommitOperation)
		{
			if (this.head.equals(((CommitOperation)object).getHead()) == false)
				return false;
			
			if (this.atom.equals(((CommitOperation)object).getAtom()) == false)
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.head.getHash()+" "+this.atom.getHash()+" ["+getStatePaths().toString()+"] ("+(getAssociationPaths().toString())+")";
	}
}
