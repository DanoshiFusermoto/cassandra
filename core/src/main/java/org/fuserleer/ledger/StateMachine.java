package org.fuserleer.ledger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.fuserleer.Context;
import org.fuserleer.Universe;
import org.fuserleer.common.Primitive;
import org.fuserleer.crypto.Hash;
import org.fuserleer.database.Fields;
import org.fuserleer.database.Indexable;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;

public class StateMachine implements LedgerInterface
{
	private static final Logger ledgerLog = Logging.getLogger("ledger");

	private final Context 		context;
	private final Atom 			atom;
	private final BlockHeader	block;
	private final StateAccumulator stateAccumulator;
	
	private transient boolean prepared = false;
	private final transient Map<Object, Object> workspace = new HashMap<Object, Object>();

	StateMachine(Context context, BlockHeader block, Atom atom, StateAccumulator stateAccumulator)
	{
		this.context = Objects.requireNonNull(context);
		this.atom = Objects.requireNonNull(atom);
		this.block = Objects.requireNonNull(block);
		this.stateAccumulator = Objects.requireNonNull(stateAccumulator);
	}

	public Atom getAtom()
	{
		return this.atom;
	}

	public boolean has(Object variable)
	{
		return this.workspace.containsKey(variable);
	}

	@SuppressWarnings("unchecked")
	public <T> T get(Object variable)
	{
		return (T) this.workspace.get(variable);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T put(Object variable, Object value)
	{
		return (T) this.workspace.put(variable, value);
	}

	public void prepare() throws ValidationException, IOException
	{
		Set<Hash> internalIndexables = new HashSet<Hash>();
		for(Particle particle : this.atom.getParticles())
		{
			if (internalIndexables.add(particle.getHash()) == false)
				throw new ValidationException("Particle "+particle+" is duplicated in atom "+this.atom.getHash());
			
			for (Indexable stateIndexable : particle.getIndexables())
			{
				if (internalIndexables.add(stateIndexable.getHash()) == false)
					throw new ValidationException("Indexable "+stateIndexable+" is duplicated in "+particle+" in atom "+this.atom.getHash());
			}
			
			particle.prepare(this);

			// TODO these are off for prototyping, turn back on!
/*				if (operation instanceof SignedOperation)
				{
					try
					{
						SignedOperation signedOperation = (SignedOperation) operation;
						if (signedOperation.requiresSignature() == true && 
							signedOperation.verify(operation.getOwner()) == false)
							throw new ValidationException("Signature for "+operation+" in Action "+action.getHash()+" did not verify with public key "+operation.getOwner());
					}
					catch (CryptoException cex)
					{
						throw new ValidationException("Verification of "+operation+" in Action "+action.getHash()+" failed", cex);
					}
				}*/
		}
		
		this.prepared = true;
	}
	
/*	private boolean has(Indexable indexable, IndexableCommit indexableCommit)
	{
		Objects.requireNonNull(indexable);
		Objects.requireNonNull(indexableCommit);
		
		if (indexableCommit.equals(IndexableCommit.NULL) == true)
			return false;
		
		if (indexableCommit.equals(IndexableCommit.DELETED) == true)
			return false;

		if (indexable.getHash().equals(indexableCommit.getIndexable()) == false)
			throw new IllegalArgumentException("IndexableCommit "+indexableCommit+" does not represent "+indexable);
		
		if (indexableCommit.getID() > this.head.getHeight())
			return false;
		
		return true;
	}*/
	
	public void lock() throws IOException, ValidationException
	{
		execute(CommitState.LOCKED);
		
		this.stateAccumulator.lock(this.block, this.atom);
	}
	
	public void precommit() throws IOException, ValidationException
	{
		execute(CommitState.PRECOMMITTED);
		
		this.stateAccumulator.precommit(this.atom);
	}
	
	private void execute(CommitState state) throws ValidationException, IOException
	{
		if (this.prepared == false)
			prepare();
		
		for (Particle particle : this.atom.getParticles())
		{
			if (ledgerLog.hasLevel(Logging.DEBUG) == true)
				ledgerLog.debug(this.context.getName()+": Validating particle "+particle);
			
			if (this.stateAccumulator.state(Indexable.from(particle.getHash(), Particle.class)).index() > state.index())
				throw new ValidationException("Particle "+particle+" is already in states");
	
			if (particle.getSpin().equals(Spin.DOWN) == true &&
				this.atom.getParticle(Indexable.from(particle.getHash(Spin.UP), Particle.class)) == null && 
				this.stateAccumulator.state(Indexable.from(particle.getHash(Spin.UP), Particle.class)).equals(CommitState.COMMITTED) == false)
				throw new ValidationException("Attempting to spin DOWN particle "+particle+" without a corresponding UP "+particle.getHash(Spin.UP));

			if (particle.getSpin().equals(Spin.DOWN) == true && ledgerLog.hasLevel(Logging.DEBUG) == true)
				ledgerLog.debug(this.context.getName()+": Attempting to spin DOWN particle "+particle.getHash(Spin.UP)+" -> "+particle.getHash());
				
			for (Indexable indexable : particle.getIndexables())
				if (this.stateAccumulator.state(indexable).index() > state.index())
					throw new ValidationException("Indexable "+indexable+" within particle "+particle+" is already in state");

			if (Universe.getDefault().getGenesis().contains(particle.getHash()) == false)
				particle.execute(this);
		}
	}
	
	@Override
	public <T extends Primitive> T get(Indexable indexable) throws IOException
	{
		return this.stateAccumulator.get(indexable);
	}

	@Override
	public <T extends Primitive> T get(Indexable indexable, Class<T> container) throws IOException
	{
		return this.stateAccumulator.get(indexable, container);
	}

	@Override
	public <T extends Primitive> SearchResponse<T> get(SearchQuery query, Class<T> container, Spin spin) throws IOException
	{
		return this.stateAccumulator.get(query, container, spin);
	}

	@Override
	public CommitState state(Indexable indexable) throws IOException
	{
		return this.stateAccumulator.state(indexable);
	}

	public void set(Hash action, Fields fields)
	{
		this.stateAccumulator.set(action, fields);
	}
}
