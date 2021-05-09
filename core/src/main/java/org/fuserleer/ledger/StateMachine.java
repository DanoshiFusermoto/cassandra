package org.fuserleer.ledger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.fuserleer.Context;
import org.fuserleer.Universe;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.crypto.MerkleTree;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.CommitOperation.Type;
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.atoms.SignedParticle;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

public final class StateMachine // implements LedgerInterface
{
	private static final Logger stateMachineLog = Logging.getLogger("statemachine");
	
	static 
	{
		stateMachineLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN | Logging.DEBUG);
//		stateMachineLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
//		stateMachineLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
//		stateMachineLog.setLevels(Logging.ERROR | Logging.FATAL);
	}

	private final Context 			context;
	private final Atom 				atom;
	private final Map<Hash, Path> 	statePaths;
	private final Multimap<Hash, Path> 	associationPaths;
	private final Map<Hash, Optional<UInt256>> stateInputs;
	private final Map<Hash, Optional<UInt256>> stateOutputs;
	private final Multimap<StateKey<?, ?>, StateOp> stateOps;
	
//	private final AtomicReference<Set<StateKey<?, ?>>> stateKeys;
	private final AtomicReference<Set<UInt256>> stateShards;
	
	private final Map<String, Object> scoped;
	
	private final BiConsumer<StateOp, Path> sop;
	private final BiConsumer<Hash, Path> 	associate;
	private final ReentrantReadWriteLock 	lock = new ReentrantReadWriteLock(true);

	private BlockHeader	block;
	private Hash 		execution;
	private Exception	thrown;
	
	StateMachine(final Context context, final Atom atom)
	{
		this.context = Objects.requireNonNull(context);
		this.atom = Objects.requireNonNull(atom);
		this.statePaths = new LinkedHashMap<Hash, Path>();
		this.associationPaths = LinkedHashMultimap.create();
		this.stateOps = LinkedHashMultimap.create();
		this.stateShards = new AtomicReference<Set<UInt256>>();
		this.stateOutputs = new HashMap<Hash, Optional<UInt256>>();
		this.stateInputs = new HashMap<Hash, Optional<UInt256>>();
		this.scoped = new HashMap<String, Object>();
		
		this.sop = (s, p) -> 
		{
			this.lock.writeLock().lock();
			try
			{
				if (Universe.getDefault().getGenesis().contains(this.atom.getHash()) == false)
				{
					if (s.ins().equals(Instruction.SET) == true && this.stateOps.containsKey(s.key()) == false)
						throw new IllegalStateException("Attempt to SET a state key that doesn't exist "+s.key());
	
					if (s.ins().equals(Instruction.SET) == true && this.stateInputs.get(s.key().get()) == null)
						throw new IllegalStateException("Attempt to SET unloaded state "+s.key());
				}
				
				if (this.stateOps.containsEntry(s.key(), p) == true && s.ins().equals(Instruction.SET) == true)
					throw new IllegalStateException("StateOp "+s+":"+p+" is already delclared in atom "+this.atom.getHash());
	
				// TODO guard new stateop keys after prepare is executed!

				this.stateOps.put(s.key(), s);
				if (s.ins().equals(Instruction.SET) == true)
				{
					this.statePaths.put(s.key().get(), p);
					this.stateOutputs.put(s.key().get(), Optional.of(s.value()));
				}
			}
			finally
			{
				this.lock.writeLock().unlock();
			}
		};

		this.associate = (i, p) -> 
		{
			this.lock.writeLock().lock();
			try
			{
				if (this.associationPaths.containsEntry(i, p) == true)
					throw new IllegalStateException("Association "+i+":"+p+" is assigned in atom "+this.atom.getHash());
	
				this.associationPaths.put(i, p);
			}
			finally
			{
				this.lock.writeLock().unlock();
			}
		};
	}
	
	public Context getContext()
	{
		return this.context;
	}

	public Atom getAtom()
	{
		return this.atom;
	}
	
	public void sop(final StateOp stateOp, final Particle particle)
	{
		Path path = null;
		if (this.block != null)
			path = new Path(stateOp.key().get(), ImmutableMap.of(Elements.BLOCK, this.block.getHash(), Elements.ATOM, this.atom.getHash(), Elements.PARTICLE, particle.getHash()));
		else
			path = new Path(stateOp.key().get(), ImmutableMap.of(Elements.ATOM, this.atom.getHash(), Elements.PARTICLE, particle.getHash()));

		sop(stateOp, path);
	}
	
	public void sop(final StateOp stateOp, final Path path)
	{
		this.sop.accept(stateOp, path);
	}

	public void associate(final Hash association, final Particle particle)
	{
		Path path = null;
		if (this.block != null)
			path = new Path(association, ImmutableMap.of(Elements.BLOCK, this.block.getHash(), Elements.ATOM, this.atom.getHash(), Elements.PARTICLE, particle.getHash()));
		else
			path = new Path(association, ImmutableMap.of(Elements.ATOM, this.atom.getHash(), Elements.PARTICLE, particle.getHash()));
			
		associate(association, path);
	}

	public void associate(final Hash association, final Path path)
	{
		this.associate.accept(association, path);
	}

	boolean isProvisioned()
	{
		this.lock.readLock().lock();
		try
		{
			if (this.stateOps.isEmpty() == true)
				return false;
			
			for (StateOp stateOp : this.stateOps.values())
			{
				if (stateOp.ins().evaluatable() == false)
					continue;
				
				if (this.stateInputs.containsKey(stateOp.key().get()) == false)
					return false;
			}
			
			return true;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	@JsonProperty("shards")
	@DsonOutput(Output.API)
	Set<UInt256> getShards()
	{
		Set<UInt256> stateShards = this.stateShards.get();
		if (stateShards == null)
			return Collections.emptySet();
		
		return stateShards;
	}
	
	Set<StateOp> getStateOps()
	{
		this.lock.readLock().lock();
		try
		{
			return Collections.unmodifiableSet(new HashSet<StateOp>(this.stateOps.values()));
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	Set<StateKey<?, ?>> getStateKeys()
	{
		this.lock.readLock().lock();
		try
		{
			return Collections.unmodifiableSet(this.stateOps.keySet().stream().collect(Collectors.toSet()));
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	Set<Path> getStatePaths()
	{
		this.lock.readLock().lock();
		try
		{
			return Collections.unmodifiableSet(this.statePaths.values().stream().collect(Collectors.toSet()));
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	Set<Path> getAssociationPaths()
	{
		this.lock.readLock().lock();
		try
		{
			return Collections.unmodifiableSet(this.associationPaths.values().stream().collect(Collectors.toSet()));
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	CommitOperation getAcceptOperation()
	{
		this.lock.readLock().lock();
		try
		{
			return new CommitOperation(Type.ACCEPT, this.block, this.atom, this.stateOps.values(), this.statePaths.values(), this.associationPaths.values());
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	CommitOperation getRejectOperation()
	{
		this.lock.readLock().lock();
		try
		{
			return new CommitOperation(Type.REJECT, this.block, this.atom, Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	Hash getExecution()
	{
		return this.execution;
	}
	
	void prepare() throws IOException, ValidationException
	{
		this.lock.writeLock().lock();
		try
		{
			for (Particle particle : this.atom.getParticles())
			{
				Set<StateOp> stateOps = new LinkedHashSet<StateOp>();
				if (particle.getSpin().equals(Spin.UP) == true)
				{
					stateOps.add(new StateOp(new StateAddress(Particle.class, particle.getHash(Spin.UP)), Instruction.GET));
					stateOps.add(new StateOp(new StateAddress(Particle.class, particle.getHash(Spin.UP)), Instruction.NOT_EXISTS));
					stateOps.add(new StateOp(new StateAddress(Particle.class, particle.getHash(Spin.UP)), UInt256.from(particle.getHash(Spin.UP).toByteArray()), Instruction.SET));
				}
				else if (particle.getSpin().equals(Spin.DOWN) == true)
				{
					stateOps.add(new StateOp(new StateAddress(Particle.class, particle.getHash(Spin.UP)), Instruction.EXISTS));
					stateOps.add(new StateOp(new StateAddress(Particle.class, particle.getHash(Spin.DOWN)), Instruction.GET));
					stateOps.add(new StateOp(new StateAddress(Particle.class, particle.getHash(Spin.DOWN)), Instruction.NOT_EXISTS));
					stateOps.add(new StateOp(new StateAddress(Particle.class, particle.getHash(Spin.DOWN)), UInt256.from(particle.getHash(Spin.DOWN).toByteArray()), Instruction.SET));
				}
				
				for (StateOp stateOp : stateOps)
				{
					if (this.stateOps.containsEntry(stateOp.key(), stateOp) == true)
						throw new ValidationException("StateOp "+stateOp+" is duplicated in "+particle+" in atom "+this.atom.getHash());
					
					this.stateOps.put(stateOp.key(), stateOp);
					
					if (stateOp.ins().equals(Instruction.SET) == true)
					{
						Path path = null;
						if (this.block != null)
							path = new Path(stateOp.key().get(), ImmutableMap.of(Elements.BLOCK, this.block.getHash(), Elements.ATOM, this.atom.getHash(), Elements.PARTICLE, particle.getHash()));
						else
							path = new Path(stateOp.key().get(), ImmutableMap.of(Elements.ATOM, this.atom.getHash(), Elements.PARTICLE, particle.getHash()));
						this.statePaths.put(stateOp.key().get(), path);
						this.stateOutputs.put(stateOp.key().get(), Optional.of(stateOp.value()));
					}
				}

				this.statePaths.put(particle.getHash(), new Path(particle.getHash(), ImmutableMap.of(Elements.ATOM, this.atom.getHash(), Elements.PARTICLE, particle.getHash())));

				particle.prepare(this);
				
				if (particle instanceof SignedParticle && ((SignedParticle)particle).requiresSignature() == true)
				{
					try
					{
						SignedParticle signedParticle = (SignedParticle) particle;
						if (signedParticle.verify(signedParticle.getOwner()) == false)
							throw new ValidationException("Signature for "+particle.toString()+" in atom "+atom.getHash()+" did not verify with public key "+signedParticle.getOwner());
					}
					catch (CryptoException cex)
					{
						throw new ValidationException("Preparation of "+particle.toString()+" in atom "+atom.getHash()+" failed", cex);
					}
				}
			}
			
			Map<Hash, StateObject> statesForAtom = new HashMap<Hash, StateObject>();
			for (StateOp stateOp : this.stateOps.values())
			{
				if (stateOp.ins().equals(Instruction.SET) == true)
					statesForAtom.put(stateOp.key().get(), new StateObject(stateOp.key(), stateOp.value()));
				else
				{
					Optional<UInt256> value = this.stateInputs.get(stateOp.key().get());
					if (value != null && value.isPresent() == true)
						statesForAtom.put(stateOp.key().get(), new StateObject(stateOp.key(), value.get()));
					else
						statesForAtom.put(stateOp.key().get(), new StateObject(stateOp.key()));
				}
			}
			this.atom.setStates(statesForAtom);
			this.stateShards.set(Collections.unmodifiableSet(this.stateOps.keySet().stream().map(skey -> UInt256.from(skey.get().toByteArray())).collect(Collectors.toSet())));
		}
		catch (Exception ex)
		{
			this.thrown = ex;
			throw ex;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	Set<StateKey<?, ?>> provision(final BlockHeader block)
	{
		this.lock.writeLock().lock();
		try
		{
			Objects.requireNonNull(block, "Block is null");
			
			if (this.block != null)
				throw new IllegalStateException("Statemachine for "+atom.getHash()+" is already provisioned");
	
			this.block = block;
			
			for (Path path : getStatePaths())
				path.add(Elements.BLOCK, this.block.getHash());
	
			for (Path path : getAssociationPaths())
				path.add(Elements.BLOCK, this.block.getHash());

			return getStateKeys();
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	void provision(final StateKey<?, ?> key, final UInt256 value) throws ValidationException
	{
		this.lock.writeLock().lock();
		try
		{
			Objects.requireNonNull(key, "State key is null");
			if (this.stateInputs.containsKey(key.get()) == true)
				return;
			
			if (this.stateOps.containsKey(key) == false)
				throw new IllegalArgumentException("State key "+key+" not relevant for StateMachine "+this.atom.getHash());

			Optional<UInt256> optional;
			if (value == null)
				optional = Optional.empty();
			else
				optional = Optional.of(value);
			this.stateInputs.put(key.get(), optional);

			for (StateOp stateOp : this.stateOps.get(key))
			{
				switch(stateOp.ins())
				{
					case EXISTS:
						if (optional.isPresent() == false)
							throw new ValidationException("Evaluation of "+stateOp+" against "+value+" failed");
						break;
					case NOT_EXISTS:
						if (optional.isPresent() == true)
							throw new ValidationException("Evaluation of "+stateOp+" against "+value+" failed");
						break;
					default:
						break;
				}
			}
		}
		catch (Exception ex)
		{
			this.thrown = ex;

			// TODO not sure if output should be set here or in a post execution handler that looks at the thrown exception type.  
			// 		Depends if a use case is found where setting it here isn't the ideal 
			this.execution = Hash.ZERO;
			throw ex;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	void execute() throws ValidationException, IOException
	{
		this.lock.writeLock().lock();
		try
		{
			if (this.block.getHash().equals(Universe.getDefault().getGenesis().getHash()) == false)
			{
				for (StateOp stateOp : this.stateOps.values())
				{
					if (stateOp.ins().evaluatable() == false)
						continue;
					
					if (stateMachineLog.hasLevel(Logging.DEBUG) == true)
						stateMachineLog.debug(this.context.getName()+": Evaluating state "+stateOp);
					
					if (this.stateInputs.containsKey(stateOp.key().get()) == false)
						throw new ValidationException("Found unprovisioned instruction "+stateOp);
		
					Optional<UInt256> value = this.stateInputs.get(stateOp.key().get());
					switch(stateOp.ins())
					{
					case GET:
					case SET:
						if (value == null)
							throw new ValidationException("Evaluation of "+stateOp+" failed as value was not provisioned");
						break;
					// Evaluated in provisioning
					case EXISTS:
					case NOT_EXISTS:
						break;
					default:
						throw new ValidationException("Unknown instruction "+stateOp.ins());
					}
				}
			}
			
			for (Particle particle : this.atom.getParticles())
			{
				if (stateMachineLog.hasLevel(Logging.DEBUG) == true)
					stateMachineLog.debug(this.context.getName()+": Executing particle "+particle);
	
				particle.execute(this);
			}
			
			// Create an output merkle
			// TODO this isn't smart right now, just dumps out all the states and values if applicable
			MerkleTree executionMerkle = new MerkleTree();
			for (StateOp stateOp : this.stateOps.values())
			{
				Hash executionLeaf = stateOp.key().get();
				executionLeaf = new Hash(executionLeaf, new Hash(stateOp.ins().name().getBytes(StandardCharsets.UTF_8), Mode.STANDARD), Mode.STANDARD);
				
				if (stateOp.ins().output() == true)
					executionLeaf = new Hash(executionLeaf, new Hash(stateOp.value().toByteArray()), Mode.STANDARD);
				
				Optional<UInt256> stateInput = this.stateInputs.get(stateOp.key().get());
				if (stateInput != null && stateInput.isPresent() == true)
					executionLeaf = new Hash(executionLeaf, new Hash(stateInput.get().toByteArray()), Mode.STANDARD);
				
				Optional<UInt256> stateOutput = this.stateOutputs.get(stateOp.key().get());
				if (stateOutput != null && stateOutput.isPresent() == true)
					executionLeaf = new Hash(executionLeaf, new Hash(stateOutput.get().toByteArray()), Mode.STANDARD);

				executionMerkle.appendLeaf(executionLeaf);
			}

			this.execution = executionMerkle.buildTree();
		}
		catch (Exception ex)
		{
			this.thrown = ex;

			// TODO not sure if output should be set here or in a post execution handler that looks at the thrown exception type.  
			// 		Depends if a use case is found where setting it here isn't the ideal 
			this.execution = Hash.ZERO;
			
			throw ex;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	Exception thrown()
	{
		return this.thrown;
	}
	
	public Optional<UInt256> getInput(final StateKey<?, ?> stateKey)
	{
		Objects.requireNonNull(stateKey, "State key is null");
		
		this.lock.readLock().lock();
		try
		{
			Optional<UInt256> value = this.stateInputs.get(stateKey.get());
			return value;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public Optional<UInt256> getOutput(final StateKey<?, ?> stateKey)
	{
		this.lock.readLock().lock();
		try
		{
			Optional<UInt256> value = this.stateOutputs.get(Objects.requireNonNull(stateKey, "State key is null").get());
			return value;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public final <T> T get(final String key)
	{
		this.lock.readLock().lock();
		try
		{
			return (T) this.scoped.get(Objects.requireNonNull(key, "Key is null"));
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public final <T> void set(final String key, T value)
	{
		this.lock.writeLock().lock();
		try
		{
			this.scoped.put(Objects.requireNonNull(key, "Key is null"), Objects.requireNonNull(value, "Value is null"));
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
}
