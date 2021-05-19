package org.fuserleer.ledger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.fuserleer.BasicObject;
import org.fuserleer.Context;
import org.fuserleer.Universe;
import org.fuserleer.crypto.CryptoException;
import org.fuserleer.crypto.Hash;
import org.fuserleer.crypto.Hash.Mode;
import org.fuserleer.crypto.Identity;
import org.fuserleer.crypto.MerkleTree;
import org.fuserleer.crypto.PublicKey;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.CommitOperation.Type;
import org.fuserleer.ledger.Path.Elements;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.ledger.atoms.AutomataExtension;
import org.fuserleer.ledger.atoms.OwnedParticle;
import org.fuserleer.ledger.atoms.Particle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.atoms.SignedParticle;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.Longs;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.sleepycat.je.OperationStatus;

public final class StateMachine // implements LedgerInterface
{
	private static final Logger stateMachineLog = Logging.getLogger("statemachine");
	
	static 
	{
//		stateMachineLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN | Logging.DEBUG);
//		stateMachineLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
//		stateMachineLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.WARN);
//		stateMachineLog.setLevels(Logging.ERROR | Logging.FATAL);
	}

	private final Context 			context;
	private final PendingAtom 		pendingAtom;
	
	private final Map<Hash, Path> 	statePaths;
	private final Multimap<Hash, Path> 	associationPaths;
	private final Map<Hash, Optional<UInt256>> stateInputs;
	private final Map<Hash, Optional<UInt256>> stateOutputs;
	private final Multimap<StateKey<?, ?>, StateOp> stateOps;
	
	private final Map<String, Object> scoped;
	
	private final BiConsumer<Hash, Path> 	associate;
	private final BiConsumer<StateOp, Path> sop;
	private final BiConsumer<Identity, Particle> 	aop;

	private final ReentrantReadWriteLock 	lock = new ReentrantReadWriteLock(true);
	private final Set<Particle> pendingExecutions;

	private BlockHeader	block;
	private Hash 		execution;
	private Exception	thrown;
	
	StateMachine(final Context context, final PendingAtom pendingAtom)
	{
		this.context = Objects.requireNonNull(context);
		this.pendingAtom = Objects.requireNonNull(pendingAtom);
		this.statePaths = new LinkedHashMap<Hash, Path>();
		this.associationPaths = LinkedHashMultimap.create();
		this.stateOps = LinkedHashMultimap.create();
		this.stateOutputs = new HashMap<Hash, Optional<UInt256>>();
		this.stateInputs = new HashMap<Hash, Optional<UInt256>>();
		this.scoped = new HashMap<String, Object>();
		this.pendingExecutions = new HashSet<Particle>();
		
		this.sop = (s, p) -> 
		{
			this.lock.writeLock().lock();
			try
			{
				if (Universe.getDefault().getGenesis().contains(this.pendingAtom.getHash()) == false)
				{
					if (s.ins().equals(Instruction.SET) == true && this.stateOps.containsKey(s.key()) == false)
						throw new IllegalStateException("Attempt to SET a state key that doesn't exist "+s.key());
	
					if (s.ins().equals(Instruction.SET) == true && this.stateInputs.get(s.key().get()) == null)
						throw new IllegalStateException("Attempt to SET unloaded state "+s.key());
				}
				
				if (this.stateOps.containsEntry(s.key(), p) == true && s.ins().equals(Instruction.SET) == true)
					throw new IllegalStateException("StateOp "+s+":"+p+" is already delclared in atom "+this.pendingAtom.getHash());
	
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

		this.aop = (i, p) -> 
		{
			this.lock.writeLock().lock();
			// TODO make this better
			try
			{
				Field nonceField = Particle.class.getDeclaredField("nonce");
				nonceField.setAccessible(true);
				nonceField.setLong(p, this.pendingAtom.getHash().asLong());
				
				Field hashField = BasicObject.class.getDeclaredField("hash");
				hashField.setAccessible(true);
				hashField.set(p, Hash.ZERO);

				// Came from an automata extension so should just be able to execute
				// TODO this will break in chained automata
				if (this.pendingAtom.getAutomataExtension() != null)
				{
					this.pendingAtom.getAtom().addAutomata(p);
					execute(p);
				}
				else
				{
					prepare(p, true);

					List<StateInput> stateInputs = new ArrayList<StateInput>();
					this.pendingAtom.getAtom().addAutomata(p);
	
					Set<Long> awareGroups = new HashSet<Long>();
					Set<Long> rebroadcastGroups = new HashSet<Long>();
					long numShardGroups = this.context.getLedger().numShardGroups(this.block.getHeight());
					long localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
					for (StateKey<?, ?> stateKey : getStateKeys())
					{
						long provisionShardGroup = ShardMapper.toShardGroup(stateKey.get(), numShardGroups);
						Optional<UInt256> value = getInput(stateKey);
						if (value != null)
						{
							stateInputs.add(new StateInput(this.pendingAtom.getHash(), stateKey, value.orElse(null)));
							awareGroups.add(provisionShardGroup);
							continue;
						}
						
						pendingAtom.setStatus(CommitStatus.AUTOMATA);
						this.context.getLedger().getStateAccumulator().lock(pendingAtom, this.stateOps.get(stateKey));
						this.context.getLedger().getStateHandler().provision(pendingAtom, stateKey);
						
						if (provisionShardGroup == localShardGroup)
							continue;
						
						rebroadcastGroups.add(provisionShardGroup);
					}
	
					rebroadcastGroups.removeAll(awareGroups);
					if (rebroadcastGroups.isEmpty() == false)
					{
						AutomataExtension ext = new AutomataExtension(pendingAtom.getAtom(), stateInputs, pendingAtom.getShards(), pendingAtom.votes());
						OperationStatus status = this.context.getLedger().getLedgerStore().store(Longs.fromByteArray(pendingAtom.getBlock().toByteArray()), ext);
						if (status.equals(OperationStatus.SUCCESS))
							this.context.getNetwork().getGossipHandler().broadcast(ext, rebroadcastGroups);
						else
							stateMachineLog.error(this.context.getName()+": Error producing automata extension for atom "+pendingAtom.getHash()+" due to "+status);
					}
				}
			}
			catch (Exception ex)
			{
				throw new IllegalStateException("Automata particle processing failed", ex);
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
					throw new IllegalStateException("Association "+i+":"+p+" is assigned in atom "+this.pendingAtom.getHash());
	
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

	public PendingAtom getPendingAtom()
	{
		return this.pendingAtom;
	}
	
	public void aop(final Identity automata, final Particle particle)
	{
		this.aop.accept(automata, particle);
	}

	public void sop(final StateOp stateOp, final Particle particle)
	{
		Path path = null;
		if (this.block != null)
			path = new Path(stateOp.key().get(), ImmutableMap.of(Elements.BLOCK, this.block.getHash(), Elements.ATOM, this.pendingAtom.getHash(), Elements.PARTICLE, particle.getHash()));
		else
			path = new Path(stateOp.key().get(), ImmutableMap.of(Elements.ATOM, this.pendingAtom.getHash(), Elements.PARTICLE, particle.getHash()));

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
			path = new Path(association, ImmutableMap.of(Elements.BLOCK, this.block.getHash(), Elements.ATOM, this.pendingAtom.getHash(), Elements.PARTICLE, particle.getHash()));
		else
			path = new Path(association, ImmutableMap.of(Elements.ATOM, this.pendingAtom.getHash(), Elements.PARTICLE, particle.getHash()));
			
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
		this.lock.readLock().lock();
		try
		{
			return Collections.unmodifiableSet(this.stateOps.keySet().stream().map(skey -> UInt256.from(skey.get().toByteArray())).collect(Collectors.toSet()));
		}
		finally
		{
			this.lock.readLock().unlock();
		}
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
			return new CommitOperation(Type.ACCEPT, this.block, this.pendingAtom.getAtom(), this.stateOps.values(), this.statePaths.values(), this.associationPaths.values());
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
			return new CommitOperation(Type.REJECT, this.block, this.pendingAtom.getAtom(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
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
			for (Particle particle : this.pendingAtom.getAtom().getParticles())
				prepare(particle, false);
			
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
			this.pendingAtom.getAtom().setStates(statesForAtom);
//			this.stateShards.set(Collections.unmodifiableSet(this.stateOps.keySet().stream().map(skey -> UInt256.from(skey.get().toByteArray())).collect(Collectors.toSet())));
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
	
	void prepare(Particle particle, boolean isAutomata) throws ValidationException, IOException
	{
		// Illegal to reference automata state or declare instructions against it in atoms
		if (isAutomata == false && particle instanceof OwnedParticle)
		{
			OwnedParticle ownedParticle = (OwnedParticle)particle;
			if (ownedParticle.getOwner().getPrefix() == Identity.COMPUTE)
				throw new ValidationException("Atom references automata state in particle "+particle.getHash()+" owned by "+ownedParticle.getOwner());
		}
		
		Set<StateOp> stateOps = new LinkedHashSet<StateOp>();
		if (particle.isEphemeral() == false)
		{
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
					throw new ValidationException("StateOp "+stateOp+" is duplicated in "+particle+" in atom "+this.pendingAtom.getHash());
				
				this.stateOps.put(stateOp.key(), stateOp);
				
				if (stateOp.ins().equals(Instruction.SET) == true)
				{
					Path path = null;
					if (this.block != null)
						path = new Path(stateOp.key().get(), ImmutableMap.of(Elements.BLOCK, this.block.getHash(), Elements.ATOM, this.pendingAtom.getHash(), Elements.PARTICLE, particle.getHash()));
					else
						path = new Path(stateOp.key().get(), ImmutableMap.of(Elements.ATOM, this.pendingAtom.getHash(), Elements.PARTICLE, particle.getHash()));
					this.statePaths.put(stateOp.key().get(), path);
					this.stateOutputs.put(stateOp.key().get(), Optional.of(stateOp.value()));
				}
			}

			if (this.block != null)
				this.statePaths.put(particle.getHash(), new Path(particle.getHash(), ImmutableMap.of(Elements.BLOCK, this.block.getHash(), Elements.ATOM, this.pendingAtom.getHash(), Elements.PARTICLE, particle.getHash())));
			else
				this.statePaths.put(particle.getHash(), new Path(particle.getHash(), ImmutableMap.of(Elements.ATOM, this.pendingAtom.getHash(), Elements.PARTICLE, particle.getHash())));
		}

		particle.prepare(this);
		
		if (particle instanceof SignedParticle && ((SignedParticle)particle).requiresSignature() == true && ((SignedParticle)particle).getOwner().getPrefix() != Identity.COMPUTE)
		{
			try
			{
				SignedParticle signedParticle = (SignedParticle) particle;
				if (signedParticle.getOwner().canVerify() == false)
					throw new ValidationException("Owner identity "+signedParticle.getOwner()+" is not a public key Signature for "+particle.toString()+" in atom "+pendingAtom.getHash());
				
				if (signedParticle.verify((PublicKey)signedParticle.getOwner().getKey()) == false)
					throw new ValidationException("Signature for "+particle.toString()+" in atom "+pendingAtom.getHash()+" did not verify with public key "+signedParticle.getOwner());
			}
			catch (CryptoException cex)
			{
				throw new ValidationException("Preparation of "+particle.toString()+" in atom "+pendingAtom.getHash()+" failed", cex);
			}
		}
		
		for (StateOp stateOp : stateOps)
		{
			if (stateOp.ins().equals(Instruction.SET) == true)
				this.pendingAtom.getAtom().addState(new StateObject(stateOp.key(), stateOp.value()));
			else
			{
				Optional<UInt256> value = this.stateInputs.get(stateOp.key().get());
				if (value != null && value.isPresent() == true)
					this.pendingAtom.getAtom().addState(new StateObject(stateOp.key(), value.get()));
				else
					this.pendingAtom.getAtom().addState(new StateObject(stateOp.key()));
			}
		}

		if (isAutomata == true)
			this.pendingExecutions.add(particle);
	}

	Set<StateKey<?, ?>> provision(final BlockHeader block)
	{
		this.lock.writeLock().lock();
		try
		{
			Objects.requireNonNull(block, "Block is null");
			
			if (this.block != null)
				throw new IllegalStateException("Statemachine for "+pendingAtom.getHash()+" is already provisioned");
	
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
	
	void provision(final StateInput stateInput) throws ValidationException, IOException
	{
		this.lock.writeLock().lock();
		try
		{
			Objects.requireNonNull(stateInput, "State input is null");
			if (this.stateInputs.containsKey(stateInput.getKey().get()) == true)
				return;
			
			if (this.stateOps.containsKey(stateInput.getKey()) == false)
				throw new IllegalArgumentException("State key "+stateInput.getKey()+" not relevant for StateMachine "+this.pendingAtom.getHash());

			Optional<UInt256> optional;
			if (stateInput.getValue() == null)
				optional = Optional.empty();
			else
				optional = Optional.of(stateInput.getValue());
			this.stateInputs.put(stateInput.getKey().get(), optional);

			for (StateOp stateOp : this.stateOps.get(stateInput.getKey()))
			{
				switch(stateOp.ins())
				{
					case EXISTS:
						if (optional.isPresent() == false)
							throw new ValidationException("Evaluation of "+stateOp+" against "+stateInput.getValue()+" failed");
						break;
					case NOT_EXISTS:
						if (optional.isPresent() == true)
							throw new ValidationException("Evaluation of "+stateOp+" against "+stateInput.getValue()+" failed");
						break;
					default:
						break;
				}
			}
			
			if (pendingAtom.getStatus().equals(CommitStatus.AUTOMATA) == true)
			{
				if (isProvisioned() == true)
				{
					// TODO split this to a worker!
					for (Particle particle : this.pendingAtom.getAtom().getAutomata())
						execute(particle);
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
			this.pendingExecutions.addAll(this.pendingAtom.getAtom().getParticles());
			for (Particle particle : this.pendingAtom.getAtom().getParticles())
				if (particle instanceof StateInstruction)
					execute(particle);
			
/*			if (this.block.getHash().equals(Universe.getDefault().getGenesis().getHash()) == false)
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
			
			for (Particle particle : this.pendingAtom.getAtom().getParticles())
			{
				if (stateMachineLog.hasLevel(Logging.DEBUG) == true)
					stateMachineLog.debug(this.context.getName()+": Executing particle "+particle);
	
				if (particle instanceof StateInstruction)
					particle.execute(this);
			}
			
			if (this.athis.pendingExecutions.isEmpty())
				buildExecutionOutput();
		}
		catch (Exception ex)
		{
			this.thrown = ex;

			// TODO not sure if output should be set here or in a post execution handler that looks at the thrown exception type.  
			// 		Depends if a use case is found where setting it here isn't the ideal 
			this.execution = Hash.ZERO;
			
			throw ex;*/
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	private void execute(Particle particle) throws ValidationException, IOException
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
			
			if (stateMachineLog.hasLevel(Logging.DEBUG) == true)
				stateMachineLog.debug(this.context.getName()+": Executing particle "+particle);
	
			if (particle instanceof StateInstruction)
			{
				try
				{
					particle.execute(this);
				}
				finally
				{
					if (this.pendingExecutions.remove(particle) == false)
						throw new IllegalStateException("Pending execution for "+particle+" remove failed in atom "+pendingAtom.getHash());
				}
			}
			
			if (this.pendingExecutions.isEmpty())
				buildExecutionOutput();
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

	private void buildExecutionOutput()
	{
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
		
		if(this.pendingAtom.getStatus().equals(CommitStatus.PROVISIONED))
			this.pendingAtom.setStatus(CommitStatus.AUTOMATA);
		this.pendingAtom.setStatus(CommitStatus.EXECUTED);
	}
	
	Exception thrown()
	{
		return this.thrown;
	}
	
	public Collection<StateInput> getInputs()
	{
		if (this.pendingAtom.getStatus().lessThan(CommitStatus.PROVISIONED))
			throw new IllegalStateException("State inputs for "+this.pendingAtom+" are not fully provisioned");
		
		this.lock.readLock().lock();
		try
		{
			List<StateInput> stateInputs = new ArrayList<StateInput>();
			for (StateKey<?, ?> stateKey : this.getStateKeys())
			{
				Optional<UInt256> value = this.stateInputs.get(stateKey.get());
				if (value == null)
					throw new IllegalStateException("State inputs for "+this.pendingAtom+" are not fully provisioned");
				
				stateInputs.add(new StateInput(this.pendingAtom.getHash(), stateKey, value.orElse(null)));
			}
			return stateInputs;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
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

