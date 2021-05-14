package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.fuserleer.Universe;
import org.fuserleer.crypto.Identity;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.SearchResult;
import org.fuserleer.ledger.ShardMapper;
import org.fuserleer.ledger.StateAddress;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.StateOp;
import org.fuserleer.ledger.StateSearchQuery;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.UInt256;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

@SerializerId2("ledger.particle.token.transfer")
public final class TokenParticle extends SignedParticle 
{
	public enum Action 
	{
		MINT, TRANSFER, BURN;

		@JsonValue
		@Override
		public String toString() 
		{
			return this.name();
		}
	}
	
	@JsonProperty("action")
	@DsonOutput(Output.ALL)
	private Action action;
	
	@JsonProperty("quantity")
	@DsonOutput(Output.ALL)
	private UInt256 quantity;

	@JsonProperty("token")
	@DsonOutput(Output.ALL)
	private Hash token;
	
	TokenParticle()
	{
		super();
	}
	
	public TokenParticle(UInt256 quantity, Hash token, Action action, Spin spin, Identity owner)
	{
		super(spin, owner);
		
		this.action = Objects.requireNonNull(action);
		this.quantity = Objects.requireNonNull(quantity);
		this.token = Objects.requireNonNull(token);
	}

	@Override
	public boolean requiresSignature()
	{
		if (getSpin().equals(Spin.DOWN) == true || action.equals(Action.MINT) == true)
			return true;
		else
			return false;
	}

	public Action getAction() 
	{
		return this.action;
	}

	public UInt256 getQuantity() 
	{
		return this.quantity;
	}

	public Hash getToken() 
	{
		return this.token;
	}

	@Override
	public void prepare(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException
	{
		if (this.action == null)
			throw new ValidationException("Action is null");
		
		if (this.action.equals(Action.MINT) == true && getSpin().equals(Spin.DOWN) == true)
			throw new ValidationException("Action is MINT but spin is DOWN");

		if (this.token == null)
			throw new ValidationException("Token is null");
		
		if (this.quantity == null)
			throw new ValidationException("Quantity is null");

		if (this.quantity.compareTo(UInt256.ZERO) == 0)
			throw new ValidationException("Quantity is zero");

		if (this.quantity.compareTo(UInt256.ZERO) < 0)
			throw new ValidationException("Quantity is negative");

		stateMachine.sop(new StateOp(new StateAddress(Particle.class, Spin.spin(this.token, Spin.UP)), Instruction.EXISTS), this);
		stateMachine.sop(new StateOp(new StateAddress(Particle.class, Spin.spin(this.token, Spin.DOWN)), Instruction.NOT_EXISTS), this);
	}
	
	@Override
	public void execute(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException
	{
		Hash token = stateMachine.get("token");
		if (stateMachine.get("token") == null)
		{
			stateMachine.set("token", this.token);
			token = this.token;
		}
		
		// Check all transfers within this state machine as using the same token
		if (token != null && token.equals(this.token) == false)
			throw new ValidationException("Transfer is not multi-token, expected token "+token+" but discovered "+this.token);

		// Check that "out" quantity does not exceed "in" quantity
		UInt256 spendable = stateMachine.get("spendable");
		UInt256 spent = stateMachine.get("spent");
		
		// FIXME take these out to produce an NPE that isn't caught correct / nor fails the unit test
		if (spendable == null)
			spendable = UInt256.ZERO;
		if (spent == null)
			spent = UInt256.ZERO;
		
		if (this.action.equals(Action.TRANSFER) == true)
		{
			if (getSpin().equals(Spin.DOWN) == true)
				spendable = spendable.add(this.quantity);
			else
				spent = spent.add(this.quantity);
		}
		else if (this.action.equals(Action.MINT) == true)
		{
			spendable = spendable.add(this.quantity);
			
			// Check if token spec is embedded in atom
			TokenSpecification tokenSpec = stateMachine.getAtom().getParticle(token);
			if (tokenSpec == null)
			{
				// Not a packed TOKEN/MINT atom ... look for the token if local shard group is responsible for it
				StateAddress tokenSpecStateAddress = new StateAddress(Particle.class, token);
				long numShardGroups = stateMachine.getContext().getLedger().numShardGroups();
				long localShardGroup = ShardMapper.toShardGroup(stateMachine.getContext().getNode().getIdentity(), numShardGroups);
				long tokenSpecShardGroup = ShardMapper.toShardGroup(tokenSpecStateAddress.get(), numShardGroups);
				
				// Search ... if not found or invalid, localShardGroup will respond with REJECT failing the MINT action
				if (localShardGroup == tokenSpecShardGroup)
				{
					// TODO find a more efficient way to do this as it will block ... multi-thread and do in the prepare?
					// TODO not ideal to have dependencies searches when executing ... temporary
					Future<SearchResult> tokenSearchFuture = stateMachine.getContext().getLedger().get(new StateSearchQuery(new StateAddress(Particle.class, token), TokenSpecification.class));
					SearchResult tokenSearchResult;
					try
					{
						tokenSearchResult = tokenSearchFuture.get(10, TimeUnit.SECONDS);
					}
					catch (Exception ex)
					{
						throw new ValidationException("Search of token specification for mint of "+getQuantity()+" "+getToken()+" failed", ex);
					}
					
					if (tokenSearchResult == null || tokenSearchResult.getPrimitive() == null)
						throw new ValidationException("Token specification for mint of "+getQuantity()+" "+getToken()+" not found");
					
					tokenSpec = tokenSearchResult.getPrimitive();
				}
			}
			
			if (tokenSpec != null && tokenSpec.getOwner().equals(getOwner()) == false)
				throw new ValidationException("Token specification for mint of "+getQuantity()+" "+getToken()+" is not owned by "+getOwner());
		}
		else if (this.action.equals(Action.BURN) == true)
		{
			throw new UnsupportedOperationException("BURN token actions not yet supported");
		}
		
		if (Universe.getDefault().getGenesis().contains(this.getHash()) == false && spent.compareTo(spendable) > 0)
			throw new ValidationException("Transfer is invalid, over spending available token "+getToken()+" by "+spent.subtract(spendable));
		
		stateMachine.set("spendable", spendable);
		stateMachine.set("spent", spent);
		
		stateMachine.associate(getOwner().asHash(), this);
	}

	@Override
	public boolean isConsumable()
	{
		return true;
	}
}
