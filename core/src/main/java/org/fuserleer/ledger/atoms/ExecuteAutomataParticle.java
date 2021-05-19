package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.fuserleer.crypto.Identity;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.SearchResult;
import org.fuserleer.ledger.StateAddress;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.StateSearchQuery;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.automata.execute")
public final class ExecuteAutomataParticle extends Particle
{
	@JsonProperty("identity")
	@DsonOutput(Output.ALL)
	private Identity identity;

	@JsonProperty("method")
	@DsonOutput(Output.ALL)
	private String method;
	
	@JsonProperty("arguments")
	@DsonOutput(Output.ALL)
	private List<Object> arguments;
	
	private transient AutomataParticle automata;
	
	@SuppressWarnings("unused")
	private ExecuteAutomataParticle()
	{
		super();
	}

	public ExecuteAutomataParticle(final Identity identity, final String method, final Object ... arguments)
	{
		super(Spin.UP);
		
		Objects.requireNonNull(identity, "Identity is null");
		if (identity.getPrefix() != Identity.COMPUTE)
			throw new IllegalArgumentException("Identity must be a COMPUTE type");
		
		Objects.requireNonNull(method, "Method is null");
		Numbers.inRange(method.length(), 1, 64, "Method name is invalid");
		
		this.identity = identity;
		this.method = method;
		if (arguments == null || arguments.length == 0)
			this.arguments = Collections.emptyList();
		else
		{
			Numbers.greaterThan(arguments.length, 8, "Too many arguments");
			this.arguments = Arrays.asList(arguments);
		}
	}
	
	public Identity getIdentity() 
	{
		return identity;
	}

	public String getMethod() 
	{
		return this.method;
	}

	public List<Object> getArguments() 
	{
		return Collections.unmodifiableList(this.arguments);
	}

	@Override
	public boolean isConsumable() 
	{
		return false;
	}
	
	@Override
	public void prepare(StateMachine stateMachine) throws ValidationException, IOException
	{
		if (this.identity == null)
			throw new ValidationException("identity is null");
		
		if (this.identity.getPrefix() != Identity.COMPUTE)
			throw new ValidationException("identity must be a COMPUTE type");
		
		if (this.method == null)
			throw new ValidationException("Method is null");
		
		if (method.length() < 0 || method.length() > 64)
			throw new ValidationException("Method name is invalid");

		if (this.arguments != null && this.arguments.size() > 8)
			throw new ValidationException("Too many arguments");
		
		if (stateMachine.getPendingAtom().getAtom().hasParticle(this.identity.getKey().asHash()) == false)
		{
			// TODO will need proper provisioning, validation and storage of unknown automata here
			// TODO what happens if an automata is updated?
			Future<SearchResult> automataSearchResultFuture = stateMachine.getContext().getLedger().get(new StateSearchQuery(new StateAddress(Particle.class, this.identity.getKey().asHash()), Particle.class));
			SearchResult automataSearchResult = null;
			try
			{
				automataSearchResult = automataSearchResultFuture.get(5, TimeUnit.SECONDS);
			}
			catch (Exception ex)
			{
				throw new ValidationException("Automata search for "+this.identity+" failed", ex);
			}
			
			if (automataSearchResult == null || automataSearchResult.getPrimitive() == null)
				throw new ValidationException("Automata for identity "+this.identity+" is null");
			
			this.automata = automataSearchResult.getPrimitive();
		}
		else
			this.automata = stateMachine.getPendingAtom().getAtom().getParticle(this.identity.getKey().asHash());
			
		this.automata.prepare(stateMachine, this.method, this.arguments == null ? null : this.arguments.toArray());
		
		super.prepare(stateMachine);
	}

	@Override
	public void execute(StateMachine stateMachine) throws ValidationException, IOException 
	{
		if (this.automata == null)
			throw new ValidationException("Automata for identity "+this.identity+" is null");
		
		this.automata.execute(stateMachine, this.method, this.arguments == null ? null : this.arguments.toArray());
	}
}
