package org.fuserleer.ledger.exceptions;

import java.util.Objects;

import org.fuserleer.crypto.Identity;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.utils.UInt256;

public class InsufficientBalanceException extends ValidationException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 937989582224129068L;
	
	private final Identity spender;
	private final Identity receiver;
	private final Hash token;
	private final UInt256 quantity;
	
	public InsufficientBalanceException(final Identity spender, final Hash token, final UInt256 quantity, final Identity receiver)
	{
		this("Insufficient balance to spend "+quantity+":"+token+" to "+receiver+" from "+spender, spender, token, quantity, receiver);
	}

	public InsufficientBalanceException(final String message, final Identity spender, final Hash token, final UInt256 quantity, final Identity receiver)
	{
		super(message);

		this.spender = Objects.requireNonNull(spender, "Spender is null");
		this.quantity = Objects.requireNonNull(quantity, "Quantity is null");
		this.token = Objects.requireNonNull(token, "Token is null");
		this.receiver = Objects.requireNonNull(receiver, "Receiver is null");
		Hash.notZero(this.token, "Token is ZERO");
	}

	public Identity getSpender() 
	{
		return this.spender;
	}

	public Identity getReceiver() 
	{
		return this.receiver;
	}

	public Hash getToken() 
	{
		return this.token;
	}

	public UInt256 getQuantity() 
	{
		return this.quantity;
	}
}
