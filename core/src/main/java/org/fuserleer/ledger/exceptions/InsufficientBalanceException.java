package org.fuserleer.ledger.exceptions;

import java.util.Objects;

import org.fuserleer.crypto.ECPublicKey;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.utils.UInt256;

public class InsufficientBalanceException extends ValidationException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 937989582224129068L;
	
	private final ECPublicKey spender;
	private final ECPublicKey receiver;
	private final Hash token;
	private final UInt256 quantity;
	
	public InsufficientBalanceException(String message, ECPublicKey spender, Hash token, UInt256 quantity, ECPublicKey receiver)
	{
		super(message);

		this.spender = Objects.requireNonNull(spender);
		this.quantity = Objects.requireNonNull(quantity);
		this.token = Objects.requireNonNull(token);
		this.receiver = Objects.requireNonNull(receiver);
		
		if (token.equals(Hash.ZERO) == true)
			throw new IllegalArgumentException("Token is ZERO");
	}

	public InsufficientBalanceException(ECPublicKey spender, Hash token, UInt256 quantity, ECPublicKey receiver)
	{
		this("Insufficient balance to spend "+quantity+":"+token+" to "+receiver+" from "+spender, spender, token, quantity, receiver);
	}

	public ECPublicKey getSpender() 
	{
		return this.spender;
	}

	public ECPublicKey getReceiver() 
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
