package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import org.fuserleer.crypto.Identity;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateAddress;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.StateOp;
import org.fuserleer.ledger.StateOp.Instruction;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.utils.Numbers;
import org.fuserleer.utils.UInt256;
import org.fuserleer.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.particle.file.hosted")
public final class HostedFileParticle extends MediaDeclaration
{
	public static int MAX_DATA_SIZE = 1<<20; // 1MB :-)
	
	@JsonProperty("path")
	@DsonOutput(Output.ALL)
	private String path;

	@JsonProperty("data")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private byte[] data;
	
	@JsonProperty("size")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private int size;

	private transient Hash checksum;
	
	HostedFileParticle() 
	{
		super();
	}
	
	public HostedFileParticle(String contentType, String path, byte[] data, Identity owner) 
	{
		super(contentType, owner);
		
		Objects.requireNonNull(path, "Path is null");
		Numbers.lessThan(path.length(), 3, "Path is too short");
		Numbers.greaterThan(path.length(), 128, "Path is too long");
		
		Objects.requireNonNull(data, "Data is null");
		if (data.length == 0)
			throw new IllegalArgumentException("Data is empty");
		
		if (data.length > HostedFileParticle.MAX_DATA_SIZE)
			throw new IllegalArgumentException("Data size "+data.length+"is greater than MAX_DATA_SIZE "+HostedFileParticle.MAX_DATA_SIZE);

		this.path = path.toLowerCase();
		this.data = Arrays.copyOf(data, data.length);
		this.size = this.data.length;
	}
	
	public void minimize()
	{
		getChecksum();
		this.data = null;
	}
	
	public int size()
	{
		return this.size;
	}
	
	public String getPath()
	{
		return this.path.toLowerCase();
	}
	
	@JsonProperty("checksum")
	@DsonOutput(Output.ALL)
	public Hash getChecksum()
	{
		if (this.checksum == null)
			this.checksum = Hash.from(this.data);
		
		return this.checksum;
	}
	
	public byte[] getData()
	{
		return Arrays.copyOf(this.data, this.data.length);
	}

	@Override
	public void prepare(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException 
	{
		if (this.path == null)
			throw new ValidationException("Path is null");

		if (this.path.length() < 3)
			throw new ValidationException("Path is too short");
		
		if (this.path.length() > 128)
			throw new ValidationException("Path is too long");
		
		if (this.data == null)
			throw new ValidationException("Data is null");
			
		if (this.data.length == 0)
			throw new ValidationException("Data is empty");
		
		if (this.data.length > HostedFileParticle.MAX_DATA_SIZE)
			throw new ValidationException("Data size "+this.data.length+"is greater than MAX_DATA_SIZE "+HostedFileParticle.MAX_DATA_SIZE);
		
		stateMachine.sop(new StateOp(new StateAddress(HostedFileParticle.class, Hash.from(this.path.toLowerCase())), Instruction.GET), this);
		stateMachine.sop(new StateOp(new StateAddress(HostedFileParticle.class, Hash.from(this.path.toLowerCase())), Instruction.NOT_EXISTS), this);
		
		super.prepare(stateMachine, arguments);
	}

	@Override
	public void execute(StateMachine stateMachine, Object ... arguments) throws ValidationException, IOException
	{
		stateMachine.sop(new StateOp(new StateAddress(HostedFileParticle.class, Hash.from(this.path.toLowerCase())), UInt256.from(getHash().toByteArray()), Instruction.SET), this);
		
		super.execute(stateMachine, arguments);
	}
	
	@Override
	public boolean isConsumable()
	{
		return false;
	}
}
