package org.fuserleer.ledger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.SerializerConstants;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

// TODO domain security required to limit StateOps
@SerializerId2("ledger.state.op")
public final class StateOp
{
	public static StateOp from(final byte[] bytes) throws IOException
	{
		try
		{
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			DataInputStream dis = new DataInputStream(bais);
			String keyType = StateOp.class.getPackage().getName()+"."+dis.readUTF();
			byte[] keyBytes = new byte[dis.read()];
			dis.read(keyBytes);
			StateKey<?, ?> key = (StateKey<?, ?>) Class.forName(keyType).newInstance();
			key.fromByteArray(keyBytes);
			
			byte[] valueBytes = new byte[dis.read()];
			dis.read(valueBytes);
			UInt256 value = UInt256.from(valueBytes);
			
			Instruction ins = Instruction.get(dis.read());
			
			return new StateOp(key, value, ins);
		}
		catch(InstantiationException | IllegalAccessException | ClassNotFoundException ex)
		{
			throw new IOException(ex);
		}
	}

	public static enum Instruction
	{
		EXISTS(1, true, false, false), NOT_EXISTS(2, true, false, false),
		GET(10, true, false, false), SET(11, false, true, true);

		private final byte		opcode;	
		private final boolean 	evaluatable;
		private final boolean 	parameter;
		private final boolean 	output;
		
		Instruction(int opcode, boolean evaluatable, boolean parameter, boolean output)
		{
			this.opcode = (byte) opcode;
			this.evaluatable = evaluatable;
			this.parameter = parameter;
			this.output = output;
		}

		@JsonValue
		@Override
		public String toString() 
		{
			return this.name();
		}

		public byte opcode()
		{
			return this.opcode;
		}
		
		public boolean evaluatable()
		{
			return this.evaluatable;
		}

		public boolean parameterized()
		{
			return this.parameter;
		}

		public boolean output()
		{
			return this.output;
		}
		
		public static Instruction get(int opcode)
		{
			for (int i = 0 ; i < Instruction.values().length ; i++)
				if (Instruction.values()[i].opcode == opcode)
					return Instruction.values()[i];
			
			return null;
		}
	}
	
	
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;
	
	@JsonProperty("key")
	@DsonOutput(Output.ALL)
	private StateKey<?, ?> key;

	@JsonProperty("value")
	@DsonOutput(Output.ALL)
	private UInt256 value;
	
	@JsonProperty("ins")
	@DsonOutput(Output.ALL)
	private Instruction ins;
	
	@SuppressWarnings("unused")
	private StateOp()
	{
		// FOR SERIALIZER
	}
	
	public StateOp(final StateKey<?, ?> key, final Instruction ins)
	{
		Objects.requireNonNull(key, "Key is null");
		Objects.requireNonNull(ins, "Instruction is null");
		
		if (ins.parameterized() == true)
			throw new IllegalArgumentException("Instruction "+ins+" requires a parameter");
		
		this.key = key;
		this.ins = ins;
		this.value = null;
	}

	public StateOp(final StateKey<?, ?> key, final UInt256 value, final Instruction ins)
	{
		Objects.requireNonNull(key, "Key is null");
		Objects.requireNonNull(ins, "Instruction is null");
		Objects.requireNonNull(value, "Value is null");
		
		if (ins.parameterized() == false)
			throw new IllegalArgumentException("Instruction "+ins+" is parameterless");
		
		this.key = key;
		this.value = value;
		this.ins = ins;
	}

	public StateKey<?, ?> key()
	{
		return this.key;
	}

	public UInt256 value()
	{
		return this.value;
	}

	public Instruction ins()
	{
		return this.ins;
	}

	@Override
	public boolean equals(Object other)
	{
		if (other == null)
			return false;
		if (other == this)
			return true;

		if (other instanceof StateOp)
		{
			if (((StateOp)other).key.equals(this.key) == true &&
				((StateOp)other).ins.equals(this.ins) == true &&
				((((StateOp)other).value == null && this.value == null) || ((StateOp)other).value.compareTo(this.value) == 0))
				return true;
		}
		
		return false;
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(this.key, this.ins, this.value);
	}

	@Override
	public String toString()
	{
		return this.ins+" "+this.key+(this.value == null ? "" : " "+this.value);
	}
	
	public byte[] toByteArray() throws IOException
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(baos);
		String keyType = this.key.getClass().getSimpleName();
		byte[] keyBytes = this.key.toByteArray();
		dos.writeUTF(keyType);
		dos.write(keyBytes.length);
		dos.write(keyBytes);
		
		byte[] valueBytes = this.value.toByteArray();
		dos.write(valueBytes.length);
		dos.write(valueBytes);

		dos.write(this.ins.opcode());
		return baos.toByteArray();
	}
}
