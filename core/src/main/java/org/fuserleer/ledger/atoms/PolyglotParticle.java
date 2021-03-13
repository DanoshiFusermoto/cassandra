package org.fuserleer.ledger.atoms;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.io.IOUtils;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.StateKey;
import org.fuserleer.ledger.StateMachine;
import org.fuserleer.ledger.StateOp;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.SerializerId2;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.json.JSONObject;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.serialization.mapper.JacksonCodecConstants;
import org.fuserleer.utils.Bytes;
import org.fuserleer.utils.Numbers;
import org.fuserleer.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;

@SerializerId2("ledger.atoms.particle.polyglot")
public final class PolyglotParticle extends Particle
{
	public final static int MAX_CODE_SIZE = 65535;
	
	public static final PolyglotParticle from(Language language, InputStream stream, Map<String, Object> fields) throws IOException
	{
		String code = IOUtils.toString(stream, StandardCharsets.UTF_8.name());
		return new PolyglotParticle(language, code, fields);
	}
	
	public static enum Language
	{
		JAVASCRIPT, PYTHON;
		
		@JsonValue
		@Override
		public String toString() 
		{
			return this.name();
		}
	}
	
	private Map<String, Object>	fields;

	private transient Context 	context;
	private transient String	code;
	private transient Language	language;
	private transient Value		eval;
	private transient Value		instance;
	
	private PolyglotParticle()
	{
		super(Spin.UP);

		this.fields = new TreeMap<>();
	}
	
	private PolyglotParticle(final Language language, final String code, final Map<String, Object> fields)
	{
		this();
		
		this.context = Context.newBuilder("js").allowAllAccess(true).build();
		if (fields != null)
			this.fields.putAll(fields);
		
		this.language = Objects.requireNonNull(language, "Language is null");
		Objects.requireNonNull(code, "Code is null");
		Numbers.lessThan(code.length(), PolyglotParticle.MAX_CODE_SIZE, "Code size "+code.length()+" is greater than max "+PolyglotParticle.MAX_CODE_SIZE);
		this.code = code;
	}

	@JsonProperty("fields")
	@DsonOutput(Output.ALL)
	private byte[] getJsonFields() 
	{
		JSONObject fieldsJSON = Serialization.getInstance().toJsonObject(this.fields, Output.WIRE);
		byte[] bytes = fieldsJSON.toString().getBytes(StandardCharsets.UTF_8);
		return bytes;
	}

	@JsonProperty("fields")
	private void setJsonFields(byte[] fields) 
	{
		// TODO Serializer limitations hack.  Loses type info on Object maps.  Easier to patch with JSON than binary
		JSONObject fieldsJSON = new JSONObject(new String(fields, StandardCharsets.UTF_8));
		for (String key : fieldsJSON.keySet())
		{
			Object value = fieldsJSON.get(key);
			if (value instanceof JSONObject)
			{
				String typed = ((JSONObject)value).getString("typed");
				Class<?> clazz = Serialization.getInstance().getClassForId(typed);
				this.fields.put(key, Serialization.getInstance().fromJsonObject((JSONObject)value, clazz));
			}
			else if (value instanceof String)
			{
				if (((String)value).startsWith(JacksonCodecConstants.BYTE_STR_VALUE) == true)
					this.fields.put(key, Bytes.fromBase64String(((String)value).substring(JacksonCodecConstants.STR_VALUE_LEN)));
				else if (((String)value).startsWith(JacksonCodecConstants.HASH_STR_VALUE) == true)
					this.fields.put(key, new Hash(((String)value).substring(JacksonCodecConstants.STR_VALUE_LEN)));
				else if (((String)value).startsWith(JacksonCodecConstants.U256_STR_VALUE) == true)
					this.fields.put(key, UInt256.from(((String)value).substring(JacksonCodecConstants.STR_VALUE_LEN)));
				else
					this.fields.put(key, value);
			}
			else
				this.fields.put(key, value);
		}
	}

	public <T> T get(final String field)
	{
		return (T) this.fields.get(field);
	}
	
	public Language getLanguage()
	{
		return this.language;
	}
	
	@Override
	public boolean isConsumable()
	{
		return false;
	}

	@Override
	public void prepare(final StateMachine stateMachine, final Object ... arguments) throws ValidationException, IOException
	{
		Objects.requireNonNull(stateMachine, "State machine is null");
		
		this.context.enter();
		try
		{
			if (this.eval == null)
			{
				// Bridges to state machine
				Function<Object, Object> get = (t) -> 
				{
					if (t instanceof String)
						return get((String)t);
					
					throw new IllegalArgumentException("Key is not of type String");
				};

				Function<Object, Object> input = (t) -> 
				{
					if (t instanceof StateKey)
						return stateMachine.getInput((StateKey<?, ?>)t);
					
					throw new IllegalArgumentException("Key is not of type StateAddress");
				};

				Function<Object, Object> output = (t) -> 
				{
					if (t instanceof StateKey)
						return stateMachine.getOutput((StateKey<?, ?>)t);
					
					throw new IllegalArgumentException("Key is not of type StateAddress");
				};

				Consumer<StateOp> sop = (s) -> {
					stateMachine.sop(s, PolyglotParticle.this);
				};
				
				Consumer<Hash> associate = (t) -> {
					stateMachine.associate(Hash.from(t), PolyglotParticle.this);
				};

				this.context.getBindings("js").putMember("get", get);
				this.context.getBindings("js").putMember("input", input);
				this.context.getBindings("js").putMember("output", output);
				this.context.getBindings("js").putMember("sop", sop);
				this.context.getBindings("js").putMember("associate", associate);
				this.eval = this.context.eval("js", code);
				this.instance = this.eval.execute().newInstance();
			}
			
			this.instance.invokeMember("prepare", arguments);
		}
		finally
		{
			this.context.leave();
		}
	}

	@Override
	public void execute(final StateMachine stateMachine, final Object ... arguments) throws ValidationException, IOException
	{
		this.context.enter();
		try
		{
			this.instance.invokeMember("execute", arguments);
		}
		finally
		{
			this.context.leave();
		}
	}
}
