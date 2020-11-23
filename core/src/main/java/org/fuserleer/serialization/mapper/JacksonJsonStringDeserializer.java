package org.fuserleer.serialization.mapper;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * Deserializer for translation from JSON encoded {@code String} data
 * to a {@code String} object.
 */
class JacksonJsonStringDeserializer extends StdDeserializer<String> {
	private static final long serialVersionUID = -2472482347700365657L;

	JacksonJsonStringDeserializer() {
		this(null);
	}

	JacksonJsonStringDeserializer(Class<String> t) {
		super(t);
	}

	@Override
	public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException 
	{
		String value = p.getValueAsString();
		return value;
	
//		if (!value.startsWith(JacksonCodecConstants.STR_STR_VALUE))
//			throw new InvalidFormatException(p, "Expecting string", value, Hash.class);
		
//		return value.substring(JacksonCodecConstants.STR_VALUE_LEN);
	}
}
