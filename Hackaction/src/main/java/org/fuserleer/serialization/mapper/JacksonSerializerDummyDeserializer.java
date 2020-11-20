package org.fuserleer.serialization.mapper;

import java.io.IOException;

import org.fuserleer.serialization.SerializerDummy;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * Deserializer for special {@link SerializerDummy} value.
 */
class JacksonSerializerDummyDeserializer extends StdDeserializer<SerializerDummy> {
	private static final long serialVersionUID = -2472482347700365657L;

	JacksonSerializerDummyDeserializer() {
		this(null);
	}

	JacksonSerializerDummyDeserializer(Class<SerializerDummy> t) {
		super(t);
	}

	@Override
	public SerializerDummy deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
		p.getLongValue(); // Ignored
		return SerializerDummy.DUMMY;
	}
}