package org.fuserleer.serialization.mapper;

import java.io.IOException;

import org.fuserleer.crypto.Hash;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * Serializer for conversion from {@code Hash} data
 * to the appropriate JSON encoding.
 */
class JacksonJsonHashSerializer extends StdSerializer<Hash> {
	private static final long serialVersionUID = -2472482347700365657L;

	JacksonJsonHashSerializer() {
		this(null);
	}

	JacksonJsonHashSerializer(Class<Hash> t) {
		super(t);
	}

	@Override
	public void serialize(Hash value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
		jgen.writeString(JacksonCodecConstants.HASH_STR_VALUE + value.toString());
	}
}
