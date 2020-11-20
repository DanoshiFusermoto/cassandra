package org.fuserleer.serialization.mapper;

/**
 * Constants for DSON protocol encoded in CBOR/JSON.
 */
public final class JacksonCodecConstants {
	private JacksonCodecConstants() {
		throw new IllegalStateException("Can't construct");
	}

	// Encodings for CBOR mappings
	public static final byte BYTES_VALUE = 0x01;
	public static final byte HASH_VALUE  = 0x03;
	public static final byte ADDR_VALUE  = 0x04;
	public static final byte U20_VALUE   = 0x05; // 0x20 byte = 256 bit unsigned int
	public static final byte RRI_VALUE   = 0x06;
	public static final byte U30_VALUE   = 0x07; // 0x30 byte = 384 bit unsigned int
	public static final byte U10_VALUE   = 0x09; // 0x10 byte = 128 bit unsigned int
	public static final byte PUB_KEY_VALUE   = 0x10;

	// Type tag prefixes used in strings for JSON mappings
	public static final int STR_VALUE_LEN     = 3;
	public static final String BYTE_STR_VALUE = ":b:";
	public static final String HASH_STR_VALUE = ":h:";
	public static final String U256_STR_VALUE  = ":u:"; // 0x20 byte = 256 bit unsigned int
	public static final String PUB_KEY_STR_VALUE  = ":k:";
}
