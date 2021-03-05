package org.fuserleer.utils;

import java.util.Objects;

/**
 * Utilities for manipulating primitive {@code int} values.
 */
public final class Ints {
	private Ints() {
		throw new IllegalStateException("Can't construct");
	}

	/**
	 * Create a byte array of length {@link Integer#BYTES}, and
	 * populate it with {@code value} in big-endian order.
	 *
	 * @param value The value to convert
	 * @return The resultant byte array.
	 */
	public static byte[] toByteArray(int value) {
		return copyTo(value, new byte[Integer.BYTES], 0);
	}

	/**
	 * Copy the byte value of {@code value} into {@code bytes}
	 * starting at {@code offset}.  A total of {@link Integer#BYTES}
	 * will be written to {@code bytes}.
	 *
	 * @param value  The value to convert
	 * @param bytes  The array to write the value into
	 * @param offset The offset at which to write the value
	 * @return The value of {@code bytes}
	 */
	public static byte[] copyTo(int value, byte[] bytes, int offset) {
		Objects.requireNonNull(bytes, "bytes is null for 'int' conversion");
	    for (int i = offset + Integer.BYTES - 1; i >= offset; i--) {
	    	bytes[i] = (byte) (value & 0xFF);
	    	value >>>= 8;
	    }
	    return bytes;
	}

	/**
	 * Exactly equivalent to {@code fromByteArray(bytes, 0)}.
	 *
	 * @param bytes The byte array to decode to an integer
	 * @return The decoded integer value
	 * @see #fromByteArray(byte[], int)
	 */
	public static int fromByteArray(byte[] bytes) {
		return fromByteArray(bytes, 0);
	}

	/**
	 * Decode an integer from array {@code bytes} at {@code offset}.
	 * Bytes from array {@code bytes[offset]} up to and including
	 * {@code bytes[offset + Integer.BYTES - 1]} will be read from
	 * array {@code bytes}.
	 *
	 * @param bytes The byte array to decode to an integer
	 * @param offset The offset within the array to start decoding
	 * @return The decoded integer value
	 */
	public static int fromByteArray(byte[] bytes, int offset) {
		Objects.requireNonNull(bytes, "bytes is null for 'int' conversion");
		int value = 0;
		for (int b = 0; b < Integer.BYTES; b++) {
			value <<= 8;
			value |= bytes[offset + b] & 0xFF;
		}

		return value;
	}

	/**
	 * Decode an integer from array {@code bytes} at {@code offset}
	 * of length {@code len}.  Bytes from array {@code bytes[offset]}
	 * up to and including {@code bytes[offset + len - 1]} will be read
	 * from array {@code bytes}.
	 *
	 * @param bytes The byte array to decode to an integer
	 * @param offset The offset within the array to start decoding
	 * @param len The number of bytes to read
	 * @return The decoded integer value
	 */
	public static int fromByteArray(byte[] bytes, int offset, int len) {
		Objects.requireNonNull(bytes, "bytes is null for 'int' conversion");
		int value = 0;
		for (int b = 0; b < len; b++) {
			value <<= 8;
			value |= bytes[offset + b] & 0xFF;
		}

		return value;
	}

	/**
	 * Assemble an {@code int} value from it's component bytes.
	 *
	 * @param b0 Most significant byte
	 * @param b1 Next most significant byte
	 * @param b2 Next least significant byte
	 * @param b3 Least significant byte
	 * @return The {@code int} value represented by the arguments.
	 */
	public static int fromBytes(byte b0, byte b1, byte b2, byte b3) {
	    return b0 << 24 | (b1 & 0xFF) << 16 | (b2 & 0xFF) << 8 | (b3 & 0xFF);
	}

	public static void greaterThan(int value, int bound, String message)
	{
		if (value <= bound)
		{
			if (message == null)
				throw new IllegalArgumentException();
			else
				throw new IllegalArgumentException(message);
		}
	}
}
