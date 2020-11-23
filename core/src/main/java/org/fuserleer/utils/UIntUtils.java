package org.fuserleer.utils;

/**
 * Utility methods for working with various UInt classes.
 */
public final class UIntUtils {

	// Values taken from
	// https://en.wikipedia.org/wiki/Double-precision_floating-point_format
	private static final int  SIGNIFICAND_PREC = 53; // Including implicit leading one bit
	private static final long SIGNIFICAND_MASK = (1L << (SIGNIFICAND_PREC - 1)) - 1L;
	private static final long SIGNIFICAND_OVF  = 1L << SIGNIFICAND_PREC;

	private static final int  EXPONENT_BIAS    = 1023;

	private UIntUtils() {
		throw new IllegalStateException("Can't construct");
	}

	/**
	 * Compute {@code addend + augend}, throwing an exception if overflow
	 * occurs.
	 *
	 * @param addend The addend
	 * @param augend The augend
	 * @return {@code addend + augend} if no overflow
	 * @throws ArithmeticException if an overflow occurs
	 */
	public static UInt384 addWithOverflow(UInt384 addend, UInt256 augend) {
		UInt384 maxAddend = UInt384.MAX_VALUE.subtract(augend);
		if (maxAddend.compareTo(addend) < 0) {
			throw new ArithmeticException("overflow");
		}
		return addend.add(augend);
	}

	/**
	 * Compute {@code addend + augend}, throwing an exception if overflow
	 * occurs.
	 *
	 * @param addend The addend
	 * @param augend The augend
	 * @return {@code addend + augend} if no overflow
	 * @throws ArithmeticException if an overflow occurs
	 */
	public static UInt384 addWithOverflow(UInt384 addend, UInt384 augend) {
		UInt384 maxAddend = UInt384.MAX_VALUE.subtract(augend);
		if (maxAddend.compareTo(addend) < 0) {
			throw new ArithmeticException("overflow");
		}
		return addend.add(augend);
	}

	/**
	 * Compute {@code minuend - subtrahend}, throwing an exception if underflow
	 * occurs.
	 *
	 * @param addend The minuend
	 * @param augend The subtrahend
	 * @return {@code minuend - subtrahend} if no overflow
	 * @throws ArithmeticException if an underflow occurs
	 */
	public static UInt384 subtractWithUnderflow(UInt384 minuend, UInt256 subtrahend) {
		if (minuend.compareTo(UInt384.from(subtrahend)) < 0) {
			throw new ArithmeticException("underflow");
		}
		return minuend.subtract(subtrahend);
	}

	/**
	 * Compute {@code minuend - subtrahend}, throwing an exception if underflow
	 * occurs.
	 *
	 * @param addend The minuend
	 * @param augend The subtrahend
	 * @return {@code minuend - subtrahend} if no overflow
	 * @throws ArithmeticException if an underflow occurs
	 */
	public static UInt384 subtractWithUnderflow(UInt384 minuend, UInt384 subtrahend) {
		if (minuend.compareTo(subtrahend) < 0) {
			throw new ArithmeticException("underflow");
		}
		return minuend.subtract(subtrahend);
	}

	/**
	 * Convert {@link UInt128} to an approximate {@code double} value.
	 *
	 * @param x the value to convert
	 * @return the value as a {@code double}
	 */
	public static double toDouble(UInt128 x) {
		long h = x.getHigh();
		long l = x.getLow();

		// If it's a number that fits into a long, let the compiler convert.
		if (h == 0L && l >= 0L) {
			return l;
		}

		// Must be at least 64 bits based on initial checks.  Note that it is
		// not possible for this exponent to overflow a double (128 < 1023).
		int shift = bitLength(h);
		long exponent = Long.SIZE + shift - 1L;

		// Merge all the bits into l, discarding lower bits
		l >>>= shift;
		h <<= Long.SIZE - shift;
		l |= h;

		// Extract 53 bits of significand. Note that we make a
		// quick stop part way through to organise rounding.
		// Note that rounding is approximate, not RTNE.
		l >>>= Long.SIZE - SIGNIFICAND_PREC - 1;
		l += 1;
		l >>>= 1;

		// If rounding has caused overflow, then shift an extra bit
		if ((l & SIGNIFICAND_OVF) != 0L) {
			exponent += 1;
			l >>>= 1;
		}

		// Assemble into a double now.
		long raw = (exponent + EXPONENT_BIAS) << (SIGNIFICAND_PREC - 1);
		raw |= l & SIGNIFICAND_MASK;
		return Double.longBitsToDouble(raw);
	}

	private static int bitLength(long n) {
		return Long.SIZE - Long.numberOfLeadingZeros(n);
	}
}
