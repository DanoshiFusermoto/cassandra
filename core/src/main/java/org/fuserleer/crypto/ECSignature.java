package org.fuserleer.crypto;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Objects;

import org.bouncycastle.asn1.ASN1InputStream;
import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.DLSequence;
import org.fuserleer.serialization.DsonOutput;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.Bytes;
import org.fuserleer.serialization.SerializerDummy;
import org.fuserleer.serialization.SerializerId2;
import org.fuserleer.serialization.SerializerConstants;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("crypto.ecdsa_signature")
public final class ECSignature
{
	// Placeholder for the serializer ID
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private SerializerDummy serializer = SerializerDummy.DUMMY;

	@JsonProperty("version")
	@DsonOutput(Output.ALL)
	private short version = 100;

	/* The two components of the signature. */
	private BigInteger r;
	private BigInteger s;

	public ECSignature()
	{
		this(BigInteger.ZERO, BigInteger.ZERO);
	}

	/**
     * Constructs a signature with the given components. Does NOT automatically canonicalise the signature.
     */
    public ECSignature(final BigInteger r, final BigInteger s)
    {
    	super();

    	this.r = Objects.requireNonNull(r, "Signature R is null");
        this.s = Objects.requireNonNull(s, "Signature S is null");
    }

	public BigInteger getR() 
	{
		return this.r;
	}

	public BigInteger getS() 
	{
		return this.s;
	}

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        	return true;

        if (o instanceof ECSignature) 
        {
        	ECSignature signature = (ECSignature) o;
        	return Objects.equals(this.r, signature.r) && Objects.equals(this.s, signature.s);
        }
        
        return false;
    }

	@Override
	public int hashCode() 
	{
		return Objects.hash(this.r, this.s);
	}
	
	@JsonProperty("r")
	@DsonOutput(Output.ALL)
	private byte[] getJsonR() 
	{
		return Bytes.trimLeadingZeros(this.r.toByteArray());
	}

	@JsonProperty("s")
	@DsonOutput(Output.ALL)
	private byte[] getJsonS() 
	{
		return Bytes.trimLeadingZeros(this.s.toByteArray());
	}

	@JsonProperty("r")
	private void setJsonR(byte[] r) 
	{
		// Set sign to positive to stop BigInteger interpreting high bit as sign
		this.r = new BigInteger(1, r);
	}

	@JsonProperty("s")
	private void setJsonS(byte[] s) 
	{
		// Set sign to positive to stop BigInteger interpreting high bit as sign
		this.s = new BigInteger(1, s);
	}
	
	public static ECSignature decodeFromDER(final byte[] bytes) 
	{
		Objects.requireNonNull(bytes, "Signature DER bytes is null");
		if (bytes.length == 0)
			throw new IllegalArgumentException("Signature DER bytes length is zero");

		try (ASN1InputStream decoder = new ASN1InputStream(bytes)) 
		{
			DLSequence seq = (DLSequence) decoder.readObject();
			ASN1Integer r = (ASN1Integer) seq.getObjectAt(0);
			ASN1Integer s = (ASN1Integer) seq.getObjectAt(1);
			return new ECSignature(r.getPositiveValue(), s.getPositiveValue());
		} 
		catch (IOException e) 
		{
			throw new IllegalArgumentException("Failed to read bytes as ASN1 decode bytes", e);
		} 
		catch (ClassCastException e) 
		{
			throw new IllegalStateException("Failed to cast to ASN1Integer", e);
		}
	}
}
