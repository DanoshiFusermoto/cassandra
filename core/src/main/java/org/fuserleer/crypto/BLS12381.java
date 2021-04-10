package org.fuserleer.crypto;

import java.util.List;
import java.util.Objects;

import org.apache.milagro.amcl.BLS381.BIG;
import org.apache.milagro.amcl.BLS381.ECP;
import org.apache.milagro.amcl.BLS381.MPIN;
import org.fuserleer.crypto.bls.group.AtePairing;
import org.fuserleer.crypto.bls.group.G1Point;
import org.fuserleer.crypto.bls.group.G2Point;
import org.fuserleer.crypto.bls.group.GTPoint;
import org.fuserleer.utils.Numbers;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
public final class BLS12381 
{
	/**
	 * Generates a SignatureAndPublicKey.
	 *
	 * @param keyPair The public and private key pair, not null
	 * @param message The message to sign, not null
	 * @return The SignatureAndPublicKey, not null
	 */
	public static BLSSignatureAndPublicKey sign(final BLSKeyPair keyPair, final byte[] message) 
	{
		Objects.requireNonNull(keyPair, "Key pair is null");
		Objects.requireNonNull(message, "Message to sign is null");
		Numbers.isZero(message.length, "Message is empty");
		CryptoUtils.BLSSigned.incrementAndGet();

		G1Point hashInGroup1 = hashFunction(message);
		/*
		 * The signature is hash point in G1 multiplied by the private key.
		 */
		G1Point sig = keyPair.getPrivateKey().sign(hashInGroup1);
		return new BLSSignatureAndPublicKey(new BLSSignature(sig), keyPair.getPublicKey());
	}

	/**
	 * Verifies the given BLS signature against the message bytes using the public key.
	 * 
	 * @param publicKey The public key, not null
	 * @param signature The signature, not null
	 * @param message The message data to verify, not null
	 * 
	 * @return True if the verification is successful.
	 */
	public static boolean verify(final BLSPublicKey publicKey, final BLSSignature signature, final byte[] message) 
	{
		Objects.requireNonNull(publicKey, "Public key is null");
		Objects.requireNonNull(signature, "Signature is null");
		Objects.requireNonNull(message, "Message to verify is null");
		Numbers.isZero(message.length, "Message is empty");
		
		CryptoUtils.BLSVerified.incrementAndGet();

		G1Point hashInGroup1 = hashFunction(message);

		G2Point g2GeneratorNeg = BLSCurveParameters.g2Generator().neg();
		GTPoint e = AtePairing.pair2(hashInGroup1, publicKey.g2Point(), signature.g1Point(), g2GeneratorNeg);
        return e.isunity();
	}

	/**
	 * Verifies the given BLS signature against the message bytes using the public key.
	 * 
	 * @param sigAndPubKey The signature and public key, not null
	 * @param message The message data to verify, not null
	 * 
	 * @return True if the verification is successful, not null
	 */
	public static boolean verify(final BLSSignatureAndPublicKey sigAndPubKey, byte[] message) 
	{
		Objects.requireNonNull(sigAndPubKey, "Signature and public key pair is null");
		return verify(sigAndPubKey.getPublicKey(), sigAndPubKey.getSignature(), message);
	}

	/**
	 * Aggregates list of Signature and PublicKey pairs
	 * 
	 * @param sigAndPubKeyList The list of Signatures and corresponding Public keys to aggregate, not
	 *        null
	 * @return SignatureAndPublicKey, not null
	 * @throws IllegalArgumentException if parameter list is empty
	 */
	public static BLSSignatureAndPublicKey aggregate(final List<BLSSignatureAndPublicKey> sigAndPubKeyList) 
	{
		Objects.requireNonNull(sigAndPubKeyList, "Signature and public key pair list is null");
		Numbers.isZero(sigAndPubKeyList.size(), "Signature and public key pair list is empty");
		return sigAndPubKeyList.stream().reduce((a, b) -> a.combine(b)).get();
	}

	/**
	 * Aggregates list of PublicKey pairs
	 * 
	 * @param publicKeyList The list of public keys to aggregate, not null
	 * @return PublicKey The public key, not null
	 * @throws IllegalArgumentException if parameter list is empty
	 */
	public static BLSPublicKey aggregatePublicKey(final List<BLSPublicKey> publicKeyList) 
	{
		Objects.requireNonNull(publicKeyList, "Public key list is null");
		Numbers.isZero(publicKeyList.size(), "Public key list is empty");
		return publicKeyList.stream().reduce((a, b) -> a.combine(b)).get();
	}

	/**
	 * Aggregates list of Signature pairs
	 * 
	 * @param signatureList The list of signatures to aggregate, not null
	 * @throws IllegalArgumentException if parameter list is empty
	 * @return Signature, not null
	 */
	public static BLSSignature aggregateSignatures(final List<BLSSignature> signatureList) 
	{
		Objects.requireNonNull(signatureList, "Signature list is null");
		Numbers.isZero(signatureList.size(), "Signature key list is empty");
		return signatureList.stream().reduce((a, b) -> a.combine(b)).get();
	}

	private static G1Point hashFunction(byte[] message) 
	{
		byte[] hashByte = MPIN.HASH_ID(ECP.SHA256, message, BIG.MODBYTES);
		return new G1Point(ECP.mapit(hashByte));
	}
}
