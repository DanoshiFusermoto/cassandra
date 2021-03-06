package org.fuserleer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.fuserleer.crypto.KeyPair;
import org.fuserleer.apps.SimpleIncrementingAutomata;
import org.fuserleer.crypto.BLSKeyPair;
import org.fuserleer.crypto.BLSPublicKey;
import org.fuserleer.crypto.ECKeyPair;
import org.fuserleer.crypto.Hash;
import org.fuserleer.exceptions.ValidationException;
import org.fuserleer.ledger.Block;
import org.fuserleer.ledger.ShardMapper;
import org.fuserleer.ledger.atoms.Atom;
import org.fuserleer.ledger.atoms.ExecuteAutomataParticle;
import org.fuserleer.ledger.atoms.Particle.Spin;
import org.fuserleer.ledger.atoms.TokenSpecification;
import org.fuserleer.ledger.atoms.TokenParticle;
import org.fuserleer.ledger.atoms.TokenParticle.Action;
import org.fuserleer.logging.Logger;
import org.fuserleer.logging.Logging;
import org.fuserleer.serialization.Serialization;
import org.fuserleer.serialization.DsonOutput.Output;
import org.fuserleer.utils.Bytes;
import org.fuserleer.utils.UInt128;
import org.fuserleer.utils.UInt256;

import java.io.File;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

public final class GenerateUniverses
{
	private static final Logger LOGGER = Logging.getLogger("generate_universes");

	public static final String RADIX_ICON_URL = "https://assets.radixdlt.com/icons/icon-xrd-32x32.png";
	
	private static final CommandLineParser parser = new DefaultParser(); 
	private final static Options options = new Options().addOption("shardgroups", true, "Number of initial shard groups of the universe")
														.addOption("nodes", true, "Number of genesis nodes of the universe")
			   											.addOption("keys", true, "The keys for the genesis nodes of the universe");


	private final ECKeyPair universeKey;
	private final int numNodes;
	private final int shardGroups;
	private final Set<BLSPublicKey> nodeKeys;
	private final CommandLine commandLine;
//	private final Configuration configuration;

	public GenerateUniverses(String[] arguments) throws Exception 
	{
		this.commandLine = GenerateUniverses.parser.parse(options, arguments);
		Security.addProvider(new BouncyCastleProvider());

		this.universeKey = ECKeyPair.fromFile(new File("universe.key"), true);
		
		this.nodeKeys = new LinkedHashSet<BLSPublicKey>();
		if (this.commandLine.hasOption("nodes") && this.commandLine.hasOption("shardgroups"))
		{
			this.numNodes = Integer.parseInt(this.commandLine.getOptionValue("nodes"));
			this.shardGroups = Integer.parseInt(this.commandLine.getOptionValue("shardgroups"));
			int nodesPerShardGroup = this.numNodes / this.shardGroups;
			int nodeID = 0;
			
			for (int sg = 0 ; sg < this.shardGroups ; sg++)
			{
				Set<BLSKeyPair> shardNodeKeys = new HashSet<BLSKeyPair>();
				while(shardNodeKeys.size() < nodesPerShardGroup)
				{
					BLSKeyPair nodeKey = new BLSKeyPair();
					long shardGroup = ShardMapper.toShardGroup(nodeKey.getPublicKey(), this.shardGroups);
					if (shardGroup == sg)
						shardNodeKeys.add(nodeKey);
				}

				for (BLSKeyPair shardNodeKey : shardNodeKeys)
				{
					KeyPair.toFile(new File("node-"+nodeID+".key"), shardNodeKey);
					nodeID++;
					this.nodeKeys.add(shardNodeKey.getPublicKey());
				}
			}
		}
		else
		{
			// TODO want to be able to specify multiple nodes to get the genesis mass as bootstrapping
			String nodeKeys = this.commandLine.getOptionValue("node.keys", "A4CN2+9CuPoCLxf8Hahacl4vWof8eePjZiAKrZgTiRHw");
	//		String nodeKeys = this.configuration.get("node.keys", "AihcVMYB7ndhmWCsfj0ll8U/CsUy9Kh/7Zb3J7g3dYv5,A4LUF3ravj4MwMtlYGc3+kiRDB7NcsB141xCgd8DhhBf");
	//		String nodeKeys = this.commandLine.getOptionValue("node.keys", "AihcVMYB7ndhmWCsfj0ll8U/CsUy9Kh/7Zb3J7g3dYv5,A4LUF3ravj4MwMtlYGc3+kiRDB7NcsB141xCgd8DhhBf,AtOM21m9f9DxaR7i2zpM1HNfzazSziwJv9smNsg9JHsO,A8h8Em/ml6X5I5amEMg/Mdz0PgcBwAI3gTUTTPCcjDyU");
			StringTokenizer nodeKeysTokenizer = new StringTokenizer(nodeKeys, ",");
			while (nodeKeysTokenizer.hasMoreTokens() == true)
			{
				String nodeKeyToken = nodeKeysTokenizer.nextToken();
				this.nodeKeys.add(BLSPublicKey.from(nodeKeyToken));
			}
			
			this.shardGroups = 1;
			this.numNodes = this.nodeKeys.size();
		}
	}

	public GenerateUniverses() throws Exception 
	{
		this(new String[] { "universe.key" });
	}

	public List<Universe> generateDeployments() throws Exception 
	{
		LOGGER.info("UNIVERSE KEY PRIVATE:  "+Bytes.toHexString(this.universeKey.getPrivateKey().toByteArray()));
		LOGGER.info("UNIVERSE KEY PUBLIC:   "+Bytes.toHexString(this.universeKey.getPublicKey().toByteArray()));

		List<Universe> universes = new ArrayList<>();

		long universeTimestampSeconds = Long.parseLong(this.commandLine.getOptionValue("timestamp", "1136073600"));
		long universeTimestampMillis = TimeUnit.SECONDS.toMillis(universeTimestampSeconds);

		universes.add(buildUniverse(10000, "Mainnet", "The public universe", Universe.Type.PRODUCTION, universeTimestampMillis, this.shardGroups, (int) TimeUnit.DAYS.toSeconds(1)));
		universes.add(buildUniverse(20000, "Testnet", "The test universe", Universe.Type.TEST, universeTimestampMillis, this.shardGroups, (int) TimeUnit.HOURS.toSeconds(1)));
		universes.add(buildUniverse(30000, "Devnet", "The development universe", Universe.Type.DEVELOPMENT, universeTimestampMillis, this.shardGroups, (int) TimeUnit.HOURS.toSeconds(1)));

		return universes;
	}

	private Universe buildUniverse(int port, String name, String description, Universe.Type type, long timestamp, int shardGroups, int epoch) throws Exception 
	{
		byte universeMagic = (byte) (Universe.computeMagic(this.universeKey.getPublicKey(), timestamp, shardGroups, epoch, port, type) & 0xFF);
		Block universeBlock = createGenesisBlock(universeMagic, timestamp);

		Universe universe = Universe.newBuilder()
			.port(port)
			.name(name)
			.description(description)
			.type(type)
			.timestamp(timestamp)
			.epoch(epoch)
			.shardGroups(shardGroups)
			.creator(this.universeKey.getPublicKey())
			.setGenesis(universeBlock)
			.setGenodes(this.nodeKeys)
			.build();
		universe.sign(this.universeKey);

		if (universe.verify(this.universeKey.getPublicKey()) == false)
			throw new ValidationException("Signature failed for " + name + " deployment");
		
		System.out.println(Serialization.getInstance().toJsonObject(universe, Output.WIRE).toString(4));
		byte[] deploymentBytes = Serialization.getInstance().toDson(universe, Output.WIRE);
		System.out.println("UNIVERSE - " + type + ": "+Bytes.toBase64String(deploymentBytes));

		return universe;
	}

	private Block createGenesisBlock(byte magic, long timestamp) throws Exception 
	{
		final TokenSpecification tokenSpecParticle = new TokenSpecification("FLEX", "Flexathon token", this.universeKey.getIdentity());
		tokenSpecParticle.sign(this.universeKey);
		final TokenParticle mintParticle = new TokenParticle(UInt256.from(UInt128.HIGH_BIT), tokenSpecParticle.getHash(), Action.MINT, Spin.UP, this.universeKey.getIdentity());
		mintParticle.sign(this.universeKey);
		final TokenParticle transferParticle = new TokenParticle(UInt256.from(UInt128.HIGH_BIT), tokenSpecParticle.getHash(), Action.TRANSFER, Spin.UP, this.universeKey.getIdentity());
		mintParticle.sign(this.universeKey);
		
		// TODo want to actually save this or something?
		final BLSKeyPair ephemeralValidator = new BLSKeyPair();
		final List<Atom> atoms = Collections.singletonList(new Atom(tokenSpecParticle, mintParticle, transferParticle));
		Block genesisBlock = new Block(0l, Hash.ZERO, ((Long.MAX_VALUE / 32) * 31), UInt256.ZERO, 0, timestamp, ephemeralValidator.getPublicKey(), 
									   atoms, Collections.emptyList(), Collections.emptyList());
		genesisBlock.getHeader().sign(ephemeralValidator);
		return genesisBlock;
	}

	public static void main(String[] arguments) throws Exception 
	{
		GenerateUniverses generateDeployments = new GenerateUniverses(arguments);
		generateDeployments.generateDeployments();
	}
}
