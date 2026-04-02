package edu.upenn.cis.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.experlog.zql.ParseException;
import com.experlog.zql.ZQuery;
import com.experlog.zql.ZqlParser;
import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple.TupleFactory;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.optimization.HashMapRelationTypes;
import edu.upenn.cis.orchestra.optimization.Location;
import edu.upenn.cis.orchestra.optimization.Optimizer;
import edu.upenn.cis.orchestra.optimization.P2PQPQueryPlanGenerator;
import edu.upenn.cis.orchestra.optimization.QpSchemaFactory;
import edu.upenn.cis.orchestra.optimization.Query;
import edu.upenn.cis.orchestra.optimization.RelationTypes;
import edu.upenn.cis.orchestra.optimization.Query.SyntaxError;
import edu.upenn.cis.orchestra.optimization.QueryPlanGenerator.CreatedQP;
import edu.upenn.cis.orchestra.optimization.Type.TypeError;
import edu.upenn.cis.orchestra.p2pqp.CombineCalibrations;
import edu.upenn.cis.orchestra.p2pqp.DHTService;
import edu.upenn.cis.orchestra.p2pqp.Id;
import edu.upenn.cis.orchestra.p2pqp.Null;
import edu.upenn.cis.orchestra.p2pqp.NullMetadataFactory;
import edu.upenn.cis.orchestra.p2pqp.QpApplication;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTuple;
import edu.upenn.cis.orchestra.p2pqp.QpTupleBag;
import edu.upenn.cis.orchestra.p2pqp.QpTupleKey;
import edu.upenn.cis.orchestra.p2pqp.QueryFailure;
import edu.upenn.cis.orchestra.p2pqp.QueryOwner;
import edu.upenn.cis.orchestra.p2pqp.Router;
import edu.upenn.cis.orchestra.p2pqp.ScratchFileGenerator;
import edu.upenn.cis.orchestra.p2pqp.SimpleScratchFileGenerator;
import edu.upenn.cis.orchestra.p2pqp.SimpleTableNameGenerator;
import edu.upenn.cis.orchestra.p2pqp.SocketManager;
import edu.upenn.cis.orchestra.p2pqp.TableNameGenerator;
import edu.upenn.cis.orchestra.p2pqp.TupleLoadingObserver;
import edu.upenn.cis.orchestra.p2pqp.DHTService.DHTException;
import edu.upenn.cis.orchestra.p2pqp.QpApplication.RecoveryMode;
import edu.upenn.cis.orchestra.p2pqp.TupleStore.TupleStoreException;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlan;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlanWithSchemas;
import edu.upenn.cis.orchestra.util.DomUtils;

public class TestHarness {
	static boolean showStats = false;

	private static Map<String,QpSchema> bioSchemas;

	public static void main(String args[]) throws IOException, SAXException, ClassNotFoundException {
		bioSchemas = Collections.unmodifiableMap(BioData.createQpSchemas(50));
		URL xmlLoc = TestHarness.class.getResource("info.xml");
		DOMConfigurator.configure(xmlLoc);
		Level level = Logger.getRootLogger().getLevel();

		int dbCacheSizeMb = 128;

		boolean writeable = false;
		boolean interactive = false;
		InetSocketAddress bootNode = null;
		InetAddress bindAddress = null, publicAddress = null;
		CombineCalibrations cc = null;
		int qpPort = 8000;
		File envDir = null;
		File rootDir = new File(".");

		TPCH tpch = null;
		STBenchmark stb = null;

		int replicationFactor = 1;

		boolean limitLocalNodes = false;

		Set<Inet4Address> localAddresses = new HashSet<Inet4Address>();

		Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
		while (interfaces.hasMoreElements()) {
			NetworkInterface iface = interfaces.nextElement();
			Enumeration<InetAddress> addresses = iface.getInetAddresses();
			while (addresses.hasMoreElements()) {
				InetAddress addr = addresses.nextElement();
				if (addr instanceof Inet4Address) {
					localAddresses.add((Inet4Address) addr);
				}
			}
		}

		List<Properties> props = new ArrayList<Properties>();
		for (int i = 0; i < args.length; ++i) {
			String arg = args[i];

			if (arg.equals("-l")) {
				limitLocalNodes = true;
			} else if (arg.equals("-w")) {
				writeable = true;
			} else if (arg.equals("-i")) {
				interactive = true;
			} else if (arg.equals("-r")) {
				rootDir = new File(args[++i]);
				if (! (rootDir.exists() && rootDir.isDirectory())) {
					System.err.println("Invalid root directory: " + rootDir);
					System.exit(-1);
				}
			} else if (arg.equals("-rf")) {
				String rfString = args[++i];
				try {
					replicationFactor = Integer.parseInt(rfString);
					if (replicationFactor < 0 || replicationFactor % 2 != 1) {
						System.err.println("Invalid replication factor (must be odd): " + rfString);
						System.exit(-1);
					}
				} catch (NumberFormatException ex) {
					System.err.println("Invalid replication factor: " + rfString);
					ex.printStackTrace();
					System.exit(-1);
				}
			} else if (arg.equals("-np") || arg.equalsIgnoreCase("-noPings")) {
				SocketManager.usePings = false;
			} else if (arg.equals("-mpt") || arg.equalsIgnoreCase("-numMessageProccesingThreads")) {
				QpApplication.numMessageProcessingThreads = Integer.parseInt(args[++i]);
			} else if (arg.equals("-qp")) {
				qpPort = Integer.parseInt(args[++i]);
			} else if (arg.equals("-numBuffers")) {
				SocketManager.DEFAULT_MAX_MEMORY_BUFFERS_PER_SOCKET = Integer.parseInt(args[++i]);
			} else if (arg.equals("-msgBufferSize")) {
				double kbFrac = Double.parseDouble(args[++i]);
				SocketManager.DEFAULT_MSG_BUFFER_SIZE = (int) Math.round(1024 * kbFrac);
			} else if (arg.equals("-maxDirectBuffers")) {
				SocketManager.MAX_DIRECT_BUFFERS = Integer.parseInt(args[++i]);
			} else if (arg.equals("-ba")) {
				String addr = args[++i];
				try {
					short s = Short.parseShort(addr);
					if (s < 0 || s > 255) {
						System.err.println("Bind address first byte must be in the range [0,255]");
						System.exit(-1);
					}
					byte b = (byte) s;
					List<InetAddress> matches = new ArrayList<InetAddress>();
					for (InetAddress ia : localAddresses) {
						byte[] addrBytes = ia.getAddress();
						if (addrBytes[0] == b) {
							matches.add(ia);
						}
					}
					if (matches.isEmpty()) {
						System.err.println("Bind address first byte " + s + " does not match any of the local addresses: " + localAddresses);
						System.exit(-1);
					} else if (matches.size() > 1) {
						System.err.println("Bind address first byte " + s + " matches multiple local addresses, please specify one of the following by name or IP address: " + localAddresses);
					} else {
						bindAddress = matches.get(0);
					}
				} catch (NumberFormatException nfe) {
					try {
						bindAddress = InetAddress.getByName(addr);
						if (! localAddresses.contains(bindAddress)) {
							System.err.println("Bind address " + bindAddress + " is not one of the local addresses " + localAddresses);
							System.exit(-1);
						}
					} catch (UnknownHostException uhe) {
						System.err.println("Could not resolve bind address:" + addr + "(" + uhe.getMessage() + ")");
						System.exit(-1);
					}
				}
			} else if (arg.equals("-bn")) {
				String bnString = args[++i];
				try {
					bootNode = getAddress(bnString);
				} catch (Exception ex) {
					System.err.println("Invalid boot node: " + bnString);
					ex.printStackTrace();
					System.exit(-1);
				}
			} else if (arg.equals("-public")) {
				String publicAddressStr = args[++i];
				try {
					publicAddress = InetAddress.getByName(publicAddressStr);
				} catch (UnknownHostException ex) {
					System.err.println("Invalid public IP address: " + publicAddressStr);
					ex.printStackTrace();
					System.exit(-1);
				}
			} else if (arg.equals("-env")) {
				envDir = new File(rootDir, args[++i]);
				if (! envDir.exists()) {
					System.err.println("Couldn't open environment directory: " + envDir);
					System.exit(-1);
				}
			} else if (arg.equals("-tpch")) {
				File tpchDir = new File(rootDir, args[++i]);
				if (! tpchDir.exists()) {
					System.err.println("Couldn't find TPCH directory: " + tpchDir);
					System.exit(-1);
				}
				tpch = new TPCH(tpchDir);
			} else if (arg.equals("-stbench")) {
				if (! new File(args[i+1]).exists()) {
					System.err.println("Couldn't find STBenchmark directory: " + args[i+1]);
					System.exit(-1);					
				}
				stb = new STBenchmark(args[++i]);
			} else if (arg.equals("-calib")) {
				File calibFile = new File(rootDir, args[++i]);
				if (! calibFile.exists()) {
					System.err.println("Could not find calibration file " + calibFile);
					System.exit(-1);
				}
				try {
					ObjectInputStream ois = new ObjectInputStream(new FileInputStream(calibFile));
					cc = (CombineCalibrations) ois.readObject();
					ois.close();
				} catch (Exception ex) {
					System.err.println("Could not read calibration file " + calibFile);
					ex.printStackTrace();
					System.exit(-1);
				}
			} else if (arg.equals("-cache")) {
				dbCacheSizeMb = Integer.parseInt(args[++i]);
			} else if (arg.equals("-pageSize")) {
				DHTService.numTuplesPerPage = Integer.parseInt(args[++i]);
			} else {
				File f = new File(rootDir, args[i]);
				try {
					Properties p = new Properties();
					p.load(new FileInputStream(f));
					props.add(p);
				} catch (FileNotFoundException ex) {
					System.err.println("Couldn't find node description file: " + f);
					System.exit(-1);
				} catch (IOException ex) {
					System.err.println("Error loading node description file " + f);
					ex.printStackTrace();
					System.exit(-1);
				}
			}
		}

		if (envDir == null) {
			System.err.println("Must specify environment directory");
			System.exit(-1);
		}

		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setReadOnly(! writeable);
		ec.setAllowCreate(writeable);
		ec.setCacheSize(((long)dbCacheSizeMb) * 1024 * 1024);
		ec.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
		ec.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");

		TableNameGenerator tng = new SimpleTableNameGenerator("tempTable");

		File scratchDir = new File(rootDir, "scratch");
		if (scratchDir.exists()) {
			for (File f : scratchDir.listFiles()) {
				f.delete();
			}
		} else {
			scratchDir.mkdir();
		}

		ScratchFileGenerator sfg = new SimpleScratchFileGenerator(scratchDir, "temp");

		List<TestHarness> nodes = new ArrayList<TestHarness>(props.size());
		Environment e;
		try {
			e = new Environment(envDir, ec);
		} catch (DatabaseException ex) {
			System.err.println("Error creating BerkeleyDB environment");
			ex.printStackTrace();
			System.exit(-1);
			return;
		}
		System.out.println("Opened BerkeleyDB environment");

		final DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
		final DocumentBuilder builder;
		try {
			builder = builderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException ex) {
			ex.printStackTrace();
			System.exit(-1);
			return;
		}

		if (bindAddress == null) {
			bindAddress = InetAddress.getLocalHost();
		}

		if (publicAddress == null) {
			publicAddress = bindAddress;
		}

		if (bootNode == null) {
			bootNode = new InetSocketAddress(publicAddress, qpPort);
		}

		System.out.println("Bind address: " + bindAddress);
		System.out.println("Public address: " + publicAddress);


		final Map<String,QpSchema> knownSchemas = new HashMap<String,QpSchema>();
		knownSchemas.putAll(TPCH.createSchemas(new CreateQpSchema(0,hashCols)));
		//		knownSchemas.putAll(TPCH.querySchemas);
		knownSchemas.putAll(bioSchemas);


		class RTAccess {
			private HashMapRelationTypes<Location,QpSchema> rt = new HashMapRelationTypes<Location,QpSchema>();
			boolean createdRT = false;
			private final TPCH tpch;
			private final STBenchmark stb;

			RTAccess(TPCH tpch, STBenchmark stb) {
				this.tpch = tpch;
				this.stb = stb;
			}

			RelationTypes<Location,QpSchema> getRT() throws IOException, ClassNotFoundException {
				if (! createdRT) {
					if (tpch != null) {
						tpch.createRT(knownSchemas, rt);
					}

					if (stb != null) {
						stb.createRT(knownSchemas, rt);
					}
					createdRT = true;
				}
				return rt;
			}
		}

		RTAccess rta = new RTAccess(tpch, stb);

		if (stb != null) {
			knownSchemas.putAll(stb.createSchemas(new CreateQpSchema(500, "ID")));
		}

		Set<InetSocketAddress> blacklist = new HashSet<InetSocketAddress>();

		try {
			int count = 0;
			for (Properties p : props) {
				System.out.println("Starting node #" + count);
				TestHarness th;
				try {
					th = new TestHarness(p, bootNode, bindAddress, publicAddress, qpPort + count, sfg, e, tng, count, cc, replicationFactor, limitLocalNodes, knownSchemas);
				} catch (Exception ex) {
					System.err.println("Error creating node #" + count);
					ex.printStackTrace();
					System.exit(-1);
					return;
				}
				nodes.add(th);
				++count;
			}


			QueryPlanWithSchemas<Null> plan = null;

			String[] commands = {"quit", "loadTPCH [blocksize]",
					"loadST [blocksize]",
					"plan filename [queryname]", "execute [numReps]",
					"select ...", "query", "optimize [tpch query name]", "log (off|info|debug|trace)",
					"stats (off|on)", "check [relationname]", "gc", "scans (sequential|parallel|numberThreadsPerScan)",
					"scanPriority [1-10]", "timeout [ms]", "probe [nodeno]",
					"router", "updateRouter [all]", "schemas", "batch (off|on)", "ownerWait delayTimeMs", "recovery (incremental|restart|off)",
					"killAfterStart nodeNo delayMs", "bioLoad prots species protsPerRef", "compression (0-9)|off|quick",
					"indexPages relationName", "hide (off|on)", "onlyCard (true|false)",
					"routerType [pastry|chord]", "pageLocation [original|withData]", "specialRouter (true|false)",
					//					"bufferConfig (off|(maxBufferSizeBytes|none) (maxBufferWaitMs|none))",
					"blacklist (clear|add node|remove node)"
			};

			String[] descs = {"", "load TPCH data",
					"load STBenchmark data",
					"load plan from file and set current query",
					"execute loaded plan and optionally check against known results",
					"execute supplied SQL query and show results",
					"execute prompted for SQL query and show results",
					"optimize TPCH query or prompted for SQL query and show plan",
					"set logging level",
					"show or hide BerkeleyDB stats after query execution",
					"check verify distributed storage using index",
					"run the Java garbage collector on all nodes", "set sequential or parallel scans",
					"set scan thread priority", "set query timeout",
					"probe for a specific tuple in local storage (at the first local node unless otherwise specified)",
					"show routing table", "recompute routing table (for first or all ndoes)", "show all base relation schemas",
					"set ship operator batching off or on",
					"set time to wait for query dissemination",
					"set recovery mode",
					"kill the specified node after a query starts", "load bio data", "set compression level [0=off,9=max]",
					"show index pages for the relation", "hide logging messages while not executing a query",
					"only show result cardinalities", "show or change the router type", "show or change index page location",
					"create special optimized recovery router",
					//"set buffering config",
			"update node blacklist"};

			File shouldExit = new File(rootDir, "shouldExit");
			shouldExit.delete();

			String currentQuery = null;

			Set<String> relevantSTschemas = null;

			if (interactive) {
				QpApplication.Configuration config = new QpApplication.Configuration();
				boolean hideWhileNotExecuting = true;
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				for ( ; ; ) {
					if (hideWhileNotExecuting) {
						Logger.getRootLogger().setLevel(Level.WARN);
					}
					System.err.flush();
					System.out.print("> ");
					System.out.flush();

					String line = br.readLine();
					if (line == null || line.equals("q") || line.equals("quit") || line.equals("exit")) {
						break;
					} else if (line.startsWith("onlyCard")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length == 2) {
							boolean onlyCardinality = Boolean.parseBoolean(tokens[1]);
							config = config.setDiscardResults(onlyCardinality);
						} else if (tokens.length != 1) {
							System.out.println("Usage: onlyCard (true|false)");
						}
						if (config.discardResults) {
							System.out.println("Only showing result cardinalities");
						} else {
							System.out.println("Showing results and cardinalities");
						}
					} else if (line.startsWith("loadTPCH")) {
						int blockSize = 500;
						String[] tokens = line.split("\\s+");
						if (tokens.length == 2) {
							try {
								blockSize = Integer.parseInt(tokens[1]);
							} catch (NumberFormatException nfe) {
								System.out.println("Usage: loadTPCH [blocksize]");
								continue;
							}
						} else if (tokens.length != 1) {
							System.out.println("Usage: loadTPCH [blocksize]");
							continue;
						}
						if (tpch == null) {
							System.out.println("TPCH directory was not specified");
						} else {
							System.out.println("Loading TPCH with block size " + blockSize + " and replication factor " + replicationFactor);
							try {
								nodes.get(0).load(tpch,blockSize, knownSchemas);
							} catch (DHTException ex) {
								ex.printStackTrace();
							}
						}
					} else if (line.startsWith("includeST")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length >= 2) {
							for (int i = 1; i < tokens.length; ++i) {
								if (tokens[i].equals("all")) {
									relevantSTschemas = null;
								} else {
									String schema = tokens[i].toUpperCase();
									if (knownSchemas.containsKey(schema)) {
										if (relevantSTschemas == null) {
											relevantSTschemas = new HashSet<String>();
										}
										relevantSTschemas.add(schema);
									} else {
										System.out.println("Schema " + schema + " is not known");
									}
								}
							}
						} else {
							System.out.println("includeST (all|tablename)");
						}
						if (relevantSTschemas == null) {
							System.out.println("Including all ST schemas");
						} else {
							System.out.println("Including only the following ST schemas: " + relevantSTschemas);
						}
					} else if (line.startsWith("loadST")) {
						int blockSize = 500;
						String[] tokens = line.split("\\s+");
						if (tokens.length == 2) {
							try {
								blockSize = Integer.parseInt(tokens[1]);
							} catch (NumberFormatException nfe) {
								System.out.println("Usage: loadST [blocksize]");
								continue;
							}
						} else if (tokens.length != 1) {
							System.out.println("Usage: loadST [blocksize]");
							continue;
						}
						if (stb == null) {
							System.out.println("STBenchmark directory was not specified");
						} else {
							System.out.println("Loading STBenchmark with block size " + blockSize + " and replication factor " + replicationFactor);
							try {
								nodes.get(0).load(stb, blockSize, knownSchemas, relevantSTschemas);
							} catch (Exception ex) {
								ex.printStackTrace();
							}
						}
					} else if (line.startsWith("timeout")) {
						int timeout = -1;
						String[] tokens = line.split("\\s+");
						if (tokens.length > 1) {
							try {
								timeout = Integer.parseInt(tokens[1]);
							} catch (NumberFormatException nfe) {
								System.out.println("Usage: timeout [ms]");
								continue;
							}
						}
						if (timeout <= 0) {
							timeout = 10000;
							System.out.println("Resetting timeout to " + timeout);
						}
						QueryOwner.timeout = timeout;
					} else if (line.startsWith("scanPriority")) {
						int priority = -1;
						String[] tokens = line.split("\\s+");
						if (tokens.length > 1) {
							try {
								priority = Integer.parseInt(tokens[1]);
							} catch (NumberFormatException nfe) {
								System.out.println("Usage: scanPriority [1-10]");
								continue;
							}
						}
						if (priority < Thread.MIN_PRIORITY || priority > Thread.MAX_PRIORITY) {
							System.out.println("Resetting scanPriority to " + Thread.NORM_PRIORITY);
							priority = Thread.NORM_PRIORITY;
						}
						config = config.changeScanPriority(priority);
					} else if (line.startsWith("plan")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length < 2 || tokens.length > 3) {
							System.err.println("plan planFile [queryName]");
						} else {
							final String filename = tokens[1];
							FileInputStream fis;
							try {
								fis = new FileInputStream(new File(rootDir, filename));
								Document document = builder.parse(fis);
								Element root = document.getDocumentElement();
								plan = QueryPlanWithSchemas.deserialize(root, knownSchemas, Null.class);
								fis.close();
							} catch (IOException ioe) {
								System.err.println("Couldn't read plan file " + filename);
								continue;
							} catch (Exception ex) {
								ex.printStackTrace();
								continue;
							}
							if (tokens.length == 3) {
								currentQuery = tokens[2];
							} else {
								currentQuery = null;
							}
						}
					} else if (line.toUpperCase().startsWith("SELECT")) {
						Logger.getRootLogger().setLevel(level);
						try {
							nodes.get(0).execute(line, rta.getRT(), config, blacklist);
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					} else if (line.startsWith("optimizeToFile")) {
						String[] tokens = line.split("\\s+");
						String query;
						String queryFile;
						String outFile;
						if (tokens.length == 2) {
							queryFile = null;
							outFile = tokens[1];
						} else if (tokens.length == 3) {
							queryFile = tokens[1];
							outFile = tokens[2];
						} else {
							outFile = null;
							queryFile = null;
							System.out.println("optimizeToFile [queryFile] planFile");
						}
						if (outFile != null) {
							if (queryFile == null) {
								System.out.println("Enter query: ");
								query = br.readLine();
							} else {
								StringBuilder sb = new StringBuilder();
								BufferedReader queryReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(rootDir, tokens[1]))));
								String queryLine = queryReader.readLine();
								while (queryLine != null) {
									sb.append(queryLine);
									sb.append('\n');
									queryLine = queryReader.readLine();
								}
								queryReader.close();
								query = sb.toString();
							}
							try {
								QueryPlanWithSchemas<Null> qpws = nodes.get(0).optimize(query,0,rta.getRT());
								Document doc = builder.newDocument();
								Element el = doc.createElement("QueryPlanWithSchemas");
								doc.appendChild(el);
								qpws.serialize(doc, el, knownSchemas);
								OutputStream out = new FileOutputStream(new File(rootDir, outFile));
								DomUtils.write(doc, out);
								out.close();
							} catch (Exception ex) {
								System.err.println("Error optimizing query: " + query);
								ex.printStackTrace();
							}
						}
					} else if (line.startsWith("optimize")) {
						String[] tokens = line.split("\\s+");
						String query;
						if (tokens.length == 1) {
							System.out.println("Enter query: ");
							query = br.readLine();
						} else {
							query = TPCH.queries.get(tokens[1]);
							if (query == null) {
								System.err.println("TPCH Query '" + tokens[1] + "' is not known");
							}
						}
						if (query != null) {
							try {
								QueryPlanWithSchemas<Null> qpws = nodes.get(0).optimize(query,0,rta.getRT());
								Document doc = builder.newDocument();
								Element el = doc.createElement("QueryPlanWithSchemas");
								doc.appendChild(el);
								qpws.serialize(doc, el, knownSchemas);
								DomUtils.write(doc, System.out);
							} catch (Exception ex) {
								ex.printStackTrace();
							}
						}
					} else if (line.startsWith("execute")) {
						if (plan == null) {
							System.err.println("Must specify plan first");
							continue;
						}
						String[] tokens = line.split("\\s+");
						int numExecs = 1;
						if (tokens.length == 2) {
							try {
								numExecs = Integer.parseInt(tokens[1]);
							} catch (Exception ex) {
								ex.printStackTrace();
								continue;
							}
						} else if (tokens.length > 2) {
							System.err.println("execute [numReps]");
							continue;
						}
						Set<QpTuple<?>> expected;
						if (currentQuery == null) {
							expected = null;
						} else if (tpch == null) {
							System.err.println("TPCH directory was not specified");
							expected = null;
						} else {
							try {
								expected = tpch.readTpchResults().get(currentQuery);
							} catch (IOException io) {
								System.err.println("Couldn't read TPCH results");
								io.printStackTrace();
								return;
							}
							if (expected == null) {
								System.err.println("Couldn't find results for query " + currentQuery);
								return;
							}
						}
						Logger.getRootLogger().setLevel(level);
						for (int i = 0; i < numExecs; ++i) {
							try {
								nodes.get(0).execute(plan,expected,config,blacklist);
							} catch (Exception ex) {
								ex.printStackTrace();
								break;
							}
						}
					} else if (line.startsWith("query")) {
						System.out.println("Enter query: ");
						try {
							nodes.get(0).execute(br.readLine(),rta.getRT(),config,blacklist);
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					} else if (line.startsWith("log")) {
						String[] tokens = line.split("\\s+");
						String arg = "";
						if (tokens.length == 2) {
							arg = tokens[1];
						}

						Class<?> c = TestHarness.class;
						URL configLoc = null;
						if (arg.equals("off")) {
							configLoc = c.getResource("off.xml");
							System.out.println("Logging off");
						} else if (arg.equals("info")) {
							configLoc = c.getResource("info.xml");
							System.out.println("Showing info-level logging");
						} else if (arg.equals("debug")) {
							configLoc = c.getResource("debug.xml");
							System.out.println("Showing debug-level logging");
						} else if (arg.equals("trace")) {
							configLoc = c.getResource("trace.xml");
							System.out.println("Showing debugging info and logging trace info to a file");
						} else {
							System.out.println("log (off|info|debug|trace)");
						}
						if (configLoc != null) {
							DOMConfigurator.configure(configLoc);
							level = Logger.getRootLogger().getLevel();
						}
					} else if (line.startsWith("stats")) {
						String[] tokens = line.split("\\s+");
						String arg = "";
						if (tokens.length == 2) {
							arg = tokens[1];
						}
						if (arg.equals("on")) {
							showStats = true;
						} else if (arg.equals("off")) {
							showStats = false;
						} else {
							System.out.println("stats (off|on)");
						}
					} else if (line.startsWith("check")) {
						Map<InetSocketAddress,Set<QpTupleKey>> missing = new HashMap<InetSocketAddress,Set<QpTupleKey>>();
						Collection<QpSchema> schemas = null;
						String[] tokens = line.split("\\s+");

						if (tokens.length == 2) {
							QpSchema schema = knownSchemas.get(tokens[1].toUpperCase());
							if (schema == null) {
								System.out.println("Unknown schema " + tokens[1].toUpperCase());
							} else {
								schemas = Collections.singleton(schema);
							}
						} else {
							schemas = knownSchemas.values();
						}
						if (schemas != null) {
							for (QpSchema schema : schemas) {
								if (schema.getLocation() != QpSchema.Location.STRIPED) {
									continue;
								}
								System.out.println("Checking " + schema.getName());
								Map<InetSocketAddress,Set<QpTupleKey>> missingFromRel = nodes.get(0).app.findMissingTuples(schema.getName(), 0); 
								if (! missingFromRel.isEmpty()) {
									for (Map.Entry<InetSocketAddress,Set<QpTupleKey>> me : missingFromRel.entrySet()) {
										if (me.getValue().isEmpty()) {
											continue;
										}
										Set<QpTupleKey> alreadyMissing = missing.get(me.getKey());
										if (alreadyMissing == null) {
											missing.put(me.getKey(), me.getValue());
										} else {
											alreadyMissing.addAll(me.getValue());
										}
									}
								}
							}
							if (missing.isEmpty()) {
								System.out.println("All nodes have all the tuples they should");
							} else {
								for (Map.Entry<InetSocketAddress, Set<QpTupleKey>> me : missing.entrySet()) {
									for (QpTupleKey t : me.getValue()) {
										System.out.println(me.getKey() + " is missing " + t + " (" + t.getQPid() + ")");
									}
								}
							}
						}
					} else if (line.startsWith("gc")) {
						System.out.print("Garbage collecting...");
						nodes.get(0).app.gc();
						System.out.println("done");
					} else if (line.startsWith("scans")) {
						String[] tokens = line.split("\\s+");
						String arg = null;
						if (tokens.length == 2) {
							arg = tokens[1];
						}

						if (arg == null) {
							if (config.multipleScanThreads) {
								System.out.println("Parallel scans are enabled with " + config.threadsPerDistributedScan + " threads per scan");
							} else {
								System.out.println("Sequential scans are enabled");
							}
						} else if (arg.equalsIgnoreCase("sequential")) {
							config = config.setSequentialScans();
							System.out.println("Set sequential scans");
						} else {
							int numThreads;
							try {
								numThreads = Integer.parseInt(arg); 
							} catch (NumberFormatException nfe) {
								numThreads = 1;
							}
							config = config.setMultipleScans(numThreads);
							System.out.println("Set parallel scans with " + numThreads + " threads per scan");
						}
					} else if (line.startsWith("schemas")) {
						Document doc = builder.newDocument();
						Element el = doc.createElement("KnownSchemas");
						doc.appendChild(el);

						for (QpSchema schema : knownSchemas.values()) {
							Element schemaEl = schema.serialize(doc);	
							el.appendChild(schemaEl);
						}
						DomUtils.write(doc, System.out);
					} else if (line.startsWith("probe")) {
						String[] tokens = line.split("\\s+");
						int node = -1;
						if (tokens.length == 1) {
							node = 0;
						} else {
							try {
								node = Integer.parseInt(args[1]);
							} catch (NumberFormatException nfe) {
								System.out.println("Invalid node " + tokens[1]);
							}
							if (node >= nodes.size()) {
								System.out.println("Invalid node " + node);
								node = -1;
							}
						}
						QpApplication<?> app = null;
						if (node >= 0) {
							app = nodes.get(node).app;
						}
						QpSchema schema = null;
						if (app != null) {
							System.out.print("Enter table name: ");
							System.out.flush();
							String table = br.readLine();
							schema = app.getStore().getSchema(table.toUpperCase());
							if (schema == null) {
								System.out.println("Unknown table name: " + table);
							}
						}
						if (schema != null) {
							Object[] fields = new Object[schema.getNumCols()];
							QpTupleKey key = null;
							if (key != null) {
								int numCols = schema.getNumCols();
								for (int i = 0; i < numCols; ++i) {
									if (! schema.getKeyColsSet().contains(i)) {
										continue;
									}
									Type t = schema.getColType(i);
									System.out.print("Enter a value for " + schema.getColName(i) + ": ");
									System.out.flush();
									String val = br.readLine();
									try {
										Object o = t.fromStringRep(val);
										fields[i]= o;
									} catch (Exception ex) {
										System.out.println("Invalid value for " + schema.getColName(i) + ": " + val);
										fields = null;
										break;
									}
								}
							}
							if (fields != null) {
								System.out.print("Enter epoch: ");
								System.out.flush();
								String epoch = br.readLine();
								try {
									int ep = Integer.parseInt(epoch);
									key = new QpTupleKey(schema, fields, ep);
								} catch (Exception ex) {
									System.out.println("Invalid epoch: " + epoch);
								}
							}
							if (key != null) {
								try {
									QpTuple<?> t = app.getStore().getTupleByKey(key);
									if (t == null) {
										System.out.println("No value found for " + key + " at epoch " + key.epoch);
									} else {
										System.out.println(t);
									}
								} catch (TupleStoreException tse) {
									tse.printStackTrace();
								}
							}
						}
					} else if (line.startsWith("updateRouter")) {
						boolean all = false;
						String[] tokens = line.split("\\s+");
						if (tokens.length >= 2 && tokens[1].equals("all")) {
							all = true;
						}
						try {
							if (all) {
								for (TestHarness th : nodes) {
									th.app.scheduleRoutingTableUpdate();
								}
								for (TestHarness th : nodes) {
									th.app.performRoutingTableUpdate();
								}
								System.out.println("Updated all routing tables");
							} else {
								nodes.get(0).app.performRoutingTableUpdate();
								System.out.println("Updated node 0 routing table");
							}
						} catch (Exception ex) {
							ex.printStackTrace();
						}
						for (int i = 0; i < (all ? nodes.size() : 1); ++i) {
							Router r = nodes.get(i).app.getRouter();
							System.out.println("Node " + i + " routing table (" + r.size() + " entries):\n" + r);
							System.out.println("Node " + i + " routing table contains " + r.size() + " entries");
						}
					} else if (line.startsWith("batch")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length != 2) {
							System.out.println("Usage: batch (shipWindowMs|off)");
							if (config.shipWindowMs <= 0) {
								System.out.println("Currently not batching ships");
							} else {
								System.out.println("Ship batching window is " + config.shipWindowMs + " msec");
							}
						} else {
							if (tokens[1].equalsIgnoreCase("off")) {
								config = config.setNoShipBatching();
								System.out.println("Set ship batching off");
							} else {
								try {
									int shipWindowMs = Integer.parseInt(tokens[1]);
									config = config.setShipBatching(shipWindowMs);
									System.out.println("Set ship batching to " + shipWindowMs + " ms");
								} catch (NumberFormatException nfe) {
									System.out.println("Usage: batch (shipWindowMs|off)");
								}
							}
						}
					} else if (line.startsWith("ownerWait")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length != 2) {
							System.out.println("Query dissemination time is " + QpApplication.QUERY_DISSEMINATION_TIME_MS + " ms");
							System.out.println("Usage: ownerWait delayTimeMs");
						} else {
							int delayTimeMs = Integer.parseInt(tokens[1]);
							QpApplication.QUERY_DISSEMINATION_TIME_MS = delayTimeMs;
						}
					} else if (line.startsWith("recovery")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length != 2) {
							System.out.println("Recovery mode is " + config.recoveryMode);
							System.out.println("Usage: recovery (restart|incremental|off)");
						} else {
							if (tokens[1].equalsIgnoreCase("on") || tokens[1].equalsIgnoreCase("incremental")) {
								config = config.setRecoveryMode(RecoveryMode.INCREMENTAL);
								System.out.println("Set recovery to " + config.recoveryMode);
							} else if (tokens[1].equals("off") || tokens[1].equalsIgnoreCase("abort")) {
								config = config.setRecoveryMode(RecoveryMode.ABORT);
								System.out.println("Set recovery to " + config.recoveryMode);
							} else if (tokens[1].equals("restart")) {
								config = config.setRecoveryMode(RecoveryMode.RESTART);
								System.out.println("Set recovery to " + config.recoveryMode);
							} else {
								System.out.println("Usage: recovery (on|off)");
							}
						}
					} else if (line.startsWith("killAfterStart")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length == 1) {
							for (int i = 0; i < nodes.size(); ++i) {
								TestHarness n = nodes.get(i);
								int delay = n.app.getDieDelayMs();
								if (delay >= 0) {
									System.out.println("Node #" + i + " will die " + delay + " ms after query starts");
								}
							}
						} else if (tokens.length != 3) {
							System.out.println("Usage: killAfterStart nodeNo delayMs");
						} else {
							int nodeNo, delayMs;
							try {
								nodeNo = Integer.parseInt(tokens[1]);
								delayMs = Integer.parseInt(tokens[2]);
								if (nodeNo < 0 || nodeNo >= nodes.size()) {
									System.out.println("nodeNo must be between 0 and " + (nodes.size() - 1));
								} else {
									nodes.get(nodeNo).app.dieAfterNextQuery(delayMs);
									System.out.println("Node #" + nodeNo + " will die " + delayMs + " ms after query starts");
								}
							} catch (NumberFormatException nfe) {
								System.out.println("Usage: killAfterStart nodeNo delayMs");
							}
						}
					} else if (line.startsWith("bioLoad")) {
						int blockSize = 500;
						int prots, species, protsPerRef;
						String[] tokens = line.split("\\s+");
						if (tokens.length == 4) {
							try {
								prots = Integer.parseInt(tokens[1]);
								species = Integer.parseInt(tokens[2]);
								protsPerRef = Integer.parseInt(tokens[3]);
							} catch (NumberFormatException nfe) {
								System.out.println("bioLoad prots species protsPerRef");
								continue;
							}
						} else {
							System.out.println("bioLoad prots species protsPerRef");
							continue;
						}
						try {
							BioData bd = new BioData(bioSchemas, prots, species, protsPerRef);
							nodes.get(0).load(bd.speciesSchema, bd.speciesIt, blockSize);
							nodes.get(0).load(bd.db1todb2Schema, bd.db1todb2It, blockSize);
							nodes.get(0).load(bd.db1Schema, bd.db1It, blockSize);
						} catch (DHTException ex) {
							ex.printStackTrace();
						}
					} else if (line.startsWith("compression")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length == 2) {
							int cLevel = Integer.MIN_VALUE;
							if (tokens[1].equals("off")) {
								cLevel = 0;
							} else if (tokens[1].equals("quick")) {
								cLevel = -1;
							} else {
								try {
									cLevel = Integer.parseInt(tokens[1]);
								} catch (NumberFormatException nfe) {
								}
							}
							if (cLevel < -1 || cLevel > 9) {
								System.out.println("compression (0-9)|off|quick");
							} else {
								config = config.setCompressionLevel(cLevel);
							}
						}
						String compressionLevel;
						if (config.compressionLevel == 0) {
							compressionLevel = "off";
						} else if (config.compressionLevel == -1) {
							compressionLevel = "quick";
						} else {
							compressionLevel = "ZIP(" + config.compressionLevel + ")";
						}
						System.out.println("Current compression level is: " + compressionLevel);
					} else if (line.startsWith("indexPages")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length != 2) {
							System.out.println("Usage: index relationName");
						} else {
							String relName = tokens[1];
							QpSchema rel = knownSchemas.get(relName.toUpperCase());
							int epoch = 0;
							if (rel == null) {
								System.out.println("Relation " + relName + " is unknown");
							} else {
								try {
									DHTService.RelationInfo ri = nodes.get(0).app.getRelationInfo(rel.relId, epoch);

									final int size = ri.pageIds.size();
									for (int i = 0; i < size; ++i) {
										System.out.println(ri.pages.get(i) + "\t" + ri.pageIds.get(i) + "\t" + ri.pageRanges.get(i));
									}
								} catch (Exception e1) {
									e1.printStackTrace();
								}
							}
						}
					} else if (line.startsWith("hide")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length != 2) {
							System.out.println("Usage: hide (off | on)");
						} else {
							if (tokens[1].equalsIgnoreCase("off")) {
								hideWhileNotExecuting = false;
								Logger.getRootLogger().setLevel(level);
							} else {
								hideWhileNotExecuting = true;
							}
						}
					} else if (line.startsWith("routerType")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length == 2) {
							if (tokens[1].equalsIgnoreCase("pastry")) {
								nodes.get(0).app.setRouterType(Router.Type.PASTRY);
							} else if (tokens[1].equalsIgnoreCase("chord")) {
								nodes.get(0).app.setRouterType(Router.Type.CHORD);
							} else if (tokens[1].equalsIgnoreCase("even")) {
								nodes.get(0).app.setRouterType(Router.Type.EVEN);
							} else {
								System.out.println("Unknown router type: " + tokens[1]);
							}
						} else if (tokens.length != 1) {
							System.out.println("Use: routerType (pastry|chord)");
						}
						System.out.println("Router type is: " + nodes.get(0).app.getRouterType());
					} else if (line.startsWith("router")) {
						boolean all = false;
						String[] tokens = line.split("\\s+");
						if (tokens.length >= 2 && tokens[1].equals("all")) {
							all = true;
						}
						for (int i = 0; i < (all ? nodes.size() : 1); ++i) {
							Router r = nodes.get(i).app.getRouter();
							System.out.println("Node " + i + " routing table (" + r.size() + " entries):\n" + r);
							System.out.println("Node " + i + " routing table contains " + r.size() + " entries");
						}
					} else if (line.startsWith("available")) {
						System.out.println("Available ranges:\n" + nodes.get(0).app.getRouter().getAvailableRanges());
					} else if (line.startsWith("pageLocation")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length >= 2 && tokens[1].equals("original")) {
							DHTService.locatePagesWithData = false;
						} else if (tokens.length >= 2 && tokens[1].equals("withData")) {
							DHTService.locatePagesWithData = true;
						}
						if (DHTService.locatePagesWithData) {	
							System.out.println("Locating pages with data");
						} else {
							System.out.println("Locating pages throughout the network");
						}
					} else if (line.startsWith("specialRouter")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length >= 2) {
							config = config.setCreateSpecialRecoveryRouter(Boolean.parseBoolean(tokens[1]));
						}
						System.out.println((config.createSpecialRecoveryRouter ? "Using " : "Not using ") + "special recovery router");
					} else if (line.startsWith("bufferConfig")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length == 2) {
							if (tokens[1].equals("off")) {
								config = config.setBufferReceivedTuples(false);
							} else {
								System.out.println("bufferConfig (off|(maxBufferSizeBytes|none) (maxBufferWaitMs|none))");
							}
						} else if (tokens.length == 3) {
							boolean failed = true;
							try {
								int maxBufferSizeBytes;
								if (tokens[1].equals("none")) {
									maxBufferSizeBytes = Integer.MAX_VALUE;
								} else {
									maxBufferSizeBytes = Integer.parseInt(tokens[1]);
								}
								int maxBufferWaitMs;
								if (tokens[2].equals("none")) {
									maxBufferWaitMs = -1;
								} else {
									maxBufferWaitMs = Integer.parseInt(tokens[2]);
								}
								if ((maxBufferSizeBytes > 0 && maxBufferSizeBytes != Integer.MAX_VALUE) || maxBufferWaitMs > 0) {
									config = config.setBufferReceivedTuples(true).setMaxBufferedLength(maxBufferSizeBytes).setMaxBufferedWaitMs(maxBufferWaitMs);
									failed = false;
								}
							} catch (Exception ex) {
								ex.printStackTrace();
							}
							if (failed) {
								System.out.println("bufferConfig (off|(maxBufferSizeBytes|none) (maxBufferWaitMs|none))");
							}
						} else {
							System.out.println("bufferConfig (off|(maxBufferSizeBytes|none) (maxBufferWaitMs|none))");
						}
						if (config.bufferReceivedTuples) {
							System.out.println("Buffer size (per stream): " + config.maxBufferedLength + " bytes, max wait time: " + config.maxBufferedWaitMs + " msec");
						} else {
							System.out.println("Not buffering received tuples");
						}
					} else if (line.startsWith("blacklist")) {
						String[] tokens = line.split("\\s+");
						if (tokens.length == 1) {
							System.out.println("Blacklist is : " + blacklist);
						} else if (tokens.length == 2) {
							if (tokens[1].equals("clear")) {
								blacklist.clear();
								System.out.println("Blacklist cleared");
							} else {
								System.out.println("Usage: blacklist [clear|add node|remove node]");
							}
						} else if (tokens.length == 3) {
							try {
								InetSocketAddress isa = getAddress(tokens[2]);
								if (tokens[1].equals("add")) {
									if (blacklist.add(isa)) {
										System.out.println(isa + " added to blacklist");
									} else {
										System.out.println(isa + " was already on blacklist");
									}
									System.out.println("Blacklist is : " + blacklist);
								} else if (tokens[1].equals("remove")) {
									if (blacklist.remove(isa)) {
										System.out.println(isa + " removed from blacklist");
									} else {
										System.out.println(isa + " was not on blacklist");
									}
									System.out.println("Blacklist is : " + blacklist);
								} else {
									System.out.println("Usage: blacklist [clear|add node|remove node]");
								}
							} catch (Exception ex) {
								ex.printStackTrace();
							}
						}
					} else {
						int maxLength = 0;
						for (String s : commands) {
							if (s.length() > maxLength) {
								maxLength = s.length();
							}
						}
						String format = "%-" + (maxLength + 1) + "s%s\n";
						System.out.println("Commands:");
						for (int i = 0; i < commands.length; ++i) {
							System.out.format(format, commands[i], descs[i]);
						}
					}
				}
			} else {
				while (! shouldExit.exists()) {
					Thread.sleep(500);
				}
				shouldExit.delete();
			}
		} catch (InterruptedException ex) {
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			System.out.print("Closing nodes...");
			System.out.flush();
			try {
				for (TestHarness th : nodes) {
					th.app.stop();
				}
			} catch (Exception ex2) {
				ex2.printStackTrace();
			}

			try {
				Thread.sleep(1000);
			} catch (InterruptedException ex) {
			}
			System.out.println("done");

			try {
				if (writeable) {
					System.out.print("Cleaning BerkeleyDB log files and checkpointing...");
					System.out.flush();
					boolean anyCleaned = false;
					while (e.cleanLog() > 0) {
						anyCleaned = true;
					}
					if (anyCleaned) {
						CheckpointConfig force = new CheckpointConfig();
						force.setForce(true);
						e.checkpoint(force);
					}
					System.out.println("done");
				}
				e.close();
			} catch (DatabaseException de) {
				de.printStackTrace();
			}
			sfg.cleanup();

			Enumeration<?> appenders = Logger.getRootLogger().getAllAppenders();
			while (appenders.hasMoreElements()) {
				((Appender) appenders.nextElement()).close();
			}
		}
	}

	private final Map<String,InetSocketAddress> namedNodes;
	private final Set<String> localNames;
	private final QpApplication<Null> app;
	private final CombineCalibrations cc;

	private TestHarness(Properties p, InetSocketAddress bootAddr, InetAddress bindAddress, InetAddress publicAddress, int qpPort, ScratchFileGenerator sfg, Environment e, TableNameGenerator tng, int count, CombineCalibrations cc, int replicationFactor, boolean limitLocalConnections, Map<String,QpSchema> knownSchemas) throws Exception {
		String localNames = p.getProperty("localNames");
		if (localNames == null) {
			this.localNames = Collections.emptySet();
		} else {
			Set<String> names = new HashSet<String>();
			String namesList[] = localNames.split("\\|");
			names.addAll(Arrays.asList(namesList));
			names.remove("");
			this.localNames = Collections.unmodifiableSet(names);
		}
		this.cc = cc;
		Id nodeId;
		String namedNodes = p.getProperty("namedNodes");
		if (namedNodes == null) {
			this.namedNodes = Collections.emptyMap();
		} else {
			String[] entries = namedNodes.split("\\|");
			Map<String,InetSocketAddress> names = new HashMap<String,InetSocketAddress>(entries.length); 
			for (String entry : entries) {
				if (entry.length() == 0) {
					continue;
				}
				String[] parts = entry.split("@");
				if (parts.length != 2) {
					throw new RuntimeException("Named nodes format: node1@host:port|node2@host:port...");
				}
				InetSocketAddress addr = getAddress(parts[1]);
				names.put(parts[0], addr);
			}
			this.namedNodes = Collections.unmodifiableMap(names);
		}
		String nodeIdStr = p.getProperty("nodeId");
		if (nodeIdStr == null) {
			nodeId = null;
		} else {
			nodeId = Id.fromString(nodeIdStr);
		}
		if (bindAddress == null) {
			bindAddress = InetAddress.getLocalHost();
		}
		InetSocketAddress bindPort = new InetSocketAddress(bindAddress, qpPort);
		InetSocketAddress publicPort = new InetSocketAddress(publicAddress, qpPort);


		app = new QpApplication<Null>(bindPort, publicPort, nodeId, bootAddr, sfg, e, "store" + count, "index" + count, tng, NullMetadataFactory.getInstance(), this.namedNodes, replicationFactor);
		for (String name : this.localNames) {
			app.addLocalName(name);
		}

		for (QpSchema s : knownSchemas.values()) {
			app.addTable(s, 0);
		}

		System.out.println("Created TestHarness node on port " + qpPort);

	}

	private Optimizer<Location,QueryPlan<Null>,Double,QpSchema> getOptimizer(RelationTypes<Location,QpSchema> rt) throws IOException, ClassNotFoundException {
		int numNodes = app.getParticipants().size();
		P2PQPQueryPlanGenerator<QpSchema,Null> qpg = new P2PQPQueryPlanGenerator<QpSchema,Null>(numNodes, cc);
		Location.Factory lf = new Location.Factory(rt);
		return new Optimizer<Location,QueryPlan<Null>,Double,QpSchema>(1,true,rt,qpg,lf);		
	}

	private static InetSocketAddress getAddress(String addr) throws UnknownHostException, NumberFormatException {
		String[] addrParts = addr.split(":");
		if (addrParts.length != 2) {
			throw new IllegalArgumentException("Node format: host.name:port");
		}
		int bootPort = Integer.parseInt(addrParts[1]);
		InetAddress host = InetAddress.getByName(addrParts[0]);
		return new InetSocketAddress(host, bootPort);
	}

	public static final Map<String,Set<String>> hashCols;

	static {
		Map<String,Set<String>> temp = new HashMap<String,Set<String>>();
		temp.put("PART", Collections.singleton("P_PARTKEY"));
		temp.put("SUPPLIER", Collections.singleton("S_SUPPKEY"));
		temp.put("PARTSUPP", Collections.singleton("PS_PARTKEY"));
		temp.put("CUSTOMER", Collections.singleton("C_CUSTKEY"));
		temp.put("SUPPLIER", Collections.singleton("S_SUPPKEY"));
		temp.put("ORDERS", Collections.singleton("O_ORDERKEY"));
		temp.put("LINEITEM", Collections.singleton("L_ORDERKEY"));
		// Nation and Region are replicated
		hashCols = Collections.unmodifiableMap(temp);
	}

	private void load(final QpSchema schema, Iterator<QpTuple<Null>> tuples, int sendBlockSize) throws IOException, InterruptedException, DHTException {
		if (schema.getLocation() == QpSchema.Location.REPLICATED) {
			QpTupleBag<Null> all = new QpTupleBag<Null>(schema, null, null);
			while (tuples.hasNext()) {
				all.add(tuples.next());
			}
			app.publishReplicatedRelation(schema.relId,all);
			System.err.println("Published replicated relation " + schema.getName());
			return;
		}
		TupleLoadingObserver tlo = new TupleLoadingObserver() {
			@Override
			public int getTupleCountGranularity() {
				return 100000;
			}

			@Override
			public void loadedTupleCountIs(int count) {
				System.err.println("Loaded " + count + " " + schema.getName() + " tuples");
			}

			@Override
			public void sentTupleCountIs(int count, int total) {
				System.err.println("Sent " + count + " of " + total + " " + schema.getName() + " tuples");
			}

			@Override
			public int getIndexPageGranularity() {
				return 10;
			}

			@Override
			public void processedIndexPages(int currCount,
					int estimatedTotalCount) {
				System.err.println("Sent " + currCount + " of " + estimatedTotalCount + " index pages for " + schema.getName());
			}
		};
		app.getDHT().setSendBlockSize(sendBlockSize);
		app.getDHT().addTuples(schema.relId, tuples, 0, tlo);
		System.err.println("Loaded all " + schema.getName() + " tuples");
		app.getDHT().finishEpochForRelation(schema.getName(), tlo);
		System.err.println("Updated index for relation " + schema.getName());

	}

	private void load(TPCH tpch, final int sendBlockSize, final Map<String,QpSchema> schemas) throws IOException, DHTException, InterruptedException {
		long startTime = System.currentTimeMillis();

		for (String relation : TPCH.tableNames) {
			// Note that this TupleFactory recycles the QpTuple object!
			TupleFactory<QpSchema,QpTuple<Null>> tf = new TupleFactory<QpSchema, QpTuple<Null>>() {
				private QpTuple<Null> t = null;
				public QpTuple<Null> createTuple(String relationName, Object... fields)
				throws ValueMismatchException {
					if (t == null) {
						QpSchema s = schemas.get(relationName);
						t = new QpTuple<Null>(s, fields);						
					} else {
						t.changeFields(fields);
					}
					return t;
				}

				public QpSchema getSchema(String relationName) {
					return schemas.get(relationName);
				}

			};
			Iterator<QpTuple<Null>> tuples = tpch.readTuples(relation, tf);
			load(schemas.get(relation), tuples, sendBlockSize);
		}

		long endTime = System.currentTimeMillis();
		double loadTime = (endTime - startTime) / 1000.0;
		System.out.println("Loading took " + loadTime + " seconds");
	}

	private static class SimpleLoadingObserver implements TupleLoadingObserver {

		private final String relation;

		SimpleLoadingObserver(String relation) {
			this.relation = relation;
		}

		@Override
		public int getTupleCountGranularity() {
			return 100000;
		}

		@Override
		public void loadedTupleCountIs(int count) {
			System.err.println("Loaded " + count + " " + relation + " tuples");
		}

		@Override
		public void sentTupleCountIs(int count, int total) {
			System.err.println("Sent " + count + " of " + total + " " + relation + " tuples");
		}

		@Override
		public int getIndexPageGranularity() {
			return 10;
		}

		@Override
		public void processedIndexPages(int currCount,
				int estimatedTotalCount) {
			System.err.println("Sent " + currCount + " of " + estimatedTotalCount + " index pages for " + relation);
		}

	}

	private void load(STBenchmark stb, final int sendBlockSize, final Map<String,QpSchema> schemas, final Set<String> relevantSTschemas)
	throws SAXException, IOException, DHTException, InterruptedException {
		app.getDHT().setSendBlockSize(sendBlockSize);
		long startTime = System.currentTimeMillis();

		// Note that this TupleFactory recycles the QpTuple object!
		TupleFactory<QpSchema,QpTuple<Null>> tf = new TupleFactory<QpSchema, QpTuple<Null>>() {
			private QpTuple<Null> t = null;
			public QpTuple<Null> createTuple(String relationName, Object... fields)
			throws ValueMismatchException {
				QpSchema s = schemas.get(relationName);
				if (t == null || (! t.getSchema().quickEquals(s))) {
					t = new QpTuple<Null>(s, fields);						
				} else {
					t.changeFields(fields);
				}
				return t;
			}

			public QpSchema getSchema(String relationName) {
				return schemas.get(relationName);
			}

		};

		class Sink implements STBenchmark.TupleSink<QpTuple<Null>> {
			private QpTupleBag<Null> tuples = null;
			private QpTupleBag<Null> corrTuples = null;
			@Override
			public void put(QpTuple<Null> tuple, String origSchema) throws SAXException {
				if (origSchema == null) {
					if (relevantSTschemas != null && (! relevantSTschemas.contains(tuple.getSchema().getName()))) {
						return;
					}
					if (tuples != null && (! tuples.schema.quickEquals(tuple.getSchema()))) {
						try {
							push();
							finish();
						} catch (Exception e) {
							throw new SAXException(e);
						}
					}
					if (tuples == null) {
						final QpSchema schema = tuple.getSchema();
						tuples = new QpTupleBag<Null>(schema, null, null);
					}
					tuples.add(tuple);
					if (tuples.size() >= sendBlockSize) {
						try {
							push();
						} catch (Exception e) {
							throw new SAXException(e);
						}			
					}
				} else {
					if (relevantSTschemas != null && (! relevantSTschemas.contains(origSchema))) {
						return;
					}					
					if (corrTuples != null && (! corrTuples.schema.quickEquals(tuple.getSchema()))) {
						try {
							push();
							finish();
						} catch (Exception e) {
							throw new SAXException(e);
						}
					}
					if (corrTuples == null) {
						final QpSchema schema = tuple.getSchema();
						corrTuples = new QpTupleBag<Null>(schema, null, null);
					}
					corrTuples.add(tuple);
					if (corrTuples.size() >= sendBlockSize) {
						try {
							push();
						} catch (Exception e) {
							throw new SAXException(e);
						}			
					}
				}
			}

			void push() throws DHTException, IOException {
				if (tuples != null && (! tuples.isEmpty())) {
					app.getDHT().addTuples(tuples.schema.relId, tuples.recyclingIterator(), 0, new SimpleLoadingObserver(tuples.schema.getName()));
					tuples.clear();
				}
				if (corrTuples != null && (! corrTuples.isEmpty())) {
					app.getDHT().addTuples(corrTuples.schema.relId, corrTuples.recyclingIterator(), 0, new SimpleLoadingObserver(corrTuples.schema.getName()));
					corrTuples.clear();
				}

			}

			void finish() throws DHTException, InterruptedException, IOException {
				if (tuples != null) {
					push();
					app.getDHT().finishEpochForRelation(tuples.schema.getName(), new SimpleLoadingObserver(tuples.schema.getName()));
					tuples = null;
				}
				if (corrTuples != null) {
					push();
					app.getDHT().finishEpochForRelation(corrTuples.schema.getName(), new SimpleLoadingObserver(corrTuples.schema.getName()));
					corrTuples = null;
				}
			}
		};

		Sink s = new Sink();

		stb.load(tf, s);
		s.finish();


		long endTime = System.currentTimeMillis();
		double loadTime = (endTime - startTime) / 1000.0;
		System.out.println("Loading took " + loadTime + " seconds");
	}
	static int queryId = 100;

	private QueryPlanWithSchemas<Null> optimize(String query, int queryId, RelationTypes<Location,QpSchema> rt) throws TypeError, SyntaxError, ParseException, InterruptedException, IOException, ClassNotFoundException {
		Optimizer<Location,QueryPlan<Null>,Double,QpSchema> optimizer = getOptimizer(rt);
		if (optimizer == null) {
			return null;
		}
		Query q = getQuery(query, rt);

		QpSchemaFactory qsf = new QpSchemaFactory(queryId);
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, Location.CENTRALIZED, qsf);
		return new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), cqp.cost, Null.class);
	}

	static ZqlParser parser = new ZqlParser();
	private static Query getQuery(String SQL, RelationTypes<?,?> rt) throws ParseException, TypeError, SyntaxError {
		if (! SQL.endsWith(";")) {
			SQL = SQL + ";";
		}
		parser.initParser(new StringReader(SQL));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, rt);
		return q;
	}


	private void execute(String query, RelationTypes<Location,QpSchema> rt, QpApplication.Configuration config, Set<InetSocketAddress> blacklist) throws Exception {
		int queryId = ++TestHarness.queryId;
		QueryPlanWithSchemas<Null> qpws = null;
		try {
			qpws = optimize(query,queryId, rt);
		} catch (SyntaxError se) {
			se.printStackTrace();
			return;
		}
		if (qpws == null) {
			System.out.println("Optimization is not supported because no calibration information was supplied");
			return;
		}

		long startTime = System.nanoTime();
		List<QpTuple<Null>> results = null;
		int card = -1;
		app.beginQuery(queryId, 0, qpws, config, blacklist);
		if (config.discardResults) {
			card = app.getQueryResultCardinality(queryId);
		} else {
			results = app.getQueryResultWithoutMetadata(queryId);
		}
		long endTime = System.nanoTime();
		app.endQuery(queryId);

		if (results == null) {
			System.out.println("Result size: " + card + " tuples");			
		} else {
			if (results.size() < 500) {
				for (QpTuple<?> t : results) {
					System.out.println(t);
				}
			}
			System.out.println("Result size: " + results.size() + " tuples");
		}
		System.out.println("Execution time: " + ((endTime - startTime) / 1.0e9));
		System.out.println("Result size: " + results.size() + " tuples");
		if (showStats) {
			app.printStoreStats(System.out);
		}
		System.out.print("Garbage collecting...");
		this.app.gc();
		System.out.println("done");
	}

	private void execute(QueryPlanWithSchemas<Null> plan, Set<QpTuple<?>> expected, QpApplication.Configuration config, Set<InetSocketAddress> blacklist) throws Exception {
		int queryId = ++TestHarness.queryId;
		List<QpTuple<Null>> results = null;
		int card = -1;
		long startTime = System.nanoTime();
		app.beginQuery(queryId, 0, plan, config, blacklist);
		try {
			if (config.discardResults) {
				card = app.getQueryResultCardinality(queryId);
			} else {
				results = app.getQueryResultWithoutMetadata(queryId);
			}
		} catch (QueryFailure qf) {
			long endTime = System.nanoTime();
			System.out.println("Execution time: " + ((endTime - startTime) / 1.0e9) + " seconds");
			long totalSentData = app.endQuery(queryId);
			System.out.println("Total sent data: "+ totalSentData + " bytes");
			qf.printStackTrace();
			return;
		} catch (Exception e) {
			System.err.println("Caught unexpected exception");
			e.printStackTrace();
			app.endQuery(queryId);
			return;
		}
		long endTime = System.nanoTime();
		long totalSentData = app.endQuery(queryId);

		if (results == null) {
			System.out.println("Result size: " + card + " tuples");
		} else {
			if (expected == null) {
				if (results.size() < 500) {
					for (QpTuple<?> t : results) {
						System.out.println(t);
					}
				}
			} else {
				Set<QpTuple<Null>> resultsSet = new HashSet<QpTuple<Null>>(results);
				if (resultsSet.equals(expected)) {
					System.out.println("Results match expected");
				} else {
					for (QpTuple<?> t : expected) {
						if (! resultsSet.contains(t)) {
							System.out.println("-" + t);
						}
					}
					for (QpTuple<?> t : results) {
						if (! expected.contains(t)) {
							System.out.println("+" + t);
						}
					}
				}
			}
			System.out.println("Result size: " + results.size() + " tuples");
		}

		System.out.println("Execution time: " + ((endTime - startTime) / 1.0e9) + " seconds");
		System.out.println("Total sent data: "+ totalSentData + " bytes");

		if (showStats) {
			app.printStoreStats(System.out);
		}
		System.out.print("Garbage collecting...");
		this.app.gc();
		System.out.println("done");
	}
}
