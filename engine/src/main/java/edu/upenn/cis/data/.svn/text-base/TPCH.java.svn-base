package edu.upenn.cis.data;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.experlog.zql.ParseException;
import com.experlog.zql.ZQuery;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.DateType;
import edu.upenn.cis.orchestra.datamodel.DoubleType;
import edu.upenn.cis.orchestra.datamodel.ForeignKey;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple.LabeledNull;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple.TupleFactory;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.optimization.HashMapRelationTypes;
import edu.upenn.cis.orchestra.optimization.Histogram;
import edu.upenn.cis.orchestra.optimization.Location;
import edu.upenn.cis.orchestra.optimization.Optimizer;
import edu.upenn.cis.orchestra.optimization.P2PQPQueryPlanGenerator;
import edu.upenn.cis.orchestra.optimization.QpSchemaFactory;
import edu.upenn.cis.orchestra.optimization.Query;
import edu.upenn.cis.orchestra.optimization.RelationTypes;
import edu.upenn.cis.orchestra.optimization.Variable;
import edu.upenn.cis.orchestra.optimization.VariablePosition;
import edu.upenn.cis.orchestra.optimization.Query.SyntaxError;
import edu.upenn.cis.orchestra.optimization.QueryPlanGenerator.CreatedQP;
import edu.upenn.cis.orchestra.optimization.Type.TypeError;
import edu.upenn.cis.orchestra.p2pqp.CombineCalibrations;
import edu.upenn.cis.orchestra.p2pqp.Null;
import edu.upenn.cis.orchestra.p2pqp.QpMutableTuple;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTuple;
import edu.upenn.cis.orchestra.p2pqp.SimpleTableNameGenerator;
import edu.upenn.cis.orchestra.p2pqp.TableNameGenerator;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlan;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlanWithSchemas;
import edu.upenn.cis.orchestra.util.DomUtils;

public class TPCH {
	static final TupleFactory<QpSchema,QpTuple<Null>> tf = new TupleFactory<QpSchema,QpTuple<Null>>() {
		public QpSchema getSchema(String relationName) {
			return simpleQpSchemas.get(relationName);
		}

		public QpTuple<Null> createTuple(String relationName, Object... fields)
		throws ValueMismatchException {
			return new QpTuple<Null>(simpleQpSchemas.get(relationName), fields);
		}
	};

	static final TupleFactory<QpSchema,QpMutableTuple<Null>> mutableTf = new TupleFactory<QpSchema,QpMutableTuple<Null>>() {

		public QpMutableTuple<Null> createTuple(String relationName,
				Object... fields) throws ValueMismatchException {
			QpMutableTuple<Null> t =  new QpMutableTuple<Null>(simpleQpSchemas.get(relationName));
			for (int i = 0; i < fields.length; ++i) {
				Object o = fields[i];
				if (o instanceof LabeledNull) {
					t.setLabeledNull(i, ((LabeledNull) o).getLabel());
				} else {
					t.set(i, o);
				}
			}
			t.setReadOnly();
			return t;
		}

		public QpSchema getSchema(String relationName) {
			return simpleQpSchemas.get(relationName);
		}

	};

	public static final boolean isTpchTable(String tableName) {
		return tableNames.contains(tableName);
	}
	
	public static final List<String> tableNames = Collections.unmodifiableList(Arrays.asList(
			"PART", "REGION", "NATION", "CUSTOMER", "SUPPLIER", "PARTSUPP",
			"ORDERS", "LINEITEM"));

	public static <S extends AbstractRelation> Map<String,S> createSchemas(CreateSchema<S> cs) {
		try {
			HashMap<String,S> retval = new HashMap<String,S>(8);

			S PART = cs.createSchema("PART");
			PART.addCol("P_PARTKEY", new IntType(false, false));
			PART.addCol("P_NAME", new StringType(false, false, true, 55));
			PART.addCol("P_MFGR", new StringType(false, false, false, 25));
			PART.addCol("P_BRAND", new StringType(false, false, false, 10));
			PART.addCol("P_TYPE", new StringType(false, false, true, 25));
			PART.addCol("P_SIZE", new IntType(false,false));
			PART.addCol("P_CONTAINER", new StringType(false, false, false, 10));
			PART.addCol("P_RETAILPRICE", new DoubleType(false, false));
			PART.addCol("P_COMMENT", new StringType(false, false, true, 23));
			PART.setPrimaryKey(Collections.singleton("P_PARTKEY"));
			cs.finalize(PART);
			retval.put(PART.getName(), PART);

			S REGION = cs.createSchema("REGION");
			REGION.addCol("R_REGIONKEY", new IntType(false, false));
			REGION.addCol("R_NAME", new StringType(false, false, false, 25));
			REGION.addCol("R_COMMENT", new StringType(false, false, true, 152));
			REGION.setPrimaryKey(Collections.singleton("R_REGIONKEY"));
			cs.finalize(REGION);
			retval.put(REGION.getName(), REGION);

			S NATION = cs.createSchema("NATION");
			NATION.addCol("N_NATIONKEY", new IntType(false, false));
			NATION.addCol("N_NAME", new StringType(false, false,false,25));
			NATION.addCol("N_REGIONKEY", new IntType(false, false));
			NATION.addCol("N_COMMENT", new StringType(false, false,true,152));
			NATION.setPrimaryKey(Collections.singleton("N_NATIONKEY"));
			NATION.addForeignKey(new ForeignKey("N_fk", NATION, Collections.singletonList("N_REGIONKEY"), REGION, Collections.singletonList("R_REGIONKEY")));
			cs.finalize(NATION);
			retval.put(NATION.getName(), NATION);

			S CUSTOMER = cs.createSchema("CUSTOMER");
			CUSTOMER.addCol("C_CUSTKEY", new IntType(false, false));
			CUSTOMER.addCol("C_NAME", new StringType(false, false,true,25));
			CUSTOMER.addCol("C_ADDRESS", new StringType(false, false,true,40));
			CUSTOMER.addCol("C_NATIONKEY", new IntType(false, false));
			CUSTOMER.addCol("C_PHONE", new StringType(false, false,false,15));
			CUSTOMER.addCol("C_ACCTBAL", new DoubleType(false, false));
			CUSTOMER.addCol("C_MKTSEGMENT", new StringType(false, false,false,10));
			CUSTOMER.addCol("C_COMMENT", new StringType(false, false,true,117));
			CUSTOMER.setPrimaryKey(Collections.singleton("C_CUSTKEY"));
			CUSTOMER.addForeignKey(new ForeignKey("C_fk", CUSTOMER, Collections.singletonList("C_NATIONKEY"), NATION, Collections.singletonList("N_NATIONKEY")));
			cs.finalize(CUSTOMER);
			retval.put(CUSTOMER.getName(), CUSTOMER);

			S SUPPLIER = cs.createSchema("SUPPLIER");
			SUPPLIER.addCol("S_SUPPKEY", new IntType(false, false));
			SUPPLIER.addCol("S_NAME", new StringType(false, false,true,25));
			SUPPLIER.addCol("S_ADDRESS", new StringType(false, false,true,40));
			SUPPLIER.addCol("S_NATIONKEY", new IntType(false, false));
			SUPPLIER.addCol("S_PHONE", new StringType(false, false,false,15));
			SUPPLIER.addCol("S_ACCTBAL", new DoubleType(false, false));
			SUPPLIER.addCol("S_COMMENT", new StringType(false, false,true,101));
			SUPPLIER.setPrimaryKey(Collections.singleton("S_SUPPKEY"));
			SUPPLIER.addForeignKey(new ForeignKey("S_fk", SUPPLIER, Collections.singletonList("S_NATIONKEY"), NATION, Collections.singletonList("N_NATIONKEY")));
			cs.finalize(SUPPLIER);
			retval.put(SUPPLIER.getName(), SUPPLIER);

			S PARTSUPP = cs.createSchema("PARTSUPP");
			PARTSUPP.addCol("PS_PARTKEY", new IntType(false, false));
			PARTSUPP.addCol("PS_SUPPKEY", new IntType(false, false));
			PARTSUPP.addCol("PS_AVAILQTY", new IntType(false, false));
			PARTSUPP.addCol("PS_SUPPLYCOST", new DoubleType(false, false));
			PARTSUPP.addCol("PS_COMMENT", new StringType(false, false,true,199));
			PARTSUPP.setPrimaryKey(Arrays.asList("PS_PARTKEY", "PS_SUPPKEY"));
			PARTSUPP.addForeignKey(new ForeignKey("PS_fk1", PARTSUPP, Collections.singletonList("PS_PARTKEY"), PART, Collections.singletonList("P_PARTKEY")));
			PARTSUPP.addForeignKey(new ForeignKey("PS_fk2", PARTSUPP, Collections.singletonList("PS_SUPPKEY"), SUPPLIER, Collections.singletonList("S_SUPPKEY")));
			cs.finalize(PARTSUPP);
			retval.put(PARTSUPP.getName(), PARTSUPP);

			S ORDERS = cs.createSchema("ORDERS");
			ORDERS.addCol("O_ORDERKEY", new IntType(false, false));
			ORDERS.addCol("O_CUSTKEY", new IntType(false, false));
			ORDERS.addCol("O_ORDERSTATUS", new StringType(false, false,false,1));
			ORDERS.addCol("O_TOTALPRICE", new DoubleType(false, false));
			ORDERS.addCol("O_ORDERDATE", new DateType(false, false));
			ORDERS.addCol("O_ORDERPRIORITY", new StringType(false, false,false,15));
			ORDERS.addCol("O_CLERK", new StringType(false, false,false,15));
			ORDERS.addCol("O_SHIPPRIORITY", new IntType(false, false));
			ORDERS.addCol("O_COMMENT", new StringType(false, false,true,79));
			ORDERS.setPrimaryKey(Collections.singleton("O_ORDERKEY"));
			ORDERS.addForeignKey(new ForeignKey("O_fk", ORDERS, Collections.singletonList("O_CUSTKEY"), CUSTOMER, Collections.singletonList("C_CUSTKEY")));
			cs.finalize(ORDERS);
			retval.put(ORDERS.getName(), ORDERS);

			S LINEITEM = cs.createSchema("LINEITEM");
			LINEITEM.addCol("L_ORDERKEY", new IntType(false, false));
			LINEITEM.addCol("L_PARTKEY", new IntType(false, false));
			LINEITEM.addCol("L_SUPPKEY", new IntType(false, false));
			LINEITEM.addCol("L_LINENUMBER", new IntType(false, false));
			LINEITEM.addCol("L_QUANTITY", new DoubleType(false, false));
			LINEITEM.addCol("L_EXTENDEDPRICE", new DoubleType(false, false));
			LINEITEM.addCol("L_DISCOUNT", new DoubleType(false, false));
			LINEITEM.addCol("L_TAX", new DoubleType(false, false));
			LINEITEM.addCol("L_RETURNFLAG", new StringType(false, false,false,1));
			LINEITEM.addCol("L_LINESTATUS", new StringType(false, false,false,1));
			LINEITEM.addCol("L_SHIPDATE", new DateType(false, false));
			LINEITEM.addCol("L_COMMIDATE", new DateType(false, false));
			LINEITEM.addCol("L_RECEIPTDATE", new DateType(false, false));
			LINEITEM.addCol("L_SHIPINSTRUCT", new StringType(false, false,false,25));
			LINEITEM.addCol("L_SHIPMODE", new StringType(false, false,false,10));
			LINEITEM.addCol("L_COMMENT", new StringType(false, false,true,44));
			LINEITEM.setPrimaryKey(Arrays.asList("L_ORDERKEY","L_LINENUMBER"));
			LINEITEM.addForeignKey(new ForeignKey("LI_fk1", LINEITEM, Collections.singletonList("L_ORDERKEY"), ORDERS, Collections.singletonList("O_ORDERKEY")));
			LINEITEM.addForeignKey(new ForeignKey("LI_fk2", LINEITEM, Collections.singletonList("L_PARTKEY"), PART, Collections.singletonList("P_PARTKEY")));
			LINEITEM.addForeignKey(new ForeignKey("LI_fk3", LINEITEM, Collections.singletonList("L_SUPPKEY"), SUPPLIER, Collections.singletonList("S_SUPPKEY")));
			LINEITEM.addForeignKey(new ForeignKey("LI_fk4", LINEITEM, Arrays.asList("L_PARTKEY","L_SUPPKEY"), PARTSUPP, Arrays.asList("PS_PARTKEY","PS_SUPPKEY")));
			cs.finalize(LINEITEM);
			retval.put(LINEITEM.getName(), LINEITEM);

			return retval;
		} catch (AbstractRelation.BadColumnName bcn) {
			throw new RuntimeException(bcn);
		} catch (UnknownRefFieldException e) {
			throw new RuntimeException(e);
		}
	}

	public static class TupleFileReader<S extends AbstractRelation, T extends AbstractTuple<S>> implements Iterator<T> {
		private final String relationName;
		private final TupleFactory<S,T> tf;
		private final InputStreamReader inputReader;
		private final S schema;
		private final int numCols;
		private final String[] fields;
		private final StringBuilder sb;
		private boolean hitEOF = false;
		private boolean returnedLast = false;
		private int lastCharRead;
		private final TupleCountObserver tco;
		private int count = 0;

		public TupleFileReader(TupleFactory<S,T> tf, String relationName, InputStreamReader inputReader) throws IOException {
			this(tf,relationName,inputReader,null);
		}
		
		public TupleFileReader(TupleFactory<S,T> tf, String relationName, InputStreamReader inputReader, TupleCountObserver tco) throws IOException {
			this.tf = tf;
			this.relationName = relationName;
			this.inputReader = inputReader;
			schema = tf.getSchema(relationName);
			numCols = schema.getNumCols();
			fields = new String[numCols];
			sb = new StringBuilder();
			lastCharRead = inputReader.read();
			if (lastCharRead == -1) {
				hitEOF = true;
				returnedLast = true;
			} else {
				read();
			}
			this.tco = tco;
		}

		public boolean hasNext() {
			return (! returnedLast);
		}

		private void read() throws IOException {
			int pos = 0;
			int c = lastCharRead;
			if (c == -1) {
				hitEOF = true;
				return;
			}
			sb.setLength(0);
			try {
				while (c != '\n' && c != '\r') {
					if (c == '|') {
						fields[pos++] = sb.toString();
						sb.setLength(0);
					} else {
						sb.append((char) c);
					}
					c = inputReader.read();
				}
				if (sb.length() != 0) {
					throw new IllegalArgumentException("Every line should end with a pipe");
				}
			} catch (IndexOutOfBoundsException e) {
				throw new RuntimeException("Line has too many fields, need " + numCols + ", read so far: " + Arrays.toString(fields));
			}

			if (pos != numCols) {
				for (int i = pos; i < numCols; ++i) {
					fields[i] = null;
				}
				throw new RuntimeException("Line has too few fields, need " + numCols + ", read so far: " + Arrays.toString(fields));
			}

			while (c == '\n' || c == '\r') {
				c = inputReader.read();
				if (c == -1) {
					hitEOF = true;
					break;
				}
			}
			lastCharRead = c;
		}

		public T next() {
			if (returnedLast) {
				throw new NoSuchElementException();
			}

			T result;

			Object[] parsed = new Object[numCols];
			try {
				for (int i = 0; i < numCols; ++i) {
					Object o = schema.getColType(i).fromStringRep(fields[i]);
					parsed[i] = o;
				}
				result = tf.createTuple(relationName, parsed);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			++count;
			if (tco != null) {
				tco.currentTupleCountIs(count);
			}
			
			if (hitEOF) {
				returnedLast = true;
				if (tco != null) {
					tco.finalTupleCountIs(count);
				}
			} else {
				try {
					read();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			return result;
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	private static final Map<String,Integer> numHistBuckets;

	static {
		Map<String,Integer> temp = new HashMap<String,Integer>();
		temp.put("NATION", 100);
		temp.put("REGION", 100);
		temp.put("PART", 20);
		temp.put("SUPPLIER", 20);
		temp.put("PARTSUPP", 20);
		temp.put("CUSTOMER", 20);
		temp.put("LINEITEM", 20);
		temp.put("ORDERS", 20);
		numHistBuckets = Collections.unmodifiableMap(temp);
	}

	interface TupleCountObserver {
		void currentTupleCountIs(int count);
		void finalTupleCountIs(int count);
	}
	
	public static <S extends AbstractRelation, T extends AbstractTuple<S>> Map<String,List<Histogram<?>>> createHistograms(Map<String,? extends S> schemas, TupleFactory<S,T> tf, File tpchDir) throws IOException, DatabaseException {
		Map<String,List<Histogram<?>>> hists = new HashMap<String,List<Histogram<?>>>(tableNames.size());

		File f = new File("histEnv");
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
		} else {
			f.mkdir();
		}
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setCachePercent(80);
		ec.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
		ec.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
		Environment e = new Environment(f, ec);
		TableNameGenerator tng = new SimpleTableNameGenerator("histogram");
		
		TupleCountObserver tco = new TupleCountObserver() {
			@Override
			public void currentTupleCountIs(int count) {
				if (count % 100000 == 0) {
					System.out.println("\t" + count);
				}
			}

			@Override
			public void finalTupleCountIs(int count) {
				System.out.println("\t" + count);
			}
		};
		try {
			for (String table : tableNames) {
				int numBuckets = numHistBuckets.get(table);
				System.out.println("Generating " + numBuckets + "-bucket histograms for " + table + "...");
				String fileName = table.toLowerCase();
				S schema = schemas.get(table);
				File tableFile = new File(tpchDir, fileName + ".tbl");
				
				InputStreamReader reader = new InputStreamReader(new FileInputStream(tableFile));
				List<Histogram<?>> histsForRel = HistogramGenerator.generateHistograms(e, tng, schema, new TupleFileReader<S,T>(tf, table, reader,tco), numBuckets);
				reader.close();
				hists.put(table, histsForRel);
				System.out.println("done");
			}
			return hists;
		} finally {
			e.close();
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
		}
	}

	public <S extends AbstractRelation, T extends AbstractTuple<S>> Iterator<T> readTuples(String table, TupleFactory<S,T> tf) throws IOException {
		File tpchDir = getTpchDir();
		String fileName = table.toLowerCase();
		File tableFile = new File(tpchDir, fileName + ".tbl");
		InputStreamReader reader = new InputStreamReader(new FileInputStream(tableFile));
		return new TupleFileReader<S,T>(tf, table, reader);
	}

	public static void main(String args[]) throws Exception {
		TPCH tpch = new TPCH();
		for (int i = 0; i < args.length; ++i) {
			if (args[i].equals("histograms") || args[i].equals("-h")) {
				File tpchDir = tpch.getTpchDir();
				File outFile = tpch.getTpchHistFile();

				Map<String, List<Histogram<?>>> hists = createHistograms(simpleQpSchemas, tf, tpchDir);
				ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)));
				oos.writeObject(hists);
				oos.close();
			} else if (args[i].equals("load") || args[i].equals("-l")) {
				tpch.loadTpchDatabase();
			} else if (args[i].equals("results") || args[i].equals("-r")) {
				File outFile = tpch.getResultsFile();
				Map<String,Set<QpTuple<?>>> results = tpch.createTpchResults();
				OutputStream os = new BufferedOutputStream(new FileOutputStream(outFile));
				for (String query : results.keySet()) {
					Set<QpTuple<?>> result = results.get(query);
					byte[] queryName = query.getBytes("UTF-8");
					os.write(IntType.getBytes(queryName.length));
					os.write(queryName);
					os.write(IntType.getBytes(result.size()));
					for (QpTuple<?> t : result) {
						byte[] bytes = t.getStoreBytes();
						os.write(IntType.getBytes(bytes.length));
						os.write(bytes);
					}
				}
				os.close();
			} else if (args[i].equals("showResults") || args[i].equals("-s")) {
				Map<String,Set<QpTuple<?>>> results = tpch.readTpchResults();
				for (Map.Entry<String, Set<QpTuple<?>>> me : results.entrySet()) {
					System.out.println(me.getKey() + ":");
					for (QpTuple<?> t : me.getValue()) {
						System.out.println(t.toString());
					}
					System.out.println("==============");
				}
			} else if (args[i].equals("optimize")) {
				BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				CombineCalibrations cc;

				int numNodes;
				System.out.print("Enter number of nodes: ");
				System.out.flush();
				numNodes = Integer.parseInt(br.readLine());

				System.out.print("Enter combined calibration filename: ");
				System.out.flush();
				String fileName = br.readLine();
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(fileName));
				cc = (CombineCalibrations) ois.readObject();
				ois.close();

				for (String query : parsedQueries.keySet()) {
					Query q = parsedQueries.get(query);
					System.out.println("Optimizing " + query);


					CreateSchema<QpSchema> cs = new CreateQpSchema(TestHarness.hashCols);
					Map<String,QpSchema> schemas = createSchemas(cs);
					HashMapRelationTypes<Location,QpSchema> rt = tpch.createRT(schemas, null);

					Location.Factory lf = new Location.Factory(rt);
					P2PQPQueryPlanGenerator<QpSchema,Null> qpg = new P2PQPQueryPlanGenerator<QpSchema,Null>(numNodes, cc);
					Optimizer<Location,QueryPlan<Null>,Double,QpSchema> optimizer = new Optimizer<Location,QueryPlan<Null>,Double,QpSchema>(1,true,rt,qpg,lf);
					QpSchemaFactory qsf = new QpSchemaFactory(1);
					QpSchema outputSchema = querySchemas.get(query);

					long startTime = System.nanoTime();
					CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, outputSchema, Location.CENTRALIZED, qsf);
					long endTime = System.nanoTime();
					System.out.println("Took " + ((endTime - startTime) / 1.0e9) + " seconds");

					Map<String,QpSchema> createdSchemas = new HashMap<String,QpSchema>(qsf.getCreatedSchemasByName());
					createdSchemas.put(outputSchema.getName(), outputSchema);

					QueryPlanWithSchemas<Null> qpws = new QueryPlanWithSchemas<Null>(cqp.qp, createdSchemas, cqp.cost, Null.class);

					DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
					DocumentBuilder builder = builderFactory.newDocumentBuilder();
					Document doc = builder.newDocument();
					Element el = doc.createElement("QueryPlanWithSchemas");
					doc.appendChild(el);
					qpws.serialize(doc, el, rt.getAllSchemas());

					OutputStream out = new FileOutputStream(query + ".plan");
					DomUtils.write(doc, out);
					out.close();
				}
			} else {
				File f = new File(args[i]);
				if (f.exists() && f.isDirectory()) {
					tpch = new TPCH(args[i]);
				} else {
					System.err.println("Arguments: [tpchDir] (histograms|results|showResults|optimize|load)");
					System.exit(-1);
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	public HashMapRelationTypes<Location,QpSchema> createRT(Map<String,QpSchema> schemas, HashMapRelationTypes<Location,QpSchema> toAddTo) throws IOException, IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(getTpchHistFile())));
		Map<String, List<Histogram<?>>> hists;
		try {
			hists = (Map<String, List<Histogram<?>>>) ois.readObject();
		} catch (java.io.InvalidClassException ice) {
			throw new IOException("Histogram file uses out of data class definition, need to regenerate", ice);
		}
		ois.close();

		HashMapRelationTypes<Location,QpSchema> retval = toAddTo == null ? new HashMapRelationTypes<Location,QpSchema>() : toAddTo;

		for (String name : tableNames) {
			QpSchema schema = schemas.get(name);
			retval.addRelation(schema, schema.getOptimizerLocation(), hists.get(name));
		}

		return retval;
	}

	private Properties p = null;
	private Properties getProperties() {
		InputStream propStreams[] = {TPCH.class.getResourceAsStream("local.properties"),
				ClassLoader.getSystemResourceAsStream("tpch.properties"), null};
		try {
			propStreams[2] = new FileInputStream("tpch.properties");
		} catch (IOException e) {
		}
		Properties p = new Properties();
		for (InputStream is : propStreams) {
			if (is != null) {
				try {
					p.load(is);
					is.close();
				} catch (IOException ioe) {
					System.err.println("Couldn't load properties file");
					ioe.printStackTrace();
				}
			}
		}

		return p;
	}

	private String getProperty(String key) {
		if (p == null) {
			p = getProperties(); 
		}
		String retval = p.getProperty(key);
		if (retval == null) {
			throw new IllegalStateException("Couldn't find property '" + key + "'");
		}
		return retval;
	}

	public final File tpchDir;
	
	public TPCH() {
		tpchDir = null;
	}
	
	public TPCH(String tpchDir) {
		this.tpchDir = new File(tpchDir);
	}

	public TPCH(File tpchDir) {
		this.tpchDir = tpchDir;
	}
	
	private File getResultsFile() {
		return new File(getTpchDir(), "results.out");
	}

	private File getTpchDir() {
		if (tpchDir == null) {
			return new File(getProperty("tpchDir"));
		} else {
			return tpchDir;
		}
	}

	private File getTpchHistFile() {
		return new File(getTpchDir(),  "hists.out");
	}

	public static final Map<String,String> queries;

	static {
		Map<String,String> temp = new HashMap<String,String>();

		temp.put("Q1", "select l_returnflag, l_linestatus, " +
				"sum(l_quantity) as sum_qty, " +
				"sum(l_extendedprice) as sum_base_price, " +
				"sum(l_extendedprice*(1-l_discount)) as sum_disc_price, " +
				"sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, " +
				"avg(l_quantity) as avg_qty, " +
				"avg(l_extendedprice) as avg_price, " +
				"avg(l_discount) as avg_disc, " +
				"count(*) as count_order " +
				"from lineitem " +
				"where l_shipdate <= date '1998-09-01' " +
				"group by l_returnflag, l_linestatus"
		);

		temp.put("Q3", "select l_orderkey, sum(l_extendedprice*(1-l_discount)) as revenue, o_orderdate, o_shippriority " +
				"from customer, orders, lineitem " + 
				"where c_mktsegment = 'MACHINERY' and c_custkey = o_custkey " +
				"and l_orderkey = o_orderkey and " +
				"o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' " +
				"group by l_orderkey, o_orderdate, o_shippriority"
		);

		temp.put("Q5", "select n_name, sum(l_extendedprice * (1 - l_discount)) as revenue " +
				"from customer, orders, lineitem, supplier, nation, region " +
				"where c_custkey = o_custkey and l_orderkey = o_orderkey " +
				"and l_suppkey = s_suppkey and c_nationkey = s_nationkey " +
				"and s_nationkey = n_nationkey and n_regionkey = r_regionkey " +
				"and r_name = 'AMERICA' " +
				"and o_orderdate >= date '1993-01-01' " +
				"and o_orderdate < date '1994-01-01' " +
		"group by n_name");

		temp.put("Q6", "select sum(l_extendedprice*l_discount) as revenue " +
				"from lineitem " +
				"where l_shipdate >= date '1994-01-01' " +
				"and l_shipdate < date '1995-01-01' " +
				"and l_discount >= 0.06 - 0.01 " +
				"and l_discount <= 0.06 + 0.01 " +
		"and l_quantity < 24.2");

		temp.put("Q10", "select c_custkey, c_name, " +
				"sum(l_extendedprice * (1 - l_discount)) as revenue, " +
				"c_acctbal, n_name, c_address, c_phone, c_comment " +
				"from customer, orders, lineitem, nation " +
				"where c_custkey = o_custkey and l_orderkey = o_orderkey " +
				"and o_orderdate >= date '1993-06-01' " +
				"and o_orderdate < date '1993-09-01' " +
				"and l_returnflag = 'R' and c_nationkey = n_nationkey " +
		"group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment");

		queries = Collections.unmodifiableMap(temp);
	}

	public static final Map<String,Query> parsedQueries;
	public static final Map<String,QpSchema> querySchemas;
	public static final Map<String,VariablePosition> queryVPs;
	private static final HashMapRelationTypes<?,QpSchema> simpleRt;

	public static final Map<String,QpSchema> simpleQpSchemas;

	static {
		CreateSchema<QpSchema> cs = new CreateSimpleQpSchema();
		simpleQpSchemas = Collections.unmodifiableMap(createSchemas(cs));
		simpleRt = new HashMapRelationTypes<Location,QpSchema>();
		for (String name : tableNames) {
			QpSchema schema = simpleQpSchemas.get(name);
			List<Histogram<?>> hists = new ArrayList<Histogram<?>>();
			for (int i  = 0; i < schema.getNumCols(); ++i) {
				hists.add(null);
			}
			simpleRt.addRelation(schema, null, hists);
		}

		Map<String,Query> tempQueries = new HashMap<String,Query>();
		Map<String,QpSchema> tempSchemas = new HashMap<String,QpSchema>();
		Map<String,VariablePosition> vps = new HashMap<String,VariablePosition>();

		int count = 1000000;
		for (String name : queries.keySet()) {
			try {
				Query q = getQuery(queries.get(name), simpleRt);
				tempQueries.put(name, q);

				QpSchema qs = new QpSchema(name, count++);
				VariablePosition vp = new VariablePosition(q.head.size());
				int col = 0;
				for (Variable v : q.head) {
					qs.addCol("C" + col, v.getType().getExecutionType());
					vp.addVariable(v);
					++col;
				}
				qs.setCentralized();
				qs.markFinished();
				vp.finish();

				tempSchemas.put(name, qs);
				vps.put(name,vp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		parsedQueries = Collections.unmodifiableMap(tempQueries);
		querySchemas = Collections.unmodifiableMap(tempSchemas);
		queryVPs = Collections.unmodifiableMap(vps);
	}

	private static Query getQuery(String SQL, RelationTypes<?,?> rt) throws ParseException, TypeError, SyntaxError {
		if (! SQL.endsWith(";")) {
			SQL = SQL + ";";
		}
		TestHarness.parser.initParser(new StringReader(SQL));
		ZQuery zq = (ZQuery) TestHarness.parser.readStatement();
		Query q = new Query(zq, rt);
		return q;
	}

	public Map<String,Set<QpTuple<?>>> readTpchResults() throws IOException {
		File filename = getResultsFile();
		InputStream is = new BufferedInputStream(new FileInputStream(filename));
		byte[] intBytes = new byte[IntType.bytesPerInt];
		Map<String,Set<QpTuple<?>>> retval = new HashMap<String,Set<QpTuple<?>>>();
		while (is.read(intBytes) > 0) {
			int nameLength = IntType.getValFromBytes(intBytes);
			byte[] name = new byte[nameLength];
			is.read(name);
			String queryName = new String(name, "UTF-8");
			is.read(intBytes);
			int numTuples = IntType.getValFromBytes(intBytes);
			QpSchema schema = querySchemas.get(queryName);
			Set<QpTuple<?>> tuples = new HashSet<QpTuple<?>>(numTuples);
			retval.put(queryName, tuples);
			while (numTuples > 0) {
				is.read(intBytes);
				int tupleLen = IntType.getValFromBytes(intBytes);
				byte[] tuple = new byte[tupleLen];
				is.read(tuple);
				tuples.add(QpTuple.fromStoreBytes(schema, tuple));
				--numTuples;
			}
		}
		is.close();
		return retval;
	}

	private Map<String,Set<QpTuple<?>>> createTpchResults() throws ClassNotFoundException, SQLException, ValueMismatchException {
		String jdbcClass = getProperties().getProperty("jdbcDriver");
		String jdbcUrl = getProperties().getProperty("jdbcUrl");
		if (jdbcClass == null || jdbcUrl == null) {
			return null;
		}
		Class.forName(jdbcClass);
		Connection conn = DriverManager.getConnection(jdbcUrl);
		Statement s = conn.createStatement();
		s.execute("set current query optimization = 9");

		Map<String,Set<QpTuple<?>>> results = new HashMap<String,Set<QpTuple<?>>>();

		for (String name : queries.keySet()) {
			QpSchema schema = querySchemas.get(name);
			final int numCols = schema.getNumCols();
			String query = queries.get(name);
			// Needed for DB2
			query = query.replace(" date '", " '");
			System.out.println("Executing " + name);
			ResultSet rs = s.executeQuery(query);

			Object[] fields = new Object[numCols];
			Set<QpTuple<?>> tuples = new HashSet<QpTuple<?>>();

			while (rs.next()) {
				for (int i = 0; i < numCols; ++i) {
					fields[i] = schema.getColType(i).getFromResultSet(rs, i + 1);
				}
				QpTuple<?> t = new QpTuple<Null>(schema, fields);
				tuples.add(t);
			}

			results.put(name, tuples);
		}

		s.close();
		conn.close();

		return results;
	}

	private static void dropTpchDatabase(Connection conn) throws SQLException {
		List<String> tables = new ArrayList<String>(tableNames);
		Collections.reverse(tables);
		Statement s = conn.createStatement();
		for (String table : tables) {
			try {
				s.execute("DROP TABLE " + table);
			} catch (SQLException e) {
				// Presumably table doesn't exist
			}
		}
		conn.commit();
		s.close();
	}

	private static void createTpchDatabase(Connection conn) throws SQLException {
		Statement s = conn.createStatement();
		for (String tableName : tableNames) {
			s.execute(getDDL(simpleQpSchemas.get(tableName)));
		}
		s.close();
	}

	private static String getDDL(AbstractRelation schema) {
		StringBuilder sb = new StringBuilder("CREATE TABLE " + schema.getName() + " (");
		for (RelationField rf : schema.getFields()) {
			sb.append(rf.getName());
			sb.append(" ");
			sb.append(rf.getSQLType());
			sb.append(", ");
		}

		sb.append("PRIMARY KEY(");
		Iterator<RelationField> pk = schema.getPrimaryKey().getFields().iterator();
		while (pk.hasNext()) {
			sb.append(pk.next().getName());
			if (pk.hasNext()) {
				sb.append(", ");
			}
		}
		sb.append(")) NOT LOGGED INITIALLY");

		return sb.toString();
	}

	private static class CreateSimpleQpSchema implements CreateSchema<QpSchema> {
		int count = 0;
		public QpSchema createSchema(String name) {
			return new QpSchema(name, count++);
		}

		public void finalize(QpSchema schema) {
			schema.markFinished();
		}
	}

	private void loadTpchDatabase() throws ClassNotFoundException, SQLException, IOException {
		String jdbcClass = getProperties().getProperty("jdbcDriver");
		String jdbcUrl = getProperties().getProperty("jdbcUrl");
		if (jdbcClass == null || jdbcUrl == null) {
			System.err.println("Must specify jdbcDriver and jdbcUrl");
			return;
		}
		Class.forName(jdbcClass);
		Connection conn = DriverManager.getConnection(jdbcUrl);
		conn.setAutoCommit(false);

		dropTpchDatabase(conn);
		System.err.println("Dropped old tables");
		createTpchDatabase(conn);
		System.err.println("Created new tables");

		final Map<String,QpSchema> schemas = simpleQpSchemas;
		String jdbcUsername = getProperties().getProperty("jdbcUsername");

		Statement stmt = conn.createStatement();
		try {
			for (Map.Entry<String, QpSchema> me : schemas.entrySet()) {
				final QpSchema s = me.getValue();
				int numCols = s.getNumCols();
				StringBuffer query = new StringBuffer("INSERT INTO " + me.getKey() + " VALUES(?");
				for (int i = 1; i < numCols; ++i) {
					query.append(",?");
				}
				query.append(")");
				PreparedStatement ps = conn.prepareStatement(query.toString());
				int count = 0;
				Iterator<? extends AbstractTuple<?>> tuples = readTuples(me.getKey(), mutableTf);
				while (tuples.hasNext()) {
					AbstractTuple<?> t = tuples.next();
					for (int i = 0; i < numCols; ++i) {
						Object o = t.get(i);
						s.getColType(i).setInPreparedStatement(o, ps, i + 1);
					}
					ps.addBatch();
					count++;
					if (count % 100000 == 0) {
						System.err.println("Loaded " + count + " tuples into " + s.getName() + ", executing batch updates");
						ps.executeBatch();
					}
				}
				System.err.println("Loaded all " + count + " tuples into " + s.getName() + ", executing batch updates");
				ps.executeBatch();
				System.err.println("Executed all batch updates for " + s.getName() + ", generating statistics");
				try {
					String command = "CALL SYSPROC.ADMIN_CMD('RUNSTATS ON TABLE " + jdbcUsername + '.' + s.getName() + "')";
					stmt.execute(command);
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.err.println("Generated statistics for " + s.getName());
				ps.close();
			}
			System.err.println("Comitting changes");
			conn.commit();
			System.err.println("Done loading data");
		} catch (SQLException sqle) {
			conn.rollback();
			while (sqle != null) {
				sqle.printStackTrace();
				sqle = sqle.getNextException();
			}
		} catch (Exception e) {
			e.printStackTrace();
			conn.rollback();
			return;
		} finally {
			stmt.close();
			conn.close();
		}

	}
}
