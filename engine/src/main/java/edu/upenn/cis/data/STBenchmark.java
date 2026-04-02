package edu.upenn.cis.data;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.ForeignKey;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.BadColumnName;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple.TupleFactory;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.optimization.HashMapRelationTypes;
import edu.upenn.cis.orchestra.optimization.Histogram;
import edu.upenn.cis.orchestra.optimization.Location;
import edu.upenn.cis.orchestra.p2pqp.Null;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTuple;
import edu.upenn.cis.orchestra.p2pqp.TableNameGenerator;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class STBenchmark {

	public <T extends AbstractTuple<?>> void
	load(TupleFactory<?,T> tf, TupleSink<? super T> ts) throws SAXException, IOException {
		InputStream file = new BufferedInputStream(new FileInputStream(new File(this.stbenchdir, instanceFile)));
		XMLReader xr = XMLReaderFactory.createXMLReader();
		InputSource is = new InputSource(file);
		InstanceHandler<T> handler = new InstanceHandler<T>(ts, tf);
		xr.setContentHandler(handler);
		xr.parse(is);
	}

	private class InstanceHandler<T extends AbstractTuple<?>> extends DefaultHandler {
		final TupleSink<? super T> ts;
		final TupleFactory<? extends AbstractRelation,T> tf;

		boolean foundSource = false;
		String relation = null;
		Map<String,Integer> tupleCountForRel = new HashMap<String,Integer>();
		String field = null;
		StringBuilder sb = new StringBuilder();
		AbstractRelation schema = null;

		List<String> fields = new ArrayList<String>();

		Map<String,Integer> skolemValues = new HashMap<String,Integer>();
		int nextSkolem = 0;

		InstanceHandler(TupleSink<? super T> ts, TupleFactory<?,T> tf) {
			this.ts = ts;
			this.tf = tf;
		}

		public void startElement(String uri, String localName, String qName,
				Attributes atts) throws SAXException {
			if (localName.equals("Source")) {
				if (foundSource) {
					throw new SAXException("Found nested or sequential Source elements");
				} else {
					foundSource = true;
				}
			} else if (! foundSource) {
				throw new SAXException("Root element is not Source");
			} else if (relation == null) {
				relation = localName.toUpperCase();
				schema = tf.getSchema(relation);
				if (schema == null) {
					throw new SAXException("Couldn't find relation " + relation);
				}
			} else if (field == null) {
				field = localName.toUpperCase();
				if (! field.equals(schema.getColName(fields.size() + 1))) {
					throw new SAXException("Field #" + (fields.size()+1) + " of relation " + relation + " is named " + schema.getColName(fields.size()+1) + " but file has " + field);
				}
			} else {
				throw new SAXException("Found an element inside a field");
			}
		}

		public void endElement(String uri, String localName, String qName) throws SAXException {
			if (field != null) {
				fields.add(sb.toString());
				sb.setLength(0);
				field = null;
			} else if (relation != null) {
				final int numCols = schema.getNumCols();
				if ((fields.size()+1) != numCols) {
					throw new SAXException("Relation " + relation + " has " + numCols + " fields but file contains " + fields.size());
				}
				Object[] fields = new Object[numCols];
				for (int i = 1; i < numCols; ++i) {
					try {
						fields[i] = schema.getColType(i).fromStringRep(this.fields.get(i-1));
					} catch (XMLParseException e) {
						throw new SAXException("Error parsing field " + schema.getColName(i) + " of relation " + relation, e);
					}
				}
				Integer count = this.tupleCountForRel.get(relation);
				if (count == null) {
					count = 0;
				}
				fields[0] = count;
				T tuple;
				this.tupleCountForRel.put(relation, count + 1);
				try {
					tuple = tf.createTuple(relation, fields); 
					ts.put(tuple, null);
				} catch (ValueMismatchException e) {
					throw new SAXException(e);
				}
				List<String> corrFields = correspondences.get(relation);
				if (corrFields != null) {
					try {
						String corrSchemaName = relation + "_CORR";
						Object[] corrFieldsVals = new Object[corrFields.size() + 2];
						StringBuilder sb = new StringBuilder(relation);
						for (int i = 2; i < corrFieldsVals.length; ++i) {
							corrFieldsVals[i] = tuple.get(corrFields.get(i-2));
							sb.append(corrFieldsVals[i]);
						}
						String skolemKey = sb.toString();
						if (! skolemValues.containsKey(skolemKey)) {
							int skolem = nextSkolem++;
							corrFieldsVals[1] = skolem;
							count = this.tupleCountForRel.get(corrSchemaName);
							if (count == null) {
								count = 0;
							}
							corrFieldsVals[0] = count;
							this.tupleCountForRel.put(corrSchemaName, count + 1);
							ts.put(tf.createTuple(corrSchemaName, corrFieldsVals), relation);
						}
					} catch (Exception e) {
						throw new SAXException(e);
					}
				}
				relation = null;
				this.fields.clear();
			}
		}

		public void characters(char[] ch, int start, int length) throws SAXException {
			if (field == null) {
				return;
			}
			sb.ensureCapacity(sb.length() + length);
			for (int i = 0; i < length; ++i) {
				sb.append(ch[start + i]);
			}
		}
	}

	public interface TupleSink<T extends AbstractTuple<?>> {
		void put(T tuple, String origSchema) throws SAXException;
	}

	static boolean printFile = false;

	static final String templateFile = "S.template";
	static final String instanceFile = "I.xml";
	static final String histsFile = "hists.out";
	static final String correspondencesFile = "corr.txt";

	private final File stbenchdir;

	public STBenchmark(String stbenchdir) throws IOException {
		this.stbenchdir = new File(stbenchdir);
		if (! (this.stbenchdir.exists() && this.stbenchdir.isDirectory())) {
			throw new FileNotFoundException("stbenchmark directory not found: " + stbenchdir);
		}

		File f = new File(stbenchdir, correspondencesFile);
		if (! f.exists()) {
			correspondences = Collections.emptyMap();
		} else {
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(f)));
			String line = br.readLine();
			Map<String,List<String>> correspondences = new HashMap<String,List<String>>();
			while (line != null) {			
				String[] tokens = line.split("\\s+");
				line = br.readLine();
				if (correspondences.containsKey(tokens[0].toUpperCase())) {
					throw new IllegalStateException("Correspondences file contains multiple lines for the same table");
				}
				List<String> fields = new ArrayList<String>(tokens.length - 1);
				for (int i = 1; i < tokens.length; ++i) {
					fields.add(tokens[i].toUpperCase());
				}
				correspondences.put(tokens[0].toUpperCase(), fields);
			}
			this.correspondences = Collections.unmodifiableMap(correspondences);
		}
	}

	public static void main(String args[]) throws Exception {
		if (args.length == 0 || args.length > 1) {
			System.err.println("Arguments: stbenchdir");
			System.exit(-1);
		}

		STBenchmark stb = new STBenchmark(args[0]);

		CreateSchema<QpSchema> cs = new CreateSchema<QpSchema>() {
			int relId = 1000;
			@Override
			public QpSchema createSchema(String name) {
				return new QpSchema(name,relId++);
			}

			@Override
			public void finalize(QpSchema schema) {
				schema.markFinished();
			}
		};

		final Map<String,QpSchema> schemas = stb.createSchemas(cs);
		for (QpSchema schema : schemas.values()) {
			System.out.println(schema);
			for (ForeignKey fk : schema.getForeignKeys()) {
				System.out.println("\t" + fk);
			}
		}

		TupleFactory<QpSchema,QpTuple<Null>> tf = new TupleFactory<QpSchema,QpTuple<Null>>() {

			@Override
			public QpTuple<Null> createTuple(String relationName,
					Object... fields) throws ValueMismatchException {
				return new QpTuple<Null>(getSchema(relationName), fields);
			}

			@Override
			public QpSchema getSchema(String relationName) {
				return schemas.get(relationName);
			}

		};

		if (printFile) {
			TupleSink<QpTuple<Null>> ts = new TupleSink<QpTuple<Null>>() {

				@Override
				public void put(QpTuple<Null> tuple, String origSchema) {
					System.out.println(tuple);
				}

			};

			stb.load(tf,ts);
		}

		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setReadOnly(false);
		ec.setAllowCreate(true);
		ec.setTransactional(false);

		TableNameGenerator tng = new TableNameGenerator() {
			int count = 0;
			@Override
			public String getFreshTableName() {
				return "temp" + (count++);
			}

		};

		File envFile = new File("env");
		if (! envFile.exists()) {
			envFile.mkdir();
		}
		Environment e = new Environment(envFile, ec);

		HistogramGenerator hg = new HistogramGenerator(e, tng, schemas, 20);
		stb.load(tf,hg);

		Map<String,List<Histogram<?>>> hists = hg.finish();

		for (Map.Entry<String, List<Histogram<?>>> me : hists.entrySet()) {
			final String relation = me.getKey();
			final QpSchema schema = schemas.get(relation);
			final List<Histogram<?>> histsForRel = me.getValue();
			System.out.println(relation);
			for (int i = 0; i < histsForRel.size(); ++i) {
				Histogram<?> h = histsForRel.get(i);
				System.out.println(schema.getColName(i) + ": " + h.getCard() + " " + h.getNumDistinctValues());
			}
		}

		e.close();
		for (File f : envFile.listFiles()) {
			f.delete();
		}
		envFile.delete();

		File outFile = new File(args[0], histsFile);
		ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(outFile)));
		oos.writeObject(hists);
		oos.close();

		System.out.println("Wrote histograms to " + outFile);
	}

	@SuppressWarnings("unchecked")
	public HashMapRelationTypes<Location,QpSchema> createRT(Map<String,QpSchema> schemas, HashMapRelationTypes<Location,QpSchema> toAddTo) throws IOException, IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(new File(this.stbenchdir, histsFile))));
		Map<String, List<Histogram<?>>> hists;
		try {
			hists = (Map<String, List<Histogram<?>>>) ois.readObject();
		} catch (java.io.InvalidClassException ice) {
			throw new IOException("Histogram file uses out of data class definition, need to regenerate", ice);
		}
		ois.close();

		HashMapRelationTypes<Location,QpSchema> retval = toAddTo == null ? new HashMapRelationTypes<Location,QpSchema>() : toAddTo;

		List<QpSchema> STschemas = new ArrayList<QpSchema>(hists.size());
		for (String schemaName : hists.keySet()) {
			STschemas.add(schemas.get(schemaName));
		}
		STschemas = QpSchema.sortByFKs(STschemas, hists.keySet());

		for (QpSchema schema : STschemas) {
			retval.addRelation(schema, schema.getOptimizerLocation(), hists.get(schema.getName()));
		}

		return retval;
	}

	public final Map<String,List<String>> correspondences;

	public <S extends AbstractRelation> Map<String,S> createSchemas(final CreateSchema<S> cs) throws SAXException, IOException {
		InputStream schemaFile = new BufferedInputStream(new FileInputStream(new File(this.stbenchdir, templateFile)));
		XMLReader xr = XMLReaderFactory.createXMLReader();
		InputSource is = new InputSource(schemaFile);
		SchemaHandler<S> handler = new SchemaHandler<S>(cs);
		xr.setContentHandler(handler);
		xr.parse(is);

		Map<String,S> retval = handler.schemas;

		for (Map.Entry<String, List<String>> me : correspondences.entrySet()) {
			final String source = me.getKey();
			final List<String> cols = me.getValue();
			AbstractRelation sourceSchema = retval.get(source);
			if (source == null) {
				throw new IllegalStateException("Schema '" + source + "' mentioned in correspondences file does not exist");
			}
			S corrSchema = cs.createSchema(source + "_CORR");
			try {
				corrSchema.addCol("ID", new IntType(false,false));
				corrSchema.addCol("SKOLEM", new IntType(false,false));
				for (String col : cols) {
					corrSchema.addCol(col, sourceSchema.getColType(col));
				}
			} catch (AbstractRelation.BadColumnName bcn) {
				throw new IllegalStateException(bcn);
			}
			cs.finalize(corrSchema);
			retval.put(corrSchema.getName(), corrSchema);
		}

		return retval;
	}

	private static class SchemaHandler<S extends AbstractRelation> extends DefaultHandler {
		boolean processingField = false;
		boolean insideDocument = false;
		S relation = null;
		Map<String,S> schemas = new HashMap<String,S>();
		boolean insideSample = false;
		Map<String,String> findRelationName = new HashMap<String,String>();
		String elementName;
		List<String> fkFields = new ArrayList<String>();
		List<String> fkRefFields = new ArrayList<String>();
		List<ForeignKey> fks = new ArrayList<ForeignKey>();
		S fkRefRel = null;

		final CreateSchema<S> cs;

		SchemaHandler(CreateSchema<S> cs) {
			this.cs = cs;
		}
		static final StringType stringType = new StringType(false,false,true,25);
		static final IntType intType = new IntType(false,false);
		public void startElement(String uri, String localName, String qName,
				Attributes atts) throws SAXException {
			if (localName.equals("tox-sample")) {
				insideSample = true;
				return;
			} else if (localName.equals("tox-document")) {
				insideDocument = true;
			} else if (insideDocument) {
			} else if (insideSample && processingField && localName.equals("tox-expr")) {
				String value = atts.getValue("value").toUpperCase();
				value = value.substring(1, value.length() -1);
				String refRel = findRelationName.get(value);
				fkRefRel = schemas.get(refRel);
				try {
					relation.addCol(elementName, fkRefRel.getColType(value));
				} catch (AbstractRelation.BadColumnName bcn) {
					throw new SAXException(bcn);
				}
				fkFields.add(elementName);
				fkRefFields.add(value);
				elementName = null;
			} else if (localName.equals("element")) {
				if (relation == null) {
					relation = cs.createSchema(atts.getValue("name").toUpperCase());
					schemas.put(relation.getName(), relation);
					try {
						relation.addCol("ID", intType);
						relation.setPrimaryKey(Collections.singleton("ID"));
					} catch (BadColumnName e) {
						throw new SAXException(e);
					} catch (UnknownRefFieldException e) {
						throw new SAXException(e);
					}
				} else {
					String name = atts.getValue("name").toUpperCase();
					findRelationName.put(name, relation.getName());
					if (insideSample) {
						elementName = name;
					} else {
						String typeStr = atts.getValue("type");
						Type type;
						if (typeStr.equals("bench_string")) {
							type = stringType;
						} else if (typeStr.equals("bench_int")) {
							type = intType;
						} else {
							throw new SAXException("Need to support type: '" + typeStr + "'");
						}
						try {
							relation.addCol(name, type);
						} catch (AbstractRelation.BadColumnName bcn) {
							throw new SAXException(bcn);
						}
					}
					processingField = true;
				}
			}
		}
		public void endElement(String uri, String localName, String qName) throws SAXException {
			if (localName.equals("tox-sample")) {
				insideSample = false;
				try {
					fks.add(new ForeignKey("fk", relation, fkFields, fkRefRel, fkRefFields, true));
				} catch (UnknownRefFieldException e) {
					throw new SAXException(e);
				}
				fkFields.clear();
				fkRefFields.clear();
				fkRefRel = null;
			} else if (localName.equals("element")) {
				if (processingField) {
					processingField = false;
				} else if (relation != null) {
					for (ForeignKey fk : fks) {
						relation.addForeignKey(fk);
					}
					fks.clear();
					cs.finalize(relation);
					relation = null;
				}
			} else if (localName.equals("tox-document")) {
				insideDocument = false;
			}
		}
	};

}
