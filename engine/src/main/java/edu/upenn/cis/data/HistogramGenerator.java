package edu.upenn.cis.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.xml.sax.SAXException;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.OperationStatus;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.Date;
import edu.upenn.cis.orchestra.datamodel.DateType;
import edu.upenn.cis.orchestra.datamodel.DoubleType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.optimization.Histogram;
import edu.upenn.cis.orchestra.p2pqp.TableNameGenerator;

public class HistogramGenerator implements STBenchmark.TupleSink<AbstractTuple<?>> {
	private static final int stringLength = 3;
	public static <S extends AbstractRelation> List<Histogram<?>> generateHistograms(Environment e, TableNameGenerator tng, S schema, Iterator<? extends AbstractTuple<S>> it, int numBuckets) throws DatabaseException {
		final int numCols = schema.getNumCols();
		final DatabaseConfig dc = new DatabaseConfig();
		dc.setTemporary(true);
		dc.setAllowCreate(true);
		dc.setSortedDuplicates(false);
		Database[] values = new Database[numCols];
		Type[] colTypes = new Type[numCols];
		int[] notNullForCol = new int[numCols];
		final byte[] ONE = IntType.getBytes(1);
		try {
			for (int i = 0; i < numCols; ++i) {
				colTypes[i] = schema.getColType(i);
				dc.setBtreeComparator(colTypes[i].getSerializedComparator());
				values[i] = e.openDatabase(null, tng.getFreshTableName(), dc);
			}
			final DatabaseEntry key = new DatabaseEntry(), data = new DatabaseEntry();
			while (it.hasNext()) {
				AbstractTuple<?> t = it.next();
				for (int col = 0; col < numCols; ++col) {
					Object o = t.get(col);
					if (o != null) {
						++notNullForCol[col];
						if (o instanceof String) {
							o = Histogram.convertForHistogram(stringLength, (String) o);
						}
						key.setData(colTypes[col].getBytes(o));
						OperationStatus os = values[col].get(null, key, data, null);
						if (os == OperationStatus.NOTFOUND) {
							data.setData(ONE);
						} else {
							data.setData(IntType.getBytes(IntType.getValFromBytes(data.getData()) + 1));
						}
						values[col].put(null, key, data);
					}
				}
			}
			List<Histogram<?>> retval = new ArrayList<Histogram<?>>(numCols);
			for (int col = 0; col < numCols; ++col) {
				final Type t = schema.getColType(col);
				retval.add(generateHistogram(t, notNullForCol[col], values[col], numBuckets));
				values[col].close();
				values[col] = null;
			}
			return retval;
		} finally {
			for (Database db : values) {
				if (db != null) {
					db.close();
				}
			}
		}
	}

	private static final byte[] ONE = IntType.getBytes(1);
	private static class RelationData {
		final Database[] values;
		final int[] notNullForCol;
		final Type[] colTypes;
		final AbstractRelation schema;
		int totalCount = 0;

		RelationData(AbstractRelation schema, Environment e, TableNameGenerator tng) throws DatabaseException {
			final int numCols = schema.getNumCols();
			values = new Database[numCols];
			colTypes = new Type[numCols];
			notNullForCol = new int[numCols];
			this.schema = schema;
			for (int i = 0; i < numCols; ++i) {
				colTypes[i] = schema.getColType(i);
				dc.setBtreeComparator(colTypes[i].getSerializedComparator());
				values[i] = e.openDatabase(null, tng.getFreshTableName(), dc);
				notNullForCol[i] = 0;
			}
		}
	}
	private Map<String,RelationData> values;
	final DatabaseEntry key = new DatabaseEntry(), data = new DatabaseEntry();
	final int numBuckets;

	private static final DatabaseConfig dc = new DatabaseConfig();
	static {
		dc.setTemporary(true);
		dc.setAllowCreate(true);
		dc.setSortedDuplicates(false);
	}

	public void put(AbstractTuple<?> t, String origSchemaName) throws SAXException {
		try {
			RelationData rd = values.get(t.getSchema().getName());
			for (int col = 0; col < rd.colTypes.length; ++col) {
				Object o = t.get(col);
				if (o != null) {
					++rd.notNullForCol[col];
					if (o instanceof String) {
						o = Histogram.convertForHistogram(stringLength, (String) o);
					}
					key.setData(rd.colTypes[col].getBytes(o));
					OperationStatus os = rd.values[col].get(null, key, data, null);
					if (os == OperationStatus.NOTFOUND) {
						data.setData(ONE);
					} else {
						data.setData(IntType.getBytes(IntType.getValFromBytes(data.getData()) + 1));
					}
					rd.values[col].put(null, key, data);
				}
			}
			++rd.totalCount;
			if (rd.totalCount % 10000 == 0) {
				System.err.println("Loaded " + rd.totalCount + " " + rd.schema.getName() + " tuples");
			}
		} catch (DatabaseException de) {
			throw new SAXException(de);
		}
	}

	public HistogramGenerator(Environment e, TableNameGenerator tng, Map<String,? extends AbstractRelation> schemas, int numBuckets) throws DatabaseException {
		values = new HashMap<String,RelationData>(schemas.size());
		this.numBuckets = numBuckets;
		for (Map.Entry<String, ? extends AbstractRelation> me : schemas.entrySet()) {
			values.put(me.getKey(), new RelationData(me.getValue(), e, tng));
		}
	}

	public Map<String,List<Histogram<?>>> finish() throws DatabaseException {
		Map<String,List<Histogram<?>>> retval = new HashMap<String,List<Histogram<?>>>();
		try {
			for (Map.Entry<String,RelationData> me : values.entrySet()) {
				final String relation = me.getKey();
				System.err.println("Generating " + relation + " histograms:");
				final RelationData rd = me.getValue();
				final int numCols = rd.colTypes.length;
				List<Histogram<?>> histograms = new ArrayList<Histogram<?>>(numCols);
				for (int col = 0; col < numCols; ++col) {
					System.err.print(rd.schema.getColName(col) + "...");
					System.err.flush();
					histograms.add(generateHistogram(rd.colTypes[col], rd.notNullForCol[col], rd.values[col], numBuckets));
					rd.values[col].close();
					rd.values[col] = null;
					System.err.println("done");
				}
				retval.put(relation, histograms);
			}
		} finally {
			for (RelationData values : this.values.values()) {
				for (Database db : values.values) {
					if (db != null) {
						db.close();
					}
				}
			}
		}
		return retval;
	}

	private static Histogram<?> generateHistogram(final Type t, int size, Database data, int numBuckets) throws DatabaseException {
		Cursor c = null;

		final DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();
		OperationStatus os;
		try {
			c = data.openCursor(null, null);
			os = c.getFirst(key, value, null);
			List<Object> bucketEdges = new ArrayList<Object>(numBuckets + 1);
			List<Double> cards = new ArrayList<Double>(numBuckets), DVs = new ArrayList<Double>(numBuckets);
			if (os == OperationStatus.SUCCESS) {
				bucketEdges.add(t.fromBytes(key.getData()));
				if (size < numBuckets * 5) {
					while (os == OperationStatus.SUCCESS) {
						Object curr = t.fromBytes(key.getData());
						if (! curr.equals(bucketEdges.get(bucketEdges.size() - 1))) {
							cards.add(0.0);
							DVs.add(0.0);
							bucketEdges.add(curr);
						}
						int count = IntType.getValFromBytes(value.getData());
						cards.add((double) count);
						DVs.add(1.0);
						bucketEdges.add(getSuccessor(curr));
						os = c.getNext(key, value, null);
					}
				} else {
					// Build an equi-depth histogram
					long approxBucketSize = size / numBuckets;
					if (approxBucketSize == 0) {
						approxBucketSize = 1;
					}

					int card = 0, DV = 0;

					Object last = null;
					while (os == OperationStatus.SUCCESS) {
						Object curr = t.fromBytes(key.getData());
						if (card > approxBucketSize) {
							bucketEdges.add(curr);
							cards.add((double) card);
							DVs.add((double) DV);
							card = 0;
							DV = 0;
						}
						card += IntType.getValFromBytes(value.getData());
						++DV;
						last = curr;
						os = c.getNext(key, value, null);
					}
					bucketEdges.add(getSuccessor(last));
					cards.add((double) card);
					DVs.add((double) DV);
				}
			}

			double[] cardsArray = new double[cards.size()], DVsArray = new double[DVs.size()];
			for (int i = 0; i < cardsArray.length; ++i) {
				cardsArray[i] = cards.get(i);
				DVsArray[i] = DVs.get(i);
			}

			if (t instanceof IntType) {
				List<Integer> intBE = new ArrayList<Integer>(bucketEdges.size());
				for (Object o : bucketEdges) {
					intBE.add((Integer) o);
				}
				return Histogram.createIntegerHistogram(intBE, cardsArray, DVsArray);
			} else if (t instanceof DoubleType) {
				List<Double> doubleBE = new ArrayList<Double>(bucketEdges.size());
				for (Object o : bucketEdges) {
					doubleBE.add((Double) o);
				}
				return Histogram.createDoubleHistogram(doubleBE, cardsArray, DVsArray);
			} else if (t instanceof DateType) {
				List<Date> dateBE = new ArrayList<Date>(bucketEdges.size());
				for (Object o : bucketEdges) {
					dateBE.add((Date) o);
				}
				return Histogram.createDateHistogram(dateBE, cardsArray, DVsArray);
			} else if (t instanceof StringType) {
				List<String> stringBE = new ArrayList<String>(bucketEdges.size());
				for (Object o : bucketEdges) {
					stringBE.add((String) o);
				}
				return Histogram.createStringHistogram(stringLength, stringBE, cardsArray, DVsArray);
			} else {
				throw new IllegalArgumentException("Don't know how to process type " + t);
			}
		} finally {
			if (c != null) {
				c.close();
			}
		}
	}



	private static Object getSuccessor(Object o) {
		if (o instanceof Integer) {
			return ((Integer) o ) + 1;
		} else if (o instanceof Double) {
			return o;
		} else if (o instanceof Date) {
			return ((Date) o).tomorrow();
		} else if (o instanceof String) {
			return Histogram.getSuccessorForHistogram(stringLength, (String) o);
		} else {
			throw new IllegalArgumentException("Don't know how to process data of type " + o.getClass().getName());
		}
	}
}
