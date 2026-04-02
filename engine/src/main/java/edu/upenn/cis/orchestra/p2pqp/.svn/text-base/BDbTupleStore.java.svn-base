package edu.upenn.cis.orchestra.p2pqp;

import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.StatsConfig;

import edu.upenn.cis.orchestra.datamodel.AbstractImmutableTuple;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Location;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.util.ByteArraySet;
import edu.upenn.cis.orchestra.util.OutputBuffer;

/**
 * @author netaylor
 *
 * @param <M>
 */
class BDbTupleStore<M> implements TupleStore<M> {
	final int numLookupThreads = 3;
	final int numDeliverThreads = 3;
	int batchingCutoff = 10000;
	int batchingCheckIntervalMs = 500;
	private final Logger logger = Logger.getLogger(BDbTupleStore.class);
	private Environment e;
	private final String dbName;
	private final MetadataFactory<M> mdf;
	private final DatabaseConfig dc;
	private final Map<Integer,QpSchema> tables;
	private final Map<String,QpSchema> tableNames;

	// Database key format:
	// table id (MSB 4-byte integer), DHT ID (20-byte unsigned MSB integer), serialized key tuple
	private ThreadLocal<Database> dbHandles;
	private List<Database> openHandles;


	BDbTupleStore(Environment e, String dbName, MetadataFactory<M> mdf) throws DatabaseException {
		this.e = e;
		dc = new DatabaseConfig();
		dc.setSortedDuplicates(false);
		dc.setAllowCreate(true);
		EnvironmentConfig ec = e.getConfig();
		dc.setReadOnly(ec.getReadOnly());
		dc.setDeferredWrite(true);

		// Open the database to create it, if necessary.
		Database db = e.openDatabase(null, dbName, dc);
		db.close();
		dc.setAllowCreate(false);


		tables = new HashMap<Integer,QpSchema>();
		tableNames = new HashMap<String,QpSchema>();
		this.dbName = dbName;
		this.mdf = mdf;

		dbHandles = new ThreadLocal<Database>();
		openHandles = new ArrayList<Database>();
	}

	private Database getDatabaseNoCatch() throws DatabaseException {
		Database db = dbHandles.get();
		if (db == null) {
			db = e.openDatabase(null, dbName, dc);
			dbHandles.set(db);
			synchronized (openHandles) {
				openHandles.add(db);
			}
		}
		return db;

	}

	private Database getDatabase() throws TupleStoreException {
		try {
			return getDatabaseNoCatch();
		} catch (DatabaseException de) {
			throw new TupleStoreException("Error opening database", de);
		}
	}

	public void close() throws TupleStoreException {
		try {
			synchronized (openHandles) {
				for (int i = 0; i < openHandles.size(); ++i) {
					Database db = openHandles.get(i);
					db.close();
				}
				openHandles.clear();
			}
			dbHandles = null;
		} catch (DatabaseException de) {
			throw new TupleStoreException("Couldn't close BerkeleyDB TupleStore", de);
		}
	}

	public void addTable(QpSchema ns) {
		int id = ns.relId;
		String name = ns.getName();
		synchronized (tables) {
			if (tables.containsKey(id)) {
				throw new IllegalArgumentException("Table with ID " + id + " is already in this TupleStore");
			}
			if (tableNames.containsKey(name)) {
				throw new IllegalArgumentException("Table with name " + name + " is already in this TupleStore");
			}
			tables.put(id, ns);
			tableNames.put(name, ns);
		}
	}

	public void dropTable(String name) throws TupleStoreException {
		QpSchema s = getSchema(name);
		int id = s.relId;

		clearTable(name);

		synchronized (tables) {
			tables.remove(id);
			tableNames.remove(name);
		}
	}

	public void clearTable(String name) throws TupleStoreException {
		QpSchema s = getSchema(name);
		int id = s.relId;

		Database db = getDatabase();
		try {
			byte[] idBytes = IntType.getBytes(id);
			Cursor c = db.openCursor(null, null);

			try {
				DatabaseEntry key = new DatabaseEntry(idBytes), value = new DatabaseEntry();
				OperationStatus os = c.getSearchKeyRange(key, value, null);

				ENTRY: while (os != OperationStatus.NOTFOUND) {
					byte[] currKey = key.getData();
					for (int i = 0; i < idBytes.length; ++i) {
						if (currKey[i] != idBytes[i]) {
							break ENTRY;
						}
					}
					c.delete();

					os = c.getNext(key, value, null);
				}
			} finally {
				c.close();
			}
		} catch (DatabaseException de) {
			throw new TupleStoreException("Error clearing table " + name, de);
		}
	}

	public Map<String, QpSchema> getTables() {
		synchronized (tables) {
			return new HashMap<String,QpSchema>(tableNames);
		}
	}

	public QpSchema getSchema(int tableId) {
		QpSchema result;
		synchronized (tables) {
			result = tables.get(tableId);
		}
		if (result == null) {
			throw new IllegalArgumentException("No table with ID " + tableId);
		}
		return result;
	}

	public QpSchema getSchema(String name) {
		QpSchema result;
		synchronized (tables) {
			result = tableNames.get(name);
		}
		if (result == null) {
			throw new IllegalArgumentException("No table with name " + name);
		}
		return result;
	}

	public TableScan beginScan(int relation, Operator<M> dest, WhichInput outputDest, Predicate keyFilter, Filter<? super QpTuple<M>> fullFilter, Integer lastEpoch,
			int queryId, InetSocketAddress nodeId, int operatorId, RecordTuples rt, boolean enableRecovery, int phaseNo) {
		return new TableScan(relation,dest,outputDest,keyFilter,fullFilter,lastEpoch,queryId,nodeId,operatorId,rt,enableRecovery, phaseNo);
	}

	public TableScan beginScan(String relation, Operator<M> dest, WhichInput outputDest, Predicate keyFilter, Filter<? super QpTuple<M>> fullFilter, Integer lastEpoch,
			int queryId, InetSocketAddress nodeId, int operatorId, RecordTuples rt, boolean enableRecovery, int phaseNo) {
		QpSchema s = getSchema(relation);
		return new TableScan(s.relId,dest,outputDest,keyFilter,fullFilter,lastEpoch,queryId,nodeId,operatorId,rt,enableRecovery, phaseNo);
	}

	private static byte[] getDbKey(QpTupleKey t) {
		QpSchema schema = t.getSchema();
		if (schema.getLocation() == Location.STRIPED) {
			byte[] id = t.getSerializedQpId();
			byte[] retval = new byte[IntType.bytesPerInt + Id.idLengthBytes + t.getSerializedLength()];
			IntType.putBytes(schema.relId, retval, 0);
			System.arraycopy(id, 0, retval, IntType.bytesPerInt, Id.idLengthBytes);
			t.putBytes(retval, IntType.bytesPerInt + Id.idLengthBytes);
			return retval;
		} else {
			byte[] retval = new byte[IntType.bytesPerInt + t.getSerializedLength()];
			IntType.putBytes(schema.relId, retval, 0);
			t.putBytes(retval, IntType.bytesPerInt);
			return retval;
		}
	}

	public class TableScan extends PullScanOperator<M> {
		private final Predicate keyFilter;
		private final Filter<? super QpTuple<M>> fullFilter;
		private final int lastEpoch;
		private final boolean noLastEpoch;
		private final int epochOffset;
		private Cursor c = null;
		private DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();
		private final byte[] relIdBytes;
		private boolean finished = false;
		private final Set<QpTupleKey> alreadyOutputKeys;
		private final QpSchema.Source tables;
		private final QpSchema schema;
		private volatile boolean interrupted = false;
		private final int phaseNo;
		private final int relId;

		private final byte[] contributingNodes;

		private TableScan(final int relation, Operator<M> dest, WhichInput outputDest, Predicate keyFilter, Filter<? super QpTuple<M>> fullFilter, Integer lastEpoch,
				int queryId, InetSocketAddress nodeId, int operatorId, RecordTuples rt, boolean enableRecovery, int phaseNo) {
			super(dest,outputDest,queryId,nodeId,operatorId,rt,BDbTupleStore.this.mdf, BDbTupleStore.this, enableRecovery);

			schema = getSchema(relation);
			tables = new QpSchema.SingleSource(schema);

			if (keyFilter != null && (! schema.getKeyColsSet().containsAll(keyFilter.getColumns()))) {
				throw new IllegalArgumentException("Key filter uses non-key columns");
			}
			this.keyFilter = keyFilter;
			this.fullFilter = fullFilter;
			if (lastEpoch == null) {
				this.lastEpoch = Integer.MAX_VALUE;
				this.noLastEpoch = true;
			} else {
				this.lastEpoch = lastEpoch;
				this.noLastEpoch = false;
			}
			relIdBytes = IntType.getBytes(relation);

			alreadyOutputKeys = new HashSet<QpTupleKey>();
			this.phaseNo = phaseNo;
			relId = relation;

			if (schema.getLocation() == QpSchema.Location.STRIPED) {
				epochOffset = IntType.bytesPerInt + Id.idLengthBytes;
			} else {
				epochOffset = IntType.bytesPerInt;
			}

			if (enableRecovery) {
				contributingNodes = OutputBuffer.getBytes(nodeId);
			} else {
				contributingNodes = null;
			}
		}

		public synchronized boolean isFinished(int phaseNo) {
			return finished;
		}

		@Override
		public int scan(int blockSize, int phaseNo) {
			return scan(blockSize, false, phaseNo);
		}

		@Override
		public int scanAll(int phaseNo) {
			return scan(Integer.MAX_VALUE, true, phaseNo);
		}

		private synchronized int scan(int blockSize, boolean all, int phaseNo) {
			if (finished) {
				throw new RuntimeException("Scan has already finished");
			}
			if (phaseNo != this.phaseNo) {
				throw new IllegalArgumentException("Expected phase " + this.phaseNo + ", got phase " + phaseNo);
			}
			QpTupleBag<M> block = new QpTupleBag<M>(schema, tables, mdf);
			QpTuple<M> t = QpTuple.fromStoreBytes(schema, IntType.getBytes(-1), 0, IntType.bytesPerInt, contributingNodes, phaseNo);
			OperationStatus os = null;
			int numScanned = 0;
			try {
				if (c == null)  {
					c = getDatabase().openCursor(null, null);
					key.setData(IntType.getBytes(relId+1));
					c.getSearchKeyRange(key, value, null);
					os = c.getPrev(key, value, null);
				} else {
					os = c.getPrev(key, value, null);
				}

				ENTRY: while ((! interrupted) && os != OperationStatus.NOTFOUND) {
					byte[] currKey = key.getData();
					for (int i = 0; i < IntType.bytesPerInt; ++i) {
						// Make sure we're still reading the right relation
						if (currKey[i] != relIdBytes[i]) {
							finished = true;
							break ENTRY;
						}
					}

					if (noLastEpoch || IntType.getValFromBytes(currKey, epochOffset) <= lastEpoch) {
						QpTupleKey k = new QpTupleKey(schema, currKey, epochOffset, currKey.length - epochOffset);
						if (keyFilter == null || keyFilter.eval(k)) {
							if (alreadyOutputKeys.add(k.changeEpoch(-1))) {
								byte[] data = value.getData();
								if (data.length == 0) {
									// Tuple deleted
								} else {
									if (mdf == null && fullFilter == null) {
										++numScanned;
										block.addFromStoreBytesWhileChanging(data, null, contributingNodes, phaseNo);
									} else {
										t.changeDataFromStoreBytes(data, 0, data.length, contributingNodes, phaseNo);
										if (fullFilter == null || fullFilter.eval(t)) {
											++numScanned;
											byte[] newMetadata = null;
											if (mdf != null) {
												newMetadata = mdf.toBytes(mdf.scan(this, t, t.getMetadata(tables, mdf)));
											}
											block.addWhileChanging(t, newMetadata, contributingNodes, phaseNo);
										}
									}
								}
								if (block.size() == blockSize) {
									sendTuples(block);
									block.clear();
									if (! all) {
										break;
									}
								}
							}
						}
					}

					os = c.getPrev(key, value, null);
				}

				if (os == OperationStatus.NOTFOUND || interrupted) {
					finished = true;
				}

				if (finished) {
					c.close();
					c = null;
				}

			} catch (Exception e) {
				this.reportException(e);
			}
			if (! block.isEmpty()) {
				sendTuples(block);
			}

			if (finished && (! interrupted)) {
				this.finishedSending(phaseNo);
			}
			return numScanned;
		}

		protected void close() {
			try {
				if (c != null) {
					c.close();
					c = null;
				}
			} catch (DatabaseException de) {
				de.printStackTrace();
			}
		}

		@Override
		public void interrupt() {
			interrupted = true;
		}

		@Override
		public boolean rescanDuringRecovery() {
			return false;
		}
	}

	public void deleteTuple(AbstractTuple<QpSchema> t, int epoch) throws TupleStoreException {
		Database db = getDatabase();
		deleteTuple(t, epoch, db, new DatabaseEntry());
	}

	private static final DatabaseEntry empty = new DatabaseEntry(new byte[0]);

	private void deleteTuple(AbstractTuple<QpSchema> t, int epoch, Database db, DatabaseEntry key) throws TupleStoreException {
		QpTupleKey newKey;
		if (t instanceof AbstractImmutableTuple) {
			newKey = new QpTupleKey((AbstractImmutableTuple<QpSchema>) t, epoch);
		} else {
			newKey = new QpTupleKey(t, epoch);
		}
		key.setData(getDbKey(newKey));
		try {
			db.putNoOverwrite(null, key, empty);
		} catch (DatabaseException e) {
			throw new TupleStoreException("Error deleting tuple " + newKey + " at epoch " + epoch, e);
		}
	}

	public void deleteTuples(Collection<? extends AbstractTuple<QpSchema>> ts, int epoch) throws TupleStoreException {
		Database db = getDatabase();
		DatabaseEntry key = new DatabaseEntry();
		for (AbstractTuple<QpSchema> t : ts) {
			deleteTuple(t, epoch, db, key);
		}
	}

	public void deleteTuples(Collection<QpTupleKey> keys) throws TupleStoreException {
		Database db = getDatabase();
		DatabaseEntry key = new DatabaseEntry();

		for (QpTupleKey keyT : keys) {
			key.setData(getDbKey(keyT));
			try {
				db.putNoOverwrite(null, key, empty);
			} catch (DatabaseException e) {
				throw new TupleStoreException("Error deleting tuple " + keyT + " at epoch " + keyT.epoch, e);
			}
		}

	}


	public ConstraintViolation addTuple(QpTuple<M> t, int epoch) throws TupleStoreException {
		Map<QpTuple<M>,ConstraintViolation> map = addTuples(Collections.singleton(t).iterator(), epoch);
		if (map == null) {
			return null;
		} else {
			return map.get(t);
		}
	}

	public Map<QpTuple<M>,ConstraintViolation> addTuples(Iterator<QpTuple<M>> ts, int epoch) throws TupleStoreException {
		Database db = getDatabase();
		DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();
		Map<QpTuple<M>,ConstraintViolation> retval = null;
		while (ts.hasNext()) {
			final QpTuple<M> t = ts.next();
			if ( ! tables.containsKey(t.getSchema().relId)) {
				throw new IllegalArgumentException("Trying to add tuple from relation " + t.getRelationName() + " to TupleStore for relations " + tables.keySet());
			}
			key.setData(getDbKey(t.getKeyTuple(epoch)));
			value.setData(t.getStoreBytes());
			OperationStatus os;
			try {
				os = db.putNoOverwrite(null, key, value);
				if (os == OperationStatus.KEYEXIST) {
					db.get(null, key, value, null);
					QpTuple<?> old = QpTuple.fromStoreBytes(t.getSchema(), value.getData());
					if (retval == null) {
						retval = new HashMap<QpTuple<M>,ConstraintViolation>();
					}
					QpTuple<M> tt = t.createSelfContainedCopy();
					retval.put(tt, new ConstraintViolation("Key violation, old value: " + old, tt));
				}
			} catch (DatabaseException e) {
				throw new TupleStoreException("Could not put tuple", e);
			}
		}
		return retval;
	}

	public QpTuple<M> getTuple(AbstractTuple<QpSchema> keyTuple, int epoch) throws TupleStoreException {
		return getTuple(keyTuple, epoch, getDatabase());
	}

	private QpTuple<M> getTuple(AbstractTuple<QpSchema> keyTuple, int epoch, Database db) throws TupleStoreException {
		QpTupleKey keyQpTuple;
		if (keyTuple instanceof AbstractImmutableTuple) {
			keyQpTuple = new QpTupleKey((AbstractImmutableTuple<QpSchema>) keyTuple, epoch);
		} else {
			keyQpTuple = new QpTupleKey(keyTuple, epoch);
		}
		DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();

		for (int currEpoch = epoch; currEpoch >= 0; --currEpoch) {
			keyQpTuple = keyQpTuple.changeEpoch(currEpoch);
			key.setData(getDbKey(keyQpTuple));
			OperationStatus os;
			try {
				os = db.get(null, key, value, null);
			} catch (DatabaseException e) {
				throw new TupleStoreException("Error retrieving data for key tuple " + keyTuple + " for epoch " + keyQpTuple.epoch, e);
			}
			if (os == OperationStatus.SUCCESS) {
				byte[] valueBytes = value.getData();
				if (valueBytes.length > 0) {
					return QpTuple.fromStoreBytes(keyTuple.getSchema(), valueBytes);
				} else {
					return null;
				}
			}
		}
		return null;
	}

	public QpTuple<M> getTupleByKey(QpTupleKey keyTuple) throws TupleStoreException {
		return getTupleByKey(keyTuple, this.getDatabase(), new DatabaseEntry(), new DatabaseEntry());
	}

	public List<QpTuple<M>> getTuplesByKey(List<QpTupleKey> keyTuples) throws TupleStoreException {
		final Database db = getDatabase();
		final DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();
		List<QpTuple<M>> retval = new ArrayList<QpTuple<M>>(keyTuples.size());
		for (QpTupleKey k : keyTuples) {
			retval.add(getTupleByKey(k, db, key, value));
		}
		return retval;
	}

	public void getTuplesByKey(Iterator<QpTupleKey> keys, TupleSink<M> dest) throws TupleStoreException {
		final Database db = getDatabase();
		final DatabaseEntry key = new DatabaseEntry(), value = new DatabaseEntry();
		while (keys.hasNext()) {
			final QpTupleKey k = keys.next();
			QpTuple<M> t = getTupleByKey(k, db, key, value);
			if (t == null) {
				dest.tupleNotFound(k);
			} else {
				dest.deliverTuple(t);
			}
		}
	}

	private QpTuple<M> getTupleByKey(QpTupleKey keyTuple, Database db, DatabaseEntry key, DatabaseEntry value) throws TupleStoreException {
		key.setData(getDbKey(keyTuple));
		OperationStatus os;
		try {
			os = db.get(null, key, value, null);
		} catch (DatabaseException e) {
			throw new TupleStoreException("Couldn't retrieve data for key tuple " + keyTuple + " for epoch " + keyTuple.epoch, e);
		}
		if (os == OperationStatus.SUCCESS) {
			byte[] valueBytes = value.getData();
			if (valueBytes.length > 0) {
				return QpTuple.fromStoreBytes(keyTuple.getSchema(), valueBytes);
			}
		}
		return null;
	}

	public synchronized void clear() throws TupleStoreException {
		try {
			tables.clear();
			tableNames.clear();

			dbHandles = new ThreadLocal<Database>();
			for (Database db : openHandles) {
				db.close();
			}
			openHandles.clear();
			e.truncateDatabase(null, dbName, false);
		} catch (DatabaseException de) {
			throw new TupleStoreException("Error clearing BerkeleyDB database", de);
		}
	}

	public void printStats(PrintStream ps)
	throws TupleStoreException {
		try {
			StatsConfig config = new StatsConfig();
			config.setClear(true);
			ps.println("BerkeleyDB statistics:");
			ps.println(e.getStats(config));
		} catch (DatabaseException de) {
			throw new TupleStoreException("Error requesting statistics from BerkeleyDB", de);
		}
	}

	public PullScanOperator<M> beginScan(int relation, ByteArraySet keys, Filter<? super QpTuple<M>> fullFilter, Operator<M> componentOf, IdRangeSet ranges, int phaseNo) {
		return new SpecifiedKeyScan(relation, keys, fullFilter, componentOf, ranges, phaseNo);
	}

	private class SpecifiedKeyScan extends PullScanOperator<M> {
		private final byte[] relId;
		private Cursor c;
		private DatabaseEntry key, value;
		private boolean finished = false;
		private final QpSchema.Source tables;
		private final ByteArraySet keys;
		private final Filter<? super QpTuple<M>> fullFilter;
		private final int keysSize;
		private int keysScanned = 0;
		private final QpSchema schema;
		private volatile boolean interrupted = false;
		private final IdRangeSet ranges;
		private final Iterator<IdRange> rangesIt;
		private IdRange currentRange;
		private final int phaseNo;
		private boolean wrapped = false;
		private final byte[] contributingNodes;

		SpecifiedKeyScan(int relation,ByteArraySet keys, Filter<? super QpTuple<M>> fullFilter, Operator<M> componentOf,
				IdRangeSet ranges, int phaseNo) {
			super(componentOf);

			relId = IntType.getBytes(relation);
			schema = getSchema(relation);
			if (schema.getLocation() != QpSchema.Location.STRIPED) {
				throw new IllegalArgumentException("Can only do a SpecifiedKeyScan for a striped relation");
			}
			tables = new QpSchema.SingleSource(schema);
			this.keys = keys;
			this.fullFilter = fullFilter;
			keysSize = keys.size();
			this.ranges = ranges.clone();
			rangesIt = ranges.iterator();
			currentRange = null;
			this.phaseNo = phaseNo;
			if (ranges.isEmpty()) {
				throw new IllegalArgumentException("Tuple range cannot be empty");
			}
			if (keys.isEmpty()) {
				throw new IllegalArgumentException("Set of keys should not be empty");
			}
			if (logger.isTraceEnabled()) {
				logger.trace("SpecifiedKeyScan " + this.operatorId + " for ranges " + ranges + " of relation " + schema.getName() + " in phase "  + phaseNo + " was constructed for " + keys.size());
			}
			if (enableRecovery) {
				contributingNodes = OutputBuffer.getBytes(nodeId);
			} else {
				contributingNodes = null;
			}
		}

		@Override
		public synchronized boolean isFinished(int phaseNo) {
			return finished;
		}

		@Override
		public int scan(int blockSize, int phaseNo) {
			return scan(blockSize, false, phaseNo);
		}

		@Override
		public int scanAll(int phaseNo) {
			return scan(Integer.MAX_VALUE, true, phaseNo);
		}
		
		private OperationStatus goToNextRange() throws DatabaseException {
			wrapped = false;
			if (! rangesIt.hasNext()) {
				finished = true;
				return null;
			}
			currentRange = rangesIt.next();
			byte[] searchKey = new byte[IntType.bytesPerInt + Id.idLengthBytes];
			System.arraycopy(relId, 0, searchKey, 0, relId.length);
			if (currentRange.isFull()) {
				Id.ZERO.copyIntoMSB(searchKey, relId.length);
			} else {
				currentRange.getCCW().copyIntoMSB(searchKey, relId.length);
			}
			key.setData(searchKey);
			return c.getSearchKeyRange(key, value, null);
		}

		private synchronized int scan(int blockSize, boolean all, int phaseNo) {
			if (phaseNo != this.phaseNo) {
				throw new IllegalArgumentException("Expected phase " + this.phaseNo + ", got phase " + phaseNo);
			}
			int numScanned = 0;
			try {
				if (interrupted) {
					throw new IllegalStateException("Scan has been interrupted");
				}
				if (finished) {
					throw new IllegalStateException("Scan has already finished");
				}
				OperationStatus os;
				if (c == null) {
					c = getDatabase().openCursor(null, null);
					key = new DatabaseEntry();
					value = new DatabaseEntry();
					os = goToNextRange();
				} else {
					os = c.getNext(key, value, null);
				}
				QpTupleBag<M> block = new QpTupleBag<M>(schema, BDbTupleStore.this, mdf);
				QpTuple<M> t = QpTuple.fromStoreBytes(schema, IntType.getBytes(-1), 0, IntType.bytesPerInt, contributingNodes, phaseNo);
				while (! interrupted) {
					boolean hitEnd = false;
					byte[] keyBytes = key.getData();
					if (os == OperationStatus.NOTFOUND) {
						hitEnd = true;
					} else {
						for (int i = 0; i < relId.length; ++i) {
							if (relId[i] != keyBytes[i]) {
								hitEnd = true;
							}
						}
					}

					if (hitEnd && wrapped) {
						os = goToNextRange();
						if (finished) {
							break;
						} else {
							continue;
						}
					} else if (hitEnd) {
						if (currentRange.wraps()) {
							wrapped = true;
							key.setData(relId);
							// Get back to the start of the relation
							os = c.getSearchKeyRange(key, value, null);
							continue;
						} else {
							os = goToNextRange();
							if (finished) {
								break;
							} else {
								continue;
							}
						}
					}

					if (! currentRange.containsMSB(keyBytes, relId.length)) {
						os = goToNextRange();
						if (finished) {
							break;
						} else {
							continue;
						}
					}

					if (keys.remove(keyBytes, IntType.bytesPerInt + Id.idLengthBytes, keyBytes.length - IntType.bytesPerInt - Id.idLengthBytes)) {
						++keysScanned;
						byte[] data = value.getData();
						if (mdf == null && fullFilter == null) {
							++numScanned;
							block.addFromStoreBytesWhileChanging(data, null, contributingNodes, phaseNo);
						} else {
							t.changeDataFromStoreBytes(data, 0, data.length, contributingNodes, phaseNo);
							if (fullFilter == null || fullFilter.eval(t)) {
								byte[] newMetadata = null;
								if (mdf != null) {
									newMetadata = mdf.toBytes(mdf.scan(this, t, t.getMetadata(tables, mdf)));
								}
								++numScanned;
								block.addWhileChanging(t, newMetadata, contributingNodes, phaseNo);
							}
						}
					}

					if (block.size() == blockSize) {
						sendTuples(block);
						block.clear();
						if (! all) {
							break;
						}
					}
					if (keys.isEmpty()) {
						finished = true;
						break;
					}
					os = c.getNext(this.key, value, null);
				}
				if (! block.isEmpty()) {
					sendTuples(block);
				}
				if (finished || interrupted) {
					c.close();
					c = null;
				}
				if (finished) {
					if (! keys.isEmpty()) {
						logger.error("SpecifiedKeyScan " + this.operatorId + " for ranges " + ranges + " of relation " + schema.getName() + " in phase "  + phaseNo + " missed " + keys.size() + " keys");
						List<QpTupleKey> missing = new ArrayList<QpTupleKey>(keys.size());
						ByteArraySet.Deserializer<QpTupleKey> d = new ByteArraySet.Deserializer<QpTupleKey>() {

							@Override
							public QpTupleKey fromBytes(byte[] data,
									int offset, int length) {
								return QpTupleKey.fromBytes(schema, data, offset, length);
							}
							
						};
						Iterator<QpTupleKey> it = keys.iterator(d);
						while (it.hasNext()) {
							missing.add(it.next());
						}
						reportMissing(missing, phaseNo);
					} else if (keysSize != keysScanned) {
						logger.error("SpecifiedKeyScan " + this.operatorId + " for ranges " + ranges + " of relation " + schema.getName() + " in phase "  + phaseNo + " was scanning for " + keysSize + " keys but read " + keysScanned);
					} else if (logger.isTraceEnabled()) {
						logger.trace("SpecifiedKeyScan " + this.operatorId + " for ranges " + ranges + " of relation " + schema.getName() + " in phase "  + phaseNo + " found all " + keysScanned + " keys");
					}
				}
			} catch (Exception e) {
				if (c != null) {
					try {
						c.close();
						c = null;
					} catch (DatabaseException de) {
						logger.error("Error closing SpecifiedKeyScan cursor", de);
					}
				}
				finished = true;				
				reportException(e);
			}
			return numScanned;
		}

		protected void close() {
			try {
				if (c != null) {
					c.close();
					c = null;
					keys.clear();
				}
			} catch (DatabaseException de) {
				de.printStackTrace();
			}
		}

		public void interrupt() {
			interrupted = true;
			logger.warn("SpecifiedKeyScan " + this.operatorId + " for ranges " + ranges + " of relation " + schema.getName() + " in phase "  + phaseNo + " was interrupted");
		}

		@Override
		public boolean rescanDuringRecovery() {
			throw new IllegalStateException("This operator should be a component of a DistributedScanOperator");
		}
	}
}
