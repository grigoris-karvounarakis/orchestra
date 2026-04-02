package edu.upenn.cis.orchestra.p2pqp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.Date;
import edu.upenn.cis.orchestra.datamodel.DateType;
import edu.upenn.cis.orchestra.datamodel.DoubleType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.LongType;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.optimization.Aggregate.AggFunc;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator.ColumnInput;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator.ColumnOrFunction;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator.EvalFunc;
import edu.upenn.cis.orchestra.p2pqp.HashAggregator.AggColumn;
import edu.upenn.cis.orchestra.p2pqp.HashAggregator.OutputColumn;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.messages.DummyMessage;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.util.ByteArraySet;
import edu.upenn.cis.orchestra.util.LongList;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class Calibrate {
	static final int numFuncTrials = 10;
	static final int numPredTrials = 10;
	static final int numAggTrials = 10;
	static final int numJoinTrials = 5;
	static final int numIdTrials = 10;
	static final int numMsgs = 10000;
	static final int numMsgTrials = 10;
	static final int numProbeTrials = 5;
	static final int probeTrialSize = 5000;
	static final int numScanTrials = 3;
	static final int numIndexedScanTrials = 20;
	static final int numIndexTrials = 5;
	static final int numSerializationTrials = 25;
	static final int serializationTrialSize = 5000;

	static final int numRetrieveTrials = 100;
	static final int retrieveTrialSize = 5000;

	private static int numPages = 4000;
	private static int numStoreCopies = 1000;
	private static long cacheSizeMB = 128;


	private static NumberFormat nf = NumberFormat.getInstance();
	static {
		nf.setMaximumFractionDigits(2);
	}


	public static void main(String[] args) {
		System.out.println("Calibrating...");
		String argExp = "Arguments: [-o outFile] [-i numIndexPages] [-s numStoreCopies] [-c cacheSizeMB]";
		try {
			if (args.length % 2 != 0) {
				System.err.println(argExp);
				System.exit(-1);
			}
			String outFilename = null;
			for (int i = 0; i < args.length; i += 2) {
				String flag = args[i], value = args[i+1];
				if (flag.equals("-o") || flag.equals("-out")) {
					outFilename = value;
				} else if (flag.equals("-i") || flag.equals("-indexPages")) {
					numPages = Integer.parseInt(value);
				} else if (flag.equals("-s") || flag.equals("-storeCopies")) {
					numStoreCopies = Integer.parseInt(value);
				} else if (flag.equals("-c") || flag.equals("-cacheSize")) {
					cacheSizeMB = Integer.parseInt(value);
				} else {
					System.err.println(argExp);
					System.exit(-1);
				}
			}
			
			// Cache size + 1GB
			long neededMemory = cacheSizeMB * 1024 * 1024 + 1024 * 1024 * 1024;
			long availMemory = Runtime.getRuntime().maxMemory();
			if (neededMemory > availMemory) {
				System.err.println("Need at least " + neededMemory + " bytes of memory, but only " + availMemory + " bytes are available, adjust -Xmx or equivalent parameter to JRE");
				System.exit(-1);
			}
			
			SystemCalibration sc = generateLocalCalibration();
			if (outFilename != null) {
				ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(outFilename));
				o.writeObject(sc);
				o.close();
			}
			System.err.println(sc);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static SystemCalibration generateLocalCalibration() throws Exception {		
		final double msgsPerSecond = 1 / msgSendTime();
		System.out.println("Messages sent per second: " + msgsPerSecond);
		
		RecordTuples rt = new RecordTuplesNothing();
		InetSocketAddress id = new InetSocketAddress(InetAddress.getLocalHost(), 5000);

		MetadataFactory<Null> mdf = null;

		QpSchema r = new QpSchema("R", 1);
		QpSchema s = new QpSchema("S", 2);
		QpSchema tS = new QpSchema("T", 3);
		QpSchema funcOut = new QpSchema("funcOut", 3);
		QpSchema aggOut1 = new QpSchema("aggOut1", 4);
		QpSchema aggOut2 = new QpSchema("aggOut2", 5);
		r.addCol("a", IntType.INT);
		r.addCol("b", IntType.INT);
		r.addCol("c", IntType.INT);
		r.addCol("d", DoubleType.DOUBLE);
		r.addCol("e", DoubleType.DOUBLE);
		r.addCol("g", new StringType(false, false, true, 400));
		r.setPrimaryKey(new PrimaryKey("pk", r, Collections.singleton("a")));
		r.setHashCols(new int[] {0});
		funcOut.addCol("a", IntType.INT);
		funcOut.addCol("bc", IntType.INT);
		funcOut.addCol("de", DoubleType.DOUBLE);
		funcOut.setPrimaryKey(new PrimaryKey("pk", funcOut, Collections.singleton("a")));
		aggOut1.addCol("a", IntType.INT);
		aggOut1.setPrimaryKey(new PrimaryKey("pk", aggOut1, Collections.singleton("a")));
		aggOut2.addCol("a", IntType.INT);
		aggOut2.addCol("maxb", IntType.INT);
		aggOut2.addCol("mind", DoubleType.DOUBLE);
		aggOut2.setPrimaryKey(new PrimaryKey("pk", aggOut2, Collections.singleton("a")));
		s.addCol("a", IntType.INT);
		s.addCol("f", IntType.INT);
		s.setPrimaryKey(new PrimaryKey("pk", s, Collections.singleton("a")));
		tS.addCol("a", IntType.INT);
		tS.addCol("b", DoubleType.DOUBLE);
		tS.addCol("c", new StringType(false, false, true, 400));
		tS.setPrimaryKey(new PrimaryKey("pk", r, Collections.singleton("a")));
		tS.setHashCols(new int[] {0});
		r.markFinished();
		funcOut.markFinished();
		aggOut1.markFinished();
		aggOut2.markFinished();
		s.markFinished();
		tS.markFinished();

		QpSchema.Source schemas = new QpSchema.CollectionSource(Arrays.asList(r, s));
		Sink sink = new Sink(id, schemas);


		List<ColumnOrFunction> copyFuncCols = new ArrayList<ColumnOrFunction>(3);
		copyFuncCols.add(new ColumnInput(0));
		copyFuncCols.add(new ColumnInput(1));
		copyFuncCols.add(new ColumnInput(3));


		List<ColumnOrFunction> evalFuncCols = new ArrayList<ColumnOrFunction>(3);
		evalFuncCols.add(new ColumnInput(0));
		List<ColumnOrFunction> sumInput1 = new ArrayList<ColumnOrFunction>(2);
		List<ColumnOrFunction> sumInput2 = new ArrayList<ColumnOrFunction>(2);
		sumInput1.add(new ColumnInput(1));
		sumInput1.add(new ColumnInput(2));
		sumInput2.add(new ColumnInput(3));
		sumInput2.add(new ColumnInput(4));
		List<Integer> mults = Arrays.asList(2, 2);
		evalFuncCols.add(new EvalFunc(new Sum(mults,true,false,false), sumInput1));
		evalFuncCols.add(new EvalFunc(new Sum(mults,true,true,false), sumInput2));

		QpTuple<Null> rTuple = null;
		QpTupleBag<Null> rTuples = new QpTupleBag<Null>(r, schemas, mdf);
		List<QpTuple<Null>> sTuples = new ArrayList<QpTuple<Null>>(1000);
		Object[] rFields = new Object[6], sFields = new Object[2];

		int rCount = 0;

		for (int i = 0; i < 1000; ++i) {
			rFields[0] = i;
			rFields[1] = 2 * i;
			rFields[2] = i + 1;
			rFields[3] = i * 2.0;
			rFields[4] = i + 1.0;
			rFields[5] = "To Sherlock Holmes she is always THE woman.";
			rTuple = new QpTuple<Null>(r, 0, rFields);
			++rCount;
			rTuples.add(rTuple);
			sFields[0] = i;
			sFields[1] = 5000 + i;
			QpTuple<Null> sTuple = new QpTuple<Null>(s, 0, sFields);
			sTuples.add(sTuple);
		}			

		File envLoc = new File("dbenv");
		cleanDirectory(envLoc);
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setReadOnly(false);
		ec.setCacheSize(cacheSizeMB * 1024 * 1024);
		Environment env = new Environment(envLoc, ec);
		TupleStore<Null> ts = new BDbTupleStore<Null>(env, "ts", null);

		ts.addTable(r);
		ts.addTable(tS);
		AbstractRelation.FieldSource[] fs = new AbstractRelation.FieldSource[r.getNumCols()];
		for (int i = 0; i < r.getNumCols(); ++i) {
			fs[i] = new AbstractRelation.FieldSource(i, true);
		}
		List<QpTupleKey> stored = new ArrayList<QpTupleKey>(rTuples.size() * 1000);
		final int numStoreEpochs = numStoreCopies * 5;
		for (QpTuple<Null> t : rTuples) {
			ts.addTuple(t, 0);
			int tupleCount = 0;

			for (int epoch = 1; epoch < numStoreEpochs; ++epoch) {
				if (tupleCount % 5 == epoch % 5) {
					ts.addTuple(t, epoch);
					++rCount;
					stored.add(t.getKeyTuple(epoch));
				}
			}
			ts.deleteTuple(t,5000);
			++rCount;
			++tupleCount;
		}

		List<KeyAndId> tKeys = new ArrayList<KeyAndId>();

		int tCount = 0;
		Object[] tFields = new Object[3];
		for (int i = 0; i < 100000; ++i) {
			tFields[0] = i;
			tFields[1] = 2.0 * i;
			tFields[2] = "It was the best of times, it was the worst of times, it was the age of wisdom, it was the age of foolishness, it was the epoch of belief, it was the epoch of incredulity, it was the season of Light, it was the season of Darkness...";
			QpTuple<Null> tTuple = new QpTuple<Null>(tS, tFields);
			ts.addTuple(tTuple, 0);
			tKeys.add(new KeyAndId(tTuple.getKeyTuple(0)));
			++tCount;
		}

		System.out.println("Stored (all versions) " + rCount + " R tuples and " + tCount + " T tuples");

		Collections.sort(tKeys);


		ts.close();

		DatabaseConfig dc = new DatabaseConfig();
		dc.setAllowCreate(true);
		dc.setSortedDuplicates(false);
		Database db = env.openDatabase(null, "index", dc);

		ByteBufferWriter bbw = new ByteBufferWriter();
		byte[] tupleData = rTuple.getKeyTuple(0).getBytes();
		for (int i = 0; i < DHTService.numTuplesPerPage; ++i) {
			bbw.addToBuffer(tupleData);
		}

		byte[] data = bbw.getByteArray();

		DatabaseEntry dataE = new DatabaseEntry(data);
		DatabaseEntry keyE = new DatabaseEntry();
		for (int i = 0; i < numPages; ++i) {
			keyE.setData(IntType.getBytes(i));
			db.put(null, keyE, dataE);
		}

		System.out.println("Index contains " + db.count() + " pages");

		db.close();
		env.close();
		System.gc();
		db = null;
		env = null;
		ec.setReadOnly(true);
		ec.setAllowCreate(false);
		dc.setAllowCreate(false);
		dc.setReadOnly(true);

		List<Double> probeTimes = new ArrayList<Double>(numProbeTrials);
		for (int i = 0; i < numProbeTrials; ++i) {
			env = new Environment(envLoc,ec);
			ts = new BDbTupleStore<Null>(env, "ts", null);
			ts.addTable(r);
			Collections.shuffle(stored);
			System.gc();
			long startTime = System.nanoTime();
			for (QpTupleKey t : stored.subList(0, probeTrialSize)) {
				ts.getTupleByKey(t);
			}
			long endTime = System.nanoTime();
			double time = (double) endTime - startTime;
			probeTimes.add(time);
			ts.close();
			env.close();
			System.out.println("Probe #" + (i + 1) + " took " + (time / 1.0e9) + " seconds");
		}

		stored = null;

		double probeTime = computeAvg(probeTimes);
		double probesPerSecond = probeTrialSize / (probeTime / 1.0e9);
		System.out.println("Store probes per second: " + nf.format(probesPerSecond));

		Operator<Null> dummyOperator = new Operator<Null>(sink, WhichInput.LEFT, id, 0, null, null, rt, false) {
			@Override
			protected void receiveTuples(WhichInput destInput, QpTupleBag<Null> tuples) {
			}

			@Override
			protected void inputHasFinished(WhichInput whichChild, int phaseNo) {
			}
	};

		// TODO: account for skipped tuples somehow, otherwise
		// we'll always favor indexed scans over probing
		final int indexedScanSize = DHTService.numTuplesPerPage;
		List<Double> emptyIndexedScanTimes = new ArrayList<Double>(numIndexedScanTrials);
		for (int i = 0; i < numIndexedScanTrials; ++i) {
			env = new Environment(envLoc,ec);
			ts = new BDbTupleStore<Null>(env, "ts", null);
			ts.addTable(tS);

			int index = new Random().nextInt(tKeys.size());			
			ByteArraySet keys = new ByteArraySet(indexedScanSize);
			Id min = tKeys.get(index).id;
			while (keys.size() < indexedScanSize) {
				keys.add(tKeys.get(index).key.getBytes());
				++index;
				if (index >= tKeys.size()) {
					index = 0;
				}
			}
			Id max = tKeys.get(index).id;
			IdRange range = new IdRange(min, max);
			IdRangeSet ranges = IdRangeSet.empty();
			ranges.add(range);

			System.gc();
			long startTime = System.nanoTime();

			PullScanOperator<Null> op = ts.beginScan(tS.relId, keys, null, dummyOperator, ranges, 0);
			op.scanAll(0);
			long endTime = System.nanoTime();
			double time = (double)endTime - startTime;
			emptyIndexedScanTimes.add(time);

			System.out.println("Indexed scan for " + indexedScanSize + " keys #" + (i + 1) + " took " + (time / 1.0e9) + " seconds");
			ts.close();
			env.close();
		}

		double indexedScanReadPerSecond = indexedScanSize / (computeAvg(emptyIndexedScanTimes) / 1.0e9);

		System.out.println("Indexed scan tuples read per second: " + nf.format(indexedScanReadPerSecond));

		List<Double> scanKeyFailsTimes = new ArrayList<Double>(numScanTrials);
		List<Double> scanNoOutputTimes = new ArrayList<Double>(numScanTrials);
		for (int i = 0; i < numScanTrials; ++i) {
			env = new Environment(envLoc,ec);
			ts = new BDbTupleStore<Null>(env, "ts", null);
			ts.addTable(r);
			System.gc();
			long startTime = System.nanoTime();
			PullScanOperator<Null> op = ts.beginScan("R", sink, WhichInput.LEFT, new FalseFilter(), null, 500, 5, id, 5, rt, false, 0);
			op.scanAll(0);
			long endTime = System.nanoTime();
			double time = (double)endTime - startTime;
			scanKeyFailsTimes.add(time);
			System.out.println("Scan key fails #" + (i+1) + " took " + (time / 1.0e9) + " seconds");

			ts.close();
			env.close();
			env = new Environment(envLoc,ec);
			ts = new BDbTupleStore<Null>(env, "ts", null);
			ts.addTable(r);

			System.gc();
			startTime = System.nanoTime();
			op = ts.beginScan("R", sink, WhichInput.LEFT, null, null, 500, 5, id, 5, rt, false, 0);
			op.scanAll(0);
			endTime = System.nanoTime();
			time = (double)endTime - startTime;
			scanNoOutputTimes.add(time);
			System.out.println("Scan no output #" + (i+1) + " took " + (time / 1.0e9) + " seconds");
			ts.close();
			env.close();
		}

		double scanKeyFailsTime = computeAvg(scanKeyFailsTimes);
		double versionedScanFailedKeyFilterPerSecond = rCount / (scanKeyFailsTime / 1.0e9);
		System.out.println("Versioned scanned tuples when key filter fails per second: " + nf.format(versionedScanFailedKeyFilterPerSecond));

		double scanNoOutputTime = computeAvg(scanNoOutputTimes);
		double versionedScanPassedKeyFilterPerSecond = rCount / ((scanNoOutputTime) / 1.0e9);
		System.out.println("Versioned scanned tuples with key filter passing but no output per second: " + nf.format(versionedScanPassedKeyFilterPerSecond));

		List<Double> indexLookupTimes = new ArrayList<Double>(numIndexTrials);
		for (int i = 0; i < numIndexTrials; ++i) {
			List<Integer> keys = new ArrayList<Integer>(numPages);
			for (int j = 0; j < numPages; ++j) {
				keys.add(j);
			}
			Collections.shuffle(keys);

			env = new Environment(envLoc,ec);
			db = env.openDatabase(null, "index", dc);
			System.gc();
			long startTime = System.nanoTime();
			for (int key : keys) {
				DatabaseEntry de = new DatabaseEntry(IntType.getBytes(key));
				DatabaseEntry value = new DatabaseEntry();
				db.get(null, de, value, null);
			}
			long endTime = System.nanoTime();
			double time = (double) endTime - startTime;
			indexLookupTimes.add(time);
			System.out.println("Index lookups #" + (i+1) + " took " + (time / 1.0e9) + " seconds");
			db.close();
			env.close();
		}
		double indexLookupTime = computeAvg(indexLookupTimes);
		double indexLookupsPerSecond = numPages / (indexLookupTime / 1.0e9);
		System.out.println("Index lookups per second: " + nf.format(indexLookupsPerSecond));

		FunctionEvaluator<Null> copyFe = new FunctionEvaluator<Null>(r, funcOut, copyFuncCols, sink, WhichInput.LEFT, id, 1, mdf, schemas, rt, false);
		List<Double> funcCopyTimes = new ArrayList<Double>(numFuncTrials);
		FunctionEvaluator<Null> fe = new FunctionEvaluator<Null>(r, funcOut, evalFuncCols, sink, WhichInput.LEFT, id, 1, mdf, schemas, rt, false);
		List<Double> funcEvalTimes = new ArrayList<Double>(numFuncTrials);
		for (int i = 0; i < numFuncTrials; ++i) {
			List<QpTupleBag<Null>> tuplesCopies = new ArrayList<QpTupleBag<Null>>();
			for (int j = 0; j < 1000; ++j) {
				tuplesCopies.add(rTuples.clone());
			}
			System.gc();
			long startTime = System.nanoTime();
			for (QpTupleBag<Null> tuples : tuplesCopies) {
				copyFe.receiveTuples(null, tuples);
			}
			long endTime = System.nanoTime();

			funcCopyTimes.add((double)endTime - startTime);

			tuplesCopies.clear();
			for (int j = 0; j < 1000; ++j) {
				tuplesCopies.add(rTuples.clone());
			}

			System.gc();
			startTime = System.nanoTime();
			for (QpTupleBag<Null> tuples : tuplesCopies) {
				fe.receiveTuples(null, tuples);
			}
			endTime = System.nanoTime();
			funcEvalTimes.add((double) endTime - startTime);
		}
		double funcCopyTime = computeAvg(funcCopyTimes);
		double funcTuplesPerSecond = 1000 * 1000 / (funcCopyTime / 1.0e9);
		System.out.println("Function tuples per second: " + nf.format(funcTuplesPerSecond));

		double funcEvalTime = computeAvg(funcEvalTimes) - funcCopyTime;
		double funcsPerSecond;
		if (funcEvalTime < 0) {
			funcsPerSecond = Double.POSITIVE_INFINITY;
		} else {
			funcsPerSecond = 1000 * 1000 / (funcEvalTime / 1.0e9);
		}
		System.out.println("Funcs per second: " + nf.format(funcsPerSecond));

		
		ComparePredicate cp = ComparePredicate.createColLit(r, "c", ComparePredicate.Op.LT, 5000);


		FilterOperator<Null> filterInput = new FilterOperator<Null>(Collections.singletonList(new FalseFilter()), sink, WhichInput.LEFT, id, 0, mdf, schemas, rt, false);
		FilterOperator<Null> filterOutput = new FilterOperator<Null>(Collections.singletonList(new TrueFilter()), sink, WhichInput.LEFT, id, 0, mdf, schemas, rt, false);
		FilterOperator<Null> predFilterOp = new FilterOperator<Null>(cp, sink, WhichInput.LEFT, id, 0, mdf, schemas, rt, false);


		List<Double> predInputTuplesTimes = new ArrayList<Double>(numPredTrials);
		List<Double> predOutputTuplesTimes = new ArrayList<Double>(numPredTrials);
		List<Double> predTimes = new ArrayList<Double>(numPredTrials);

		for (int i = 0; i < numPredTrials; ++i) {
			List<QpTupleBag<Null>> tuplesCopies = new ArrayList<QpTupleBag<Null>>();
			for (int j = 0; j < 1000; ++j) {
				tuplesCopies.add(rTuples.clone());
			}

			System.gc();
			long startTime = System.nanoTime();
			for (QpTupleBag<Null> tuples : tuplesCopies) {
				filterInput.receiveTuples(null, tuples);
			}
			long endTime = System.nanoTime();
			predInputTuplesTimes.add((double) endTime - startTime);
			
			tuplesCopies.clear();
			for (int j = 0; j < 1000; ++j) {
				tuplesCopies.add(rTuples.clone());
			}


			System.gc();
			startTime = System.nanoTime();
			for (QpTupleBag<Null> tuples : tuplesCopies) {
				filterOutput.receiveTuples(null, tuples);
			}
			endTime = System.nanoTime();

			predOutputTuplesTimes.add((double)endTime - startTime);

			tuplesCopies.clear();
			for (int j = 0; j < 1000; ++j) {
				tuplesCopies.add(rTuples.clone());
			}

			System.gc();
			startTime = System.nanoTime();
			for (QpTupleBag<Null> tuples : tuplesCopies) {
				predFilterOp.receiveTuples(null, tuples);
			}
			endTime = System.nanoTime();
			predTimes.add((double) endTime - startTime);
		}

		double predInputTuplesTime = computeAvg(predInputTuplesTimes);
		double predInputTuplesPerSecond = 1000 * 1000 / (predInputTuplesTime / 1.0e9);
		System.out.println("Predicate tuples input per second: " + nf.format(predInputTuplesPerSecond));

		double predOutputTuplesTime = computeAvg(predOutputTuplesTimes) - predInputTuplesTime;
		double predOutputTuplesPerSecond = 1000 * 1000 / (predOutputTuplesTime / 1.0e9);
		System.out.println("Predicate tuples output per second: " + nf.format(predOutputTuplesPerSecond));

		double predTime = computeAvg(predTimes) - predInputTuplesTime - predOutputTuplesTime;
		double predsPerSecond;
		if (predTime < 0) {
			predsPerSecond = Double.POSITIVE_INFINITY;
		} else {
			predsPerSecond = 1000 * 1000 / (predTime / 1.0e9);
		}
		System.out.println("Preds per second: " + nf.format(predsPerSecond));


		List<Double> aggTimes = new ArrayList<Double>(numAggTrials);
		List<Double> aggFuncTimes = new ArrayList<Double>(numAggTrials);

		for (int i = 0; i < numAggTrials; ++i) {
			List<OutputColumn> ocs = new ArrayList<OutputColumn>();
			ocs.add(new OutputColumn(0));
			HashAggregator<Null> ha = new HashAggregator<Null>(sink, WhichInput.LEFT, r, aggOut1, Collections.singletonList(0), ocs, id, 0, rt, mdf, schemas, null, null);
			List<QpTupleBag<Null>> tuplesCopies = new ArrayList<QpTupleBag<Null>>();
			for (int j = 0; j < 1000; ++j) {
				tuplesCopies.add(rTuples.clone());
			}
			System.gc();
			long startTime = System.nanoTime();
			for (QpTupleBag<Null> tuples : tuplesCopies) {
				ha.receiveTuples(null, tuples);
			}
			ha.inputHasFinished(null, 0);
			long endTime = System.nanoTime();
			aggTimes.add((double) endTime - startTime);

			ocs.clear();
			ocs.add(new OutputColumn(0));
			ocs.add(new AggColumn(1, AggFunc.MAX));
			ocs.add(new AggColumn(3, AggFunc.MIN));
			ha = new HashAggregator<Null>(sink, WhichInput.LEFT, r, aggOut2, Collections.singletonList(0), ocs, id, 0, rt, mdf, schemas, null, null);

			tuplesCopies.clear();
			for (int j = 0; j < 1000; ++j) {
				tuplesCopies.add(rTuples.clone());
			}

			System.gc();
			startTime = System.nanoTime();
			for (QpTupleBag<Null> tuples : tuplesCopies) {
				ha.receiveTuples(null, tuples);
			}
			ha.inputHasFinished(null, 0);
			endTime = System.nanoTime();
			aggFuncTimes.add((double) endTime - startTime);

		}

		double aggTime = computeAvg(aggTimes);
		double aggInputRowsPerSecond = 1000 * 1000 / (aggTime / 1.0e9);
		System.out.println("Aggregate input rows per second: " + nf.format(aggInputRowsPerSecond));

		double aggFuncTime = computeAvg(aggFuncTimes) - aggTime;
		double aggFuncsPerSecond;
		if (aggFuncTime < 0) {
			aggFuncsPerSecond = Double.POSITIVE_INFINITY;
		} else {
			aggFuncsPerSecond = 2 * 1000 * 1000 / (aggFuncTime / 1.0e9);
		}
		System.out.println("Aggregate funcs * rows per second: " + nf.format(aggFuncsPerSecond));

		List<Double> idTimes = new ArrayList<Double>(numIdTrials);
		for (int i = 0; i < numIdTrials; ++i) {
			System.gc();
			long startTime = System.nanoTime();
			for (int j = 0; j < 1000; ++j) {
				for (QpTuple<Null> t : rTuples) {
					t.getQPid();
				}
			}
			long endTime = System.nanoTime();

			idTimes.add((double)endTime - startTime);
		}
		double idTime = computeAvg(idTimes);
		double tupleIdsPerSecond = 1000 * 1000 / (idTime / 1.0e9);

		System.out.println("Tuple ids per second: " + nf.format(tupleIdsPerSecond));
		
		List<Integer> leftOutputPos = new ArrayList<Integer>(), rightOutputPos = new ArrayList<Integer>();
		leftOutputPos.add(0);
		leftOutputPos.add(1);
		leftOutputPos.add(null);
		leftOutputPos.add(3);
		leftOutputPos.add(4);
		leftOutputPos.add(5);
		rightOutputPos.add(null);
		rightOutputPos.add(2);

		List<Double> joinStoreTimes = new ArrayList<Double>(numJoinTrials);
		List<Double> joinProduceTimes = new ArrayList<Double>(numJoinTrials);
		int storeOutputCount = 0, produceOutputCount = 0;

		for (int i = 0; i < numJoinTrials; ++i) {
			sink.reset();
			PipelinedHashJoin<Null> phj = new PipelinedHashJoin<Null>(r, s, Collections.singletonList(0), Collections.singletonList(1),
					leftOutputPos, rightOutputPos, r, 1000, sink, WhichInput.LEFT, id, 0, rt, mdf, schemas, null);

			List<QpTupleBag<Null>> tuplesCopies = new ArrayList<QpTupleBag<Null>>();
			for (int j = 0; j < 1000; ++j) {
				tuplesCopies.add(rTuples.clone());
			}

			QpTupleBag<Null> sTuplesCopy = new QpTupleBag<Null>(s, schemas, mdf);

			System.gc();
			long startTime = System.nanoTime();
			phj.receiveTuples(WhichInput.RIGHT, sTuplesCopy);
			for (QpTupleBag<Null> tuples : tuplesCopies) {
				phj.receiveTuples(WhichInput.LEFT, tuples);
			}
			long endTime = System.nanoTime();

			joinStoreTimes.add((double) endTime - startTime);

			storeOutputCount = sink.getCount();

			sink.reset();
			phj = new PipelinedHashJoin<Null>(r, s, Collections.singletonList(0), Collections.singletonList(0),
					leftOutputPos, rightOutputPos, r, 1000, sink, WhichInput.LEFT, id, 0, rt, mdf, schemas, null);

			tuplesCopies.clear();
			for (int j = 0; j < 1000; ++j) {
				tuplesCopies.add(rTuples.clone());
			}
			sTuplesCopy.clear();
			for (QpTuple<Null> t : sTuples) {
				sTuplesCopy.add(t);
			}

			System.gc();
			startTime = System.nanoTime();
			phj.receiveTuples(WhichInput.RIGHT, sTuplesCopy);
			for (QpTupleBag<Null> tuples : tuplesCopies) {
				phj.receiveTuples(WhichInput.LEFT, tuples);
			}
			endTime = System.nanoTime();
			joinProduceTimes.add((double) endTime - startTime);
			produceOutputCount = sink.getCount();
		}

		double joinStoreTime = computeAvg(joinStoreTimes);
		double joinStoresPerSecond = 1000 * 1001 / (joinStoreTime / 1.0e9);
		System.out.println("Join stores per second: " + nf.format(joinStoresPerSecond));
		System.out.println("Join stores produced tuples: " + nf.format(storeOutputCount));

		double joinProduceTime = computeAvg(joinProduceTimes) - joinStoreTime;
		double joinProducePerSecond = 1000 * 1000 / (joinProduceTime / 1.0e9);
		System.out.println("Join produce per second: " + nf.format(joinProducePerSecond));
		System.out.println("Join produce produced tuples: " + produceOutputCount);

		List<Double> fullSerializationTimes = new ArrayList<Double>(numSerializationTrials);
		List<Double> fullDeserializationTimes = new ArrayList<Double>(numSerializationTrials);
		List<Double> keySerializationTimes = new ArrayList<Double>(numSerializationTrials);
		List<Double> keyDeserializationTimes = new ArrayList<Double>(numSerializationTrials);

		InetSocketAddress[] nodes = new InetSocketAddress[] {
				new InetSocketAddress(InetAddress.getByName("www.cis.upenn.edu"),500),
				new InetSocketAddress(InetAddress.getByName("www.seas.upenn.edu"),500)
		};
		QpTuple<Null> t = new QpTuple<Null>(r, rFields);
		t = t.changeContributingNodes(nodes);
		for (int i = 0; i < numSerializationTrials; ++i) {
			ScratchOutputBuffer out = new ScratchOutputBuffer();
			System.gc();
			long startTime = System.nanoTime();
			for (int j = 0; j < serializationTrialSize; ++j) {
				t.getBytes(out);
				out.reset();
			}
			long endTime = System.nanoTime();
			fullSerializationTimes.add((double) endTime - startTime);

			t.getBytes(out);
			byte[] tupleBytes = out.getData();

			System.gc();
			ScratchInputBuffer in = new ScratchInputBuffer();
			startTime = System.nanoTime();
			for (int j = 0; j < serializationTrialSize; ++j) {
				in.reset(tupleBytes);
				QpTuple.fromBytes(r, in);
			}
			endTime = System.nanoTime();
			fullDeserializationTimes.add((double) endTime - startTime);

			System.gc();
			startTime = System.nanoTime();
			QpTupleKey key = t.getKeyTuple(0);
			for (int j = 0; j < serializationTrialSize; ++j) {
				key.getBytes();
			}
			endTime = System.nanoTime();
			keySerializationTimes.add((double) endTime - startTime);

			tupleBytes = key.getBytes();

			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < serializationTrialSize; ++j) {
				in.reset(tupleBytes);
				QpTupleKey.deserialize(r, in);
			}
			endTime = System.nanoTime();
			keyDeserializationTimes.add((double) endTime - startTime);

		}

		double fullSerializationsPerSecond = serializationTrialSize / (computeAvg(fullSerializationTimes) / 1.0e9);
		double fullDeserializationsPerSecond = serializationTrialSize / (computeAvg(fullDeserializationTimes) / 1.0e9);
		double keySerializationsPerSecond = serializationTrialSize / (computeAvg(keySerializationTimes) / 1.0e9);
		double keyDeserializationsPerSecond = serializationTrialSize / (computeAvg(keyDeserializationTimes) / 1.0e9);

		System.out.println("Full serializations per second: " + nf.format(fullSerializationsPerSecond));
		System.out.println("Full deserializations per second: " + nf.format(fullDeserializationsPerSecond));
		System.out.println("Key serializations per second: " + nf.format(keySerializationsPerSecond));
		System.out.println("Key deserializations per second: " + nf.format(keyDeserializationsPerSecond));
		
		return new SystemCalibration(joinStoresPerSecond, joinProducePerSecond, aggInputRowsPerSecond, 
				aggFuncsPerSecond, predsPerSecond, predInputTuplesPerSecond, predOutputTuplesPerSecond,
				funcsPerSecond, funcTuplesPerSecond, tupleIdsPerSecond, msgsPerSecond,
				versionedScanFailedKeyFilterPerSecond, versionedScanPassedKeyFilterPerSecond,
				probesPerSecond, indexLookupsPerSecond, fullSerializationsPerSecond, fullDeserializationsPerSecond,
				keySerializationsPerSecond, keyDeserializationsPerSecond, indexedScanReadPerSecond);
	}

	public static double msgSendTime() throws InterruptedException, IOException {
		QpMessageSerialization qms = new QpMessageSerialization();
		TestClient tc1 = new TestClient(), tc2 = new TestClient();

		InetAddress localHost = InetAddress.getLocalHost();;
		InetSocketAddress address1 = new InetSocketAddress(localHost, 5000);
		InetSocketAddress address2 = new InetSocketAddress(localHost, 6000);

		File scratchDir = new File("scratch");
		if (scratchDir.exists()) {
			for (File f : scratchDir.listFiles()) {
				f.delete();
			}
		} else {
			scratchDir.mkdir();
		}

		ThreadGroup tg = new ThreadGroup("Calibration Socket Managers");
		ScratchFileGenerator sfg = new SimpleScratchFileGenerator(scratchDir, "temp");


		SocketManager sm1 = new SocketManager(qms, sfg, tc1, tg, address1, address1);
		SocketManager sm2 = new SocketManager(qms, sfg, tc2, tg, address2, address2);
		sm1.sendMessage(new DummyMessage(address2));
		tc2.reset(1);
		tc2.receiveMessages(sm2);

		List<Double> msgSendTimes = new ArrayList<Double>(numMsgTrials);

		for (int j = 0; j < numMsgTrials; ++j) {
			tc2.reset(numMsgs);
			System.gc();
			long startTime = System.nanoTime();
			for (int i = 0; i < numMsgs; ++i) {
				sm1.sendMessage(new DummyMessage(address2));
			}
			tc2.receiveMessages(sm2);
			long endTime = System.nanoTime();
			

			double time = (endTime - startTime) / 1.0e9;
			System.out.println("Trial #" +j + " took " + nf.format(time) + " sec to send " + numMsgs + " messages");
			msgSendTimes.add(time / numMsgs);
		}

		sm1.close();
		sm2.close();
		return computeAvg(msgSendTimes);
	}


	private static class Sink extends Operator<Null> {
		private int count = 0;
		protected Sink(InetSocketAddress nodeId, QpSchema.Source schemas) {
			super(nodeId, 0, null, schemas, new RecordTuplesNothing(), false);
		}

		@Override
		protected void receiveTuples(WhichInput dest, QpTupleBag<Null> tuples) {
			count += tuples.size();
		}
		void reset() {
			count = 0;
		}
		int getCount() {
			return count;
		}

		@Override
		protected void inputHasFinished(WhichInput whichChild, int phaseNo) {
		}
	}

	public static class RecordTuplesNothing implements RecordTuples {
		@Override
		public void reportException(Exception e) {
		}

		@Override
		public void keysNotFound(int operatorId, Collection<QpTupleKey> keys, int phaseNo) {
		}

		@Override
		public void close() {
		}

		@Override
		public void keysScanned(int operatorId, IdRange range) {
		}

		@Override
		public void nodesHaveFailed(Collection<InetSocketAddress> node) {
		}

		@Override
		public void operatorHasFinished(int operatorId, int phaseNo) {
		}

		@Override
		public void activityHasOccurred() {
		}
	}

	private static class FalseFilter implements Predicate {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean eval(AbstractTuple<?> t) throws CompareMismatch {
			return false;
		}

		@Override
		public String getSqlCondition(AbstractRelation ts, String prefix,
				String suffix) {
			return null;
		}

		@Override
		public void serialize(Document d, Element el, AbstractRelation ts) {
		}

		@Override
		public Set<Integer> getColumns() {
			return Collections.emptySet();
		}
	}

	private static class TrueFilter implements Predicate {
		private static final long serialVersionUID = 1L;

		public boolean eval(AbstractTuple<?> t) throws FilterException {
			return true;
		}

		public Set<Integer> getColumns() {
			return Collections.emptySet();		}

		@Override
		public String getSqlCondition(AbstractRelation ts, String prefix,
				String suffix) {
			return null;
		}

		@Override
		public void serialize(Document d, Element el, AbstractRelation ts) {
		}
	}

	public static class TestClient implements SocketManagerClient {
		public final LongList received = new LongList();
		public final LongList sent = new LongList();
		public final LongList failed = new LongList();
		public final Set<InetSocketAddress> deadPeers = new HashSet<InetSocketAddress>();

		public TestClient() {
		}

		private int expectedMessages = 0;
		private int currentlyReceivingMessages = 0;

		public synchronized void setExpectedMessages(int expected) {
			expectedMessages = expected;
		}

		public synchronized void reset(int expectedMessages) {
			received.clear();
			sent.clear();
			failed.clear();
			deadPeers.clear();
			this.expectedMessages = expectedMessages;
		}
		

		public void receiveMessages(SocketManager sm) throws InterruptedException {
			synchronized (this) {
				if (expectedMessages <= 0) {
					return;
				}
			}
			for ( ; ; ) {
				boolean last = false;
				synchronized (this) {
					--expectedMessages;
					if (expectedMessages == 0) {
						last = true;
					} else if (expectedMessages < 0) {
						return;
					}
					++currentlyReceivingMessages;
				}

				sm.readMessage();
				synchronized (this) {
					--currentlyReceivingMessages;
					if (last) {
						notifyAll();
						return;
					}
				}
			}
		}

		public synchronized void sentMessagesReceived(InetSocketAddress sentTo, LongList msgIds) {
			received.addFrom(msgIds);
			notifyAll();
		}

		public synchronized int remaining() {
			return expectedMessages + currentlyReceivingMessages;
		}

		public synchronized void messageSendingFailed(InetSocketAddress dest, LongList msgIds) {
			failed.addFrom(msgIds);
		}

		public synchronized void messagesSent(LongList msgIds) {
			sent.addFrom(msgIds);
			notifyAll();
		}

		public synchronized void peerIsDead(InetSocketAddress peer) {
			deadPeers.add(peer);
		}

		public synchronized void peerIsNotDead(InetSocketAddress peer) {
		}

		public void startedThrottling(InetSocketAddress node) {
			System.out.println("Started throttling " + node);
		}


		public void stoppedThrottling(InetSocketAddress node) {
			System.out.println("Stopped throttling " + node);
		}

	}

	public static double computeAvg(List<Double> vals) {
		List<Double> toConsider;
		final int size = vals.size();
		if (size < 5 ) {
			toConsider = vals;
		} else {
			Collections.sort(vals);
			int start = size / 5;
			int end = ((4 * size) / 5) - 1;
			toConsider = vals.subList(start, end);
		}
		double sum = 0.0;
		for (Double d : toConsider) {
			sum += d;
		}
		return sum / (toConsider.size());
	}

	static void copyDirectory(File from, File to) throws IOException {
		if (! to.exists()) {
			to.mkdir();
		}

		for (String file : from.list()) {
			FileChannel orig = new FileInputStream(new File(from, file)).getChannel();
			FileChannel copy = new FileOutputStream(new File(to, file)).getChannel();
			long size = orig.size();
			long written = 0;
			while (written < size) {
				written += orig.transferTo(written, size - written, copy);
			}
			orig.close();
			copy.close();
		}
	}

	static void cleanDirectory(File dir) {
		if (dir.exists()) {
			File[] files = dir.listFiles();
			for (File file : files) {
				file.delete();
			}
		} else {
			dir.mkdir();
		}
	}

	static class RetrieveInfo {
		double intsPerSecond, longsPerSecond, datesPerSecond, doublesPerSecond;
		double stringOverheadPerSecond, stringCharactersPerSecond;

		double hashTuplesPerSecond, hashIntsPerSecond, hashLongsPerSecond, hashDatesPerSecond, hashDoublesPerSecond;
		double hashStringOverheadPerSecond, hashStringCharactersPerSecond;
	}

	static RetrieveInfo getRetrieveInfo() throws Exception {
		RetrieveInfo retval = new RetrieveInfo();

		QpSchema schema = new QpSchema("I", 10);
		schema.addCol("a", IntType.INT);
		schema.addCol("b", LongType.LONG);
		schema.addCol("c", DateType.DATE);
		schema.addCol("d", DoubleType.DOUBLE);
		schema.addCol("s5", new StringType(false, false, false, 5));
		schema.addCol("s35", new StringType(false, false, false, 35));
		schema.markFinished();
		QpTuple<?> data[] = new QpTuple<?>[retrieveTrialSize];
		for (int i = 0; i < retrieveTrialSize; ++i) {
			data[i] = new QpTuple<Null>(schema, new Object[]{i, (long) i, new Date(2000, 1, (i % 30) + 1),
					(double) i, "ABCDE", "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789"});
		}

		System.out.print("Computing retrieval and hash times...");
		System.out.flush();
		List<Double> intTimes = new ArrayList<Double>(), longTimes = new ArrayList<Double>();
		List<Double> dateTimes = new ArrayList<Double>(), doubleTimes = new ArrayList<Double>();
		List<Double> s5Times = new ArrayList<Double>(), s35Times = new ArrayList<Double>();
		List<Double> tupleHashTimes = new ArrayList<Double>();
		List<Double> intHashTimes = new ArrayList<Double>(), longHashTimes = new ArrayList<Double>();
		List<Double> dateHashTimes = new ArrayList<Double>(), doubleHashTimes = new ArrayList<Double>();
		List<Double> s5HashTimes = new ArrayList<Double>(), s35HashTimes = new ArrayList<Double>();
		for (int i = 0; i < numRetrieveTrials; ++i) {
			System.gc();
			long startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].get(0);
			}
			long endTime = System.nanoTime();
			intTimes.add((endTime - startTime) / 1.0e9);

			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].get(1);
			}
			endTime = System.nanoTime();
			longTimes.add((endTime - startTime) / 1.0e9);

			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].get(2);
			}
			endTime = System.nanoTime();
			dateTimes.add((endTime - startTime) / 1.0e9);

			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].get(3);
			}
			endTime = System.nanoTime();
			doubleTimes.add((endTime - startTime) / 1.0e9);

			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].get(4);
			}
			endTime = System.nanoTime();
			s5Times.add((endTime - startTime) / 1.0e9);

			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].get(5);
			}
			endTime = System.nanoTime();
			s35Times.add((endTime - startTime) / 1.0e9);

			int[] hashCols = {};
			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].hashCode(hashCols);
			}
			endTime = System.nanoTime();
			tupleHashTimes.add((endTime - startTime) / 1.0e9);

			hashCols = new int[] {0};
			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].hashCode(hashCols);
			}
			endTime = System.nanoTime();
			intHashTimes.add((endTime - startTime) / 1.0e9);

			hashCols = new int[] {1};
			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].hashCode(hashCols);
			}
			endTime = System.nanoTime();
			longHashTimes.add((endTime - startTime) / 1.0e9);

			hashCols = new int[] {2};
			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].hashCode(hashCols);
			}
			endTime = System.nanoTime();
			dateHashTimes.add((endTime - startTime) / 1.0e9);

			hashCols = new int[] {3};
			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].hashCode(hashCols);
			}
			endTime = System.nanoTime();
			doubleHashTimes.add((endTime - startTime) / 1.0e9);

			hashCols = new int[] {4};
			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].hashCode(hashCols);
			}
			endTime = System.nanoTime();
			s5HashTimes.add((endTime - startTime) / 1.0e9);

			hashCols = new int[] {5};
			System.gc();
			startTime = System.nanoTime();
			for (int j = 0; j < retrieveTrialSize; ++j) {
				data[j].hashCode(hashCols);
			}
			endTime = System.nanoTime();
			s35HashTimes.add((endTime - startTime) / 1.0e9);

		}
		retval.intsPerSecond = retrieveTrialSize / computeAvg(intTimes);
		retval.longsPerSecond = retrieveTrialSize / computeAvg(longTimes);
		retval.datesPerSecond = retrieveTrialSize / computeAvg(dateTimes);
		retval.doublesPerSecond = retrieveTrialSize / computeAvg(doubleTimes);

		double s5Time = computeAvg(s5Times) / retrieveTrialSize;
		double s35Time = computeAvg(s35Times) / retrieveTrialSize;

		double timePerChar = (s35Time - s5Time) / 30;
		double stringBaseTime = s5Time - (5 * timePerChar);

		retval.stringOverheadPerSecond = 1 / stringBaseTime;
		retval.stringCharactersPerSecond = 1 / timePerChar;

		double tupleHashTime = computeAvg(tupleHashTimes) / retrieveTrialSize;
		double intHashTime = (computeAvg(intHashTimes) / retrieveTrialSize) - tupleHashTime;
		double longHashTime = (computeAvg(longHashTimes) / retrieveTrialSize) - tupleHashTime;
		double dateHashTime = (computeAvg(dateHashTimes) / retrieveTrialSize) - tupleHashTime;
		double doubleHashTime = (computeAvg(doubleHashTimes) / retrieveTrialSize) - tupleHashTime;
		retval.hashTuplesPerSecond = 1 / tupleHashTime;
		retval.hashIntsPerSecond = 1 / intHashTime;
		retval.hashLongsPerSecond = 1 / longHashTime;
		retval.hashDatesPerSecond = 1 / dateHashTime;
		retval.hashDoublesPerSecond = 1 / doubleHashTime;

		double s5HashTime = (computeAvg(s5HashTimes) / retrieveTrialSize) - tupleHashTime;
		double s35HashTime = (computeAvg(s35HashTimes) / retrieveTrialSize) - tupleHashTime;
		double hashStringCharTime = (s35HashTime - s5HashTime) / 30;
		double hashStringOverheadTime = s5HashTime - (5 * hashStringCharTime);
		retval.hashStringOverheadPerSecond = 1 / hashStringOverheadTime;
		retval.hashStringCharactersPerSecond = 1 / hashStringCharTime;

		System.out.println("done");
		System.out.println("Integers per second: " + nf.format(retval.intsPerSecond));
		System.out.println("Longs per second: " + nf.format(retval.longsPerSecond));
		System.out.println("Dates per second: " + nf.format(retval.datesPerSecond));
		System.out.println("Doubles per second: " + nf.format(retval.doublesPerSecond));
		System.out.println("Strings per second: " + nf.format(retval.stringOverheadPerSecond));
		System.out.println("String characters per second: " + nf.format(retval.stringCharactersPerSecond));
		System.out.println("Hash tuples per second: " + nf.format(retval.hashTuplesPerSecond));
		System.out.println("Hash integers per second: " + nf.format(retval.hashIntsPerSecond));
		System.out.println("Hash longs per second: " + nf.format(retval.hashLongsPerSecond));
		System.out.println("Hash dates per second: " + nf.format(retval.hashDatesPerSecond));
		System.out.println("Hash doubles per second: " + nf.format(retval.hashDoublesPerSecond));
		System.out.println("Hash strings per second: " + nf.format(retval.hashStringOverheadPerSecond));
		System.out.println("Hash string characters per second: " + nf.format(retval.hashStringCharactersPerSecond));
		return retval;
	}

	private static class KeyAndId implements Comparable<KeyAndId> {
		final QpTupleKey key;
		final Id id;

		KeyAndId(QpTupleKey key) {
			this.key = key;
			this.id = key.getQPid();
		}

		public int compareTo(KeyAndId arg0) {
			return id.compareTo(arg0.id);
		}

		@Override
		public boolean equals(Object o) {
			KeyAndId other = (KeyAndId) o;
			return key.equals(other.key);
		}
	}
}
