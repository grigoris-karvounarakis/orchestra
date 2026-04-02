package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.BadColumnName;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.FieldSource;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.JoinFieldSource;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.util.ByteArraySet;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class PipelinedHashJoin<M> extends Operator<M> {
	private static final int primes[] = {29, 53, 79, 103, 151, 211, 251, 307, 401, 503, 601, 701, 809, 907, 1009,
		1259, 1511, 1753, 2003, 2251, 2503, 2753, 3001, 3511, 4001, 4507, 5003, 5507, 6007, 6251, 7001, 7507, 8009,
		9001, 10007, 11003, 12007, 13001, 14009, 15013, 16001, 17011, 18013, 19001, 20011, 25013, 30011, 35023,
		40009, 45007, 50021, 55001, 60013, 65003, 70001, 75011, 80021, 85009, 90001, 95003, 100003, 110017,
		120011, 130021, 140009, 150001, 160001, 170003, 180001, 190027, 200003, 225023, 250007, 275003, 300007,
		325001, 350003, 375017, 400009, 425003, 450001, 475037, 500009, 550007, 600011, 700001, 800011, 900001,
		1000003	};

	public static int getNextLargerPrime(int num) {
		if (num == defaultNumBuckets) {
			return defaultNumBuckets;
		}
		if (num >= primes[primes.length -1]) {
			return primes[primes.length -1];
		} else if (num <= primes[0]) {
			return primes[0];
		}
		int index = Arrays.binarySearch(primes, num);
		if (index >= 0) {
			return primes[index];
		} else {
			return primes[-1*(index+1)];
		}
	}

	private final Logger logger = Logger.getLogger(this.getClass());
	private final int[] leftJoinIndices, rightJoinIndices;

	// When processing an unprojected input against projected stored data
	private final AbstractRelation.JoinComparator leftJoinComparator, rightJoinComparator;
	private final AbstractRelation.RelationMapping leftJoinMapping, rightJoinMapping;

	// When processing projected input against projected stored data (i.e. during recovery)
	private final AbstractRelation.JoinComparator recoveryLeftJoinComparator, recoveryRightJoinComparator;
	private final AbstractRelation.RelationMapping recoveryLeftJoinMapping, recoveryRightJoinMapping;


	private int lastLeftPhaseFinished = -1, lastRightPhaseFinished = -1;

	private final boolean preserveLeftInput, preserveRightInput;

	private final QpTupleBag<M>[] leftTuples, rightTuples;
	private boolean[] begunPhases = new boolean [] { true };
	private QpTupleBag<M> unbegunLeftTuples, unbegunRightTuples;

	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock readLock = lock.readLock();
	private final Lock writeLock = lock.writeLock();

	private final QpSchema outputSchema;

	private final AbstractRelation.RelationMapping leftNeededCols, rightNeededCols;
	private final QpSchema leftProjectedSchema, rightProjectedSchema;
	private final int[] outputSchemaIdCols;

	// If all of the ID cols are found in the left or right schema, they are here
	private final int[] leftSchemaIdCols, rightSchemaIdCols;

	private static int defaultNumBuckets = 199;

	public PipelinedHashJoin(QpSchema leftSchema, QpSchema rightSchema,
			List<Integer> leftJoinIndices, List<Integer> rightJoinIndices,
			List<Integer> leftOutputPos, List<Integer> rightOutputPos,
			QpSchema outputSchema, Operator<M> dest,
			WhichInput outputDest, InetSocketAddress nodeId, int operatorId,
			RecordTuples rt, MetadataFactory<M> mdf, QpSchema.Source schemas, Map<Integer,int[]> recoveryOperators) throws ValueMismatchException {
		this(leftSchema, rightSchema, leftJoinIndices, rightJoinIndices, leftOutputPos, rightOutputPos,
				outputSchema, defaultNumBuckets, dest, outputDest, nodeId, operatorId, rt, mdf, schemas, recoveryOperators);

	}

	@SuppressWarnings("unchecked")
	public PipelinedHashJoin(QpSchema leftSchema, QpSchema rightSchema,
			List<Integer> leftJoinIndices, List<Integer> rightJoinIndices,
			List<Integer> leftOutputPos, List<Integer> rightOutputPos,
			QpSchema outputSchema, int numBuckets, Operator<M> dest,
			WhichInput outputDest, InetSocketAddress nodeId, int operatorId,
			RecordTuples rt, MetadataFactory<M> mdf, QpSchema.Source schemas, Map<Integer,int[]> recoveryOperators) throws ValueMismatchException {
		super(dest, outputDest, nodeId, operatorId, mdf, schemas, rt, recoveryOperators != null);

		if (this.enableRecovery) {
			failedNodes = new ByteArraySet(100);
			preserveLeftInput = true;
			preserveRightInput = true;
			int[] outputSchemaIdCols = recoveryOperators.get(operatorId);
			if (outputSchemaIdCols == null) {
				this.outputSchemaIdCols = null;
				this.leftSchemaIdCols = null;
				this.rightSchemaIdCols = null;
			} else {
				this.outputSchemaIdCols = new int[outputSchemaIdCols.length];
				for (int i = 0; i < outputSchemaIdCols.length; ++i) {
					this.outputSchemaIdCols[i] = outputSchemaIdCols[i];
				}
				int[] leftSchemaIdCols = new int[outputSchemaIdCols.length];
				COL: for (int i = 0; i < outputSchemaIdCols.length; ++i) {
					int pos = 0;
					for (Integer col : leftOutputPos) {
						if (col != null && col == outputSchemaIdCols[i]) {
							leftSchemaIdCols[i] = pos;
							continue COL;
						}
						++pos;
					}
					leftSchemaIdCols = null;
					break;
				}
				int[] rightSchemaIdCols = new int[outputSchemaIdCols.length];
				COL: for (int i = 0; i < outputSchemaIdCols.length; ++i) {
					int pos = 0;
					for (Integer col : rightOutputPos) {
						if (col != null && col == outputSchemaIdCols[i]) {
							rightSchemaIdCols[i] = pos;
							continue COL;
						}
						++pos;
					}
					rightSchemaIdCols = null;
					break;
				}
				this.leftSchemaIdCols = leftSchemaIdCols;
				this.rightSchemaIdCols = rightSchemaIdCols;
			}
		} else {
			preserveLeftInput = false;
			preserveRightInput = false;
			outputSchemaIdCols = null;
			leftSchemaIdCols = null;
			rightSchemaIdCols = null;
			failedNodes = null;
		}

		numBuckets = getNextLargerPrime(numBuckets);

		final int numJoinVars = leftJoinIndices.size();
		if (rightJoinIndices.size() != numJoinVars) {
			throw new IllegalArgumentException("Must have same number of left and right join variables");
		}

		this.outputSchema = outputSchema;
		Set<Integer> leftNeededCols = new HashSet<Integer>();
		Set<Integer> rightNeededCols = new HashSet<Integer>();

		int pos = 0;
		for (Integer col : leftOutputPos) {
			if (col != null && col >= 0) {
				leftNeededCols.add(pos);
			}
			pos++;
		}
		pos = 0;
		for (Integer col : rightOutputPos) {
			if (col != null && col >= 0) {
				rightNeededCols.add(pos);
			}
			pos++;
		}


		for (int col : leftJoinIndices) {
			leftNeededCols.add(col);
		}

		for (int col : rightJoinIndices) {
			rightNeededCols.add(col);
		}


		int[] leftColMapping = new int[leftSchema.getNumCols()];
		int[] rightColMapping = new int[rightSchema.getNumCols()];

		try {
			if (leftNeededCols.size() == leftOutputPos.size()) {
				this.leftNeededCols = null;
				this.leftProjectedSchema = leftSchema;
				for (int i = 0; i < leftColMapping.length; ++i) {
					leftColMapping[i] = i;
				}
			} else {
				final int numCols = leftSchema.getNumCols();
				leftProjectedSchema = new QpSchema("LEFT",-100000);
				FieldSource[] fs = new FieldSource[leftNeededCols.size()];
				pos = 0;
				for (int i = 0; i < numCols; ++i) {
					if (leftNeededCols.contains(i)) {
						fs[pos] = new FieldSource(i, true);
						leftColMapping[i] = pos++;
						leftProjectedSchema.addCol(leftSchema.getColName(i), leftSchema.getColType(i));
					} else {
						leftColMapping[i] = -1;
					}
				}
				leftProjectedSchema.markFinished();
				this.leftNeededCols = new AbstractRelation.RelationMapping(leftSchema, leftProjectedSchema, null, fs);
				if (this.leftSchemaIdCols != null) {
					for (int i = 0; i < leftSchemaIdCols.length; ++i) {
						this.leftSchemaIdCols[i] = leftColMapping[this.leftSchemaIdCols[i]];
					}
				}
			}

			if (rightNeededCols.size() == rightOutputPos.size()) {
				this.rightNeededCols = null;
				this.rightProjectedSchema = rightSchema;
				for (int i = 0; i < rightColMapping.length; ++i) {
					rightColMapping[i] = i;
				}
			} else {
				final int numCols = rightSchema.getNumCols();
				rightProjectedSchema = new QpSchema("RIGHT",-100000);
				FieldSource[] fs = new FieldSource[rightNeededCols.size()];
				pos = 0;
				for (int i = 0; i < numCols; ++i) {
					if (rightNeededCols.contains(i)) {
						fs[pos] = new FieldSource(i, true);
						rightColMapping[i] = pos++;
						rightProjectedSchema.addCol(rightSchema.getColName(i), rightSchema.getColType(i));
					} else {
						rightColMapping[i] = -1;
					}
				}
				rightProjectedSchema.markFinished();
				this.rightNeededCols = new AbstractRelation.RelationMapping(rightSchema, rightProjectedSchema, null, fs);
				if (this.rightSchemaIdCols != null) {
					for (int i = 0; i < rightSchemaIdCols.length; ++i) {
						this.rightSchemaIdCols[i] = rightColMapping[this.rightSchemaIdCols[i]];
					}
				}
			}
		} catch (BadColumnName bcn) {
			throw new IllegalStateException("Should not get a BadColumnName", bcn);
		}



		int[] leftJoinIndicesArray = new int[numJoinVars];
		int[] rightJoinIndicesArray = new int[numJoinVars];
		int[] projectedLeftJoinIndicesArray = new int[numJoinVars];
		int[] projectedRightJoinIndicesArray = new int[numJoinVars];
		for (int i = 0; i < numJoinVars; ++i) {
			leftJoinIndicesArray[i] = leftJoinIndices.get(i);
			rightJoinIndicesArray[i] = rightJoinIndices.get(i);
			projectedLeftJoinIndicesArray[i] = leftColMapping[leftJoinIndices.get(i)];
			projectedRightJoinIndicesArray[i] = rightColMapping[rightJoinIndices.get(i)];
		}
		this.recoveryLeftJoinComparator = new AbstractRelation.JoinComparator(leftProjectedSchema, rightProjectedSchema, projectedLeftJoinIndicesArray, projectedRightJoinIndicesArray);
		this.recoveryRightJoinComparator = recoveryLeftJoinComparator.swapInputs();
		this.leftJoinComparator = new AbstractRelation.JoinComparator(leftSchema, rightProjectedSchema, leftJoinIndicesArray, projectedRightJoinIndicesArray);
		this.rightJoinComparator = new AbstractRelation.JoinComparator(rightSchema, leftProjectedSchema, rightJoinIndicesArray, projectedLeftJoinIndicesArray);
		this.leftJoinIndices = leftJoinIndicesArray;
		this.rightJoinIndices = rightJoinIndicesArray;

		final int numOutputCols = outputSchema.getNumCols();
		Set<Integer> remainingOutputPos = new HashSet<Integer>(numOutputCols);
		for (int i = 0; i < numOutputCols; ++i) {
			remainingOutputPos.add(i);
		}

		JoinFieldSource[] leftJfss = new JoinFieldSource[numOutputCols];
		JoinFieldSource[] rightJfss = new JoinFieldSource[numOutputCols];
		JoinFieldSource[] recoveryJfss = new JoinFieldSource[numOutputCols];

		// leftOutPos[pos] = i => column i in output comes from pos in left
		// input
		pos = 0;
		for (Integer i : leftOutputPos) {
			if (i == null || i == -1) {
			} else if (i >= 0) {
				if (remainingOutputPos.remove(i)) {
					leftJfss[i] = new JoinFieldSource(pos, true);
					rightJfss[i] = new JoinFieldSource(leftColMapping[pos], false);
					recoveryJfss[i] = new JoinFieldSource(leftColMapping[pos], true);
				} else {
					throw new IllegalArgumentException("Output position " + i
							+ " is specified more than once");
				}
			} else {
				throw new IllegalArgumentException("Output position must be >= -1 or null");
			}
			++pos;
		}

		pos = 0;
		for (Integer i : rightOutputPos) {
			if (i == null || i == -1) {
			} else if (i >= 0) {
				if (remainingOutputPos.remove(i)) {
					leftJfss[i] = new JoinFieldSource(rightColMapping[pos], false);
					rightJfss[i] = new JoinFieldSource(pos, true);
					recoveryJfss[i] = new JoinFieldSource(rightColMapping[pos], false);
				} else {
					throw new IllegalArgumentException("Output position " + i
							+ " is specified more than once");
				}
			} else {
				throw new IllegalArgumentException("Output position must be >= -1 or null");
			}
			++pos;
		}

		if (!remainingOutputPos.isEmpty()) {
			throw new IllegalArgumentException("Output positions "
					+ remainingOutputPos + " are not specified in operator "
					+ operatorId);
		}

		this.leftJoinMapping = new AbstractRelation.RelationMapping(leftSchema, rightProjectedSchema, outputSchema, leftJfss);
		this.rightJoinMapping = new AbstractRelation.RelationMapping(rightSchema, leftProjectedSchema, outputSchema, rightJfss);
		this.recoveryLeftJoinMapping = new AbstractRelation.RelationMapping(leftProjectedSchema, rightProjectedSchema, outputSchema, recoveryJfss);
		this.recoveryRightJoinMapping = recoveryLeftJoinMapping.switchInputs();
		leftTuples = (QpTupleBag<M>[]) new QpTupleBag<?>[numBuckets];
		rightTuples = (QpTupleBag<M>[]) new QpTupleBag<?>[numBuckets];

		for (int i = 0; i < numBuckets; ++i) {
			leftTuples[i] = new QpTupleBag<M>(leftProjectedSchema, schemas, mdf);
			rightTuples[i] = new QpTupleBag<M>(rightProjectedSchema, schemas, mdf);
		}


		this.unbegunLeftTuples = new QpTupleBag<M>(leftSchema, schemas, mdf);
		this.unbegunRightTuples = new QpTupleBag<M>(rightSchema, schemas, mdf);
	}

	protected void receiveTuples(WhichInput destInput, QpTupleBag<M> inputTuples) {
		QpTupleBag<M> output = new QpTupleBag<M>(outputSchema, this.schemas, this.mdf);
		readLock.lock();
		final boolean left = destInput == WhichInput.LEFT;
		final int numBuckets = leftTuples.length;
		final QpTupleBag<M>[] probe, store;
		final QpTupleBag<M> unbegun;
		final AbstractRelation.RelationMapping retainMapping;
		final AbstractRelation.RelationMapping joinMapping;
		final AbstractRelation.JoinComparator joinComparator;
		final int[] joinIndices;
		final boolean preserveInput;
		final int maxSkipStorePhase;
		if (left) {
			joinIndices = this.leftJoinIndices;
			joinComparator = this.leftJoinComparator;
			probe = rightTuples;
			store = leftTuples;
			retainMapping = this.leftNeededCols;
			joinMapping = this.leftJoinMapping;
			preserveInput = preserveLeftInput;
			maxSkipStorePhase = this.lastRightPhaseFinished;
			unbegun = this.unbegunLeftTuples;
		} else {
			joinIndices = this.rightJoinIndices;
			joinComparator = this.rightJoinComparator;
			probe = leftTuples;
			store = rightTuples;
			retainMapping = this.rightNeededCols;
			joinMapping = this.rightJoinMapping;
			preserveInput = preserveRightInput;
			maxSkipStorePhase = this.lastLeftPhaseFinished;
			unbegun = this.unbegunRightTuples;
		}

		int bufferedCount = 0;
		try {
			QpTupleBag.SerializedTuplePosition stp = inputTuples.getSerializedTuplePositions();
			while (! stp.done) {
				int bucket = stp.getHashCode(joinIndices) % numBuckets;
				if (bucket < 0) {
					bucket += numBuckets;
				}
				final int tuplePhase = QpTuple.getPhase(stp.data, stp.pos);
				if ((left && tuplePhase <= this.lastLeftPhaseFinished) || ((! left) && tuplePhase <= this.lastRightPhaseFinished)) {
					this.reportException(new RuntimeException("PipelinedHashJoin #" + this.operatorId + " at " + this.nodeId + " received tuple from finished phase " + tuplePhase));
					return;
				}
				if (this.enableRecovery) {
					if (QpTuple.contributes(stp.data, stp.pos, failedNodes)) {
						stp.advance();
						continue;
					}
					if (! (begunPhases.length > tuplePhase && begunPhases[tuplePhase])) {
						synchronized (unbegun) {
							unbegun.add(stp.data, stp.pos, stp.length);
						}
						stp.advance();
						++bufferedCount;
						continue;
					}
				}
				boolean needToStore = preserveInput || (tuplePhase > maxSkipStorePhase);
				synchronized (leftTuples[bucket]) {
					if (needToStore) {
						if (mdf != null) {
							store[bucket].addCombiningWithExistingMetadata(retainMapping, stp.data, stp.pos);
						} else if (retainMapping != null) {
							store[bucket].addAndApplyMapping(retainMapping, stp.data, stp.pos);
						} else {
							store[bucket].add(stp.data, stp.pos, stp.length);
						}
					}
					probe[bucket].joinWith(this.lastPhaseBegun, inputTuples.schema, stp.data, stp.pos, output, joinMapping, joinComparator);
				}
				stp.advance();
			}
			if (!output.isEmpty()) {
				sendTuples(output);
			}
			if (bufferedCount > 0) {
				logger.info("PipelinedHashJoin " + this.operatorId + " at " + this.nodeId + " buffered " + bufferedCount + " early tuples");
			}
		} finally {
			readLock.unlock();
		}
		output.clear();
	}

	protected void close() {
		for (int i = 0; i < leftTuples.length; ++i) {
			leftTuples[i] = null;
		}
		for (int i = 0; i < rightTuples.length; ++i) {
			rightTuples[i] = null;
		}
	}

	protected void inputHasFinished(WhichInput whichChild, int phaseNo) {
		writeLock.lock();
		int newFinishedPhaseNo;
		boolean sendFinished = false;
		try {
			if (whichChild == WhichInput.LEFT) {
				if (lastLeftPhaseFinished < phaseNo) {
					lastLeftPhaseFinished = phaseNo;
				}
			} else if (whichChild == WhichInput.RIGHT) {
				if (lastRightPhaseFinished < phaseNo) {
					lastRightPhaseFinished = phaseNo;
				}
			} else {
				throw new IllegalArgumentException(
				"whichChild must be LEFT or RIGHT");
			}
			newFinishedPhaseNo = getCurrentFinishablePhase();
			if (newFinishedPhaseNo > this.lastPhaseFinished) {
				sendFinished = true;
				this.lastPhaseFinished = newFinishedPhaseNo;
			}
		} finally {
			writeLock.unlock();
		}
		if (sendFinished) {
			this.finishedSending(newFinishedPhaseNo);
			logger.info("PipelinedHashJoin #" + this.operatorId + " at " + this.nodeId + " has finished for phase # " + newFinishedPhaseNo);
		}
		purgeState(false);
	}

	private int getCurrentFinishablePhase() {
		int inputFinishedPhaseNo = lastLeftPhaseFinished < lastRightPhaseFinished ? lastLeftPhaseFinished
				: lastRightPhaseFinished;
		int retval = -1;
		for (int i = 0; i <= inputFinishedPhaseNo; ++i) {
			if (begunPhases.length < i || (! begunPhases[i])) {
				break;
			} else {
				retval = i;
			}
		}
		return retval;
	}
	
	private int lastPhaseFinished = -1;

	protected void purgeState(boolean destructive) {
		if (preserveLeftInput && preserveRightInput) {
			return;
		}
		final int numBuckets = this.leftTuples.length;
		if (destructive) {
			for (int i = 0; i < numBuckets; ++i) {
				leftTuples[i] = null;
				rightTuples[i] = null;
			}
			return;
		}
		// TODO: implement non-destructive purging on a per-phase basis?
	}

	public int getOperatorId() {
		return operatorId;
	}

	private final ByteArraySet failedNodes;

	private int lastPhaseBegun = 0;

	@Override
	protected void beginNewPhase(int newPhaseNo, InetSocketAddress[] newlyFailedNodes, IdRangeSet failedRanges) throws InterruptedException {
		if (! this.enableRecovery) {
			throw new IllegalStateException("Recovery was not enabled when operator was created");
		}
		final int numBuckets = this.leftTuples.length;
		for (InetSocketAddress node : newlyFailedNodes) {
			this.failedNodes.add(OutputBuffer.getBytes(node));
		}
		final byte[] scratch;
		if (this.outputSchemaIdCols == null) {
			scratch = null;
		} else {
			scratch = new byte[IntType.bytesPerInt * outputSchemaIdCols.length];
		}
		writeLock.lock();
		int matchCount = 0;
		int skipCount = 0;
		QpTupleBag<M> toResend = new QpTupleBag<M>(this.outputSchema, this.schemas, this.mdf);
		QpTupleBag<M> begunLeft = new QpTupleBag<M>(this.unbegunLeftTuples.schema, schemas, mdf);
		QpTupleBag<M> begunRight = new QpTupleBag<M>(this.unbegunRightTuples.schema, schemas, mdf);
		QpTupleBag<M> newUnbegunLeft = new QpTupleBag<M>(this.unbegunLeftTuples.schema, schemas, mdf);
		QpTupleBag<M> newUnbegunRight = new QpTupleBag<M>(this.unbegunRightTuples.schema, schemas, mdf);
		QpTupleBag.SerializedTuplePosition stp = this.unbegunLeftTuples.getSerializedTuplePositions();
		while (! stp.done) {
			if (QpTuple.getPhase(stp.data, stp.pos) == newPhaseNo) {
				begunLeft.add(stp.data, stp.pos, stp.length);
			} else {
				newUnbegunLeft.add(stp.data, stp.pos, stp.length);
			}
			stp.advance();
		}
		stp = this.unbegunRightTuples.getSerializedTuplePositions();
		while (! stp.done) {
			if (QpTuple.getPhase(stp.data, stp.pos) == newPhaseNo) {
				begunRight.add(stp.data, stp.pos, stp.length);
			} else {
				newUnbegunRight.add(stp.data, stp.pos, stp.length);
			}
			stp.advance();
		}
		this.unbegunLeftTuples = newUnbegunLeft;
		this.unbegunRightTuples = newUnbegunRight;
		int newFinishedPhaseNo;
		boolean sendFinished = false;
		if (begunPhases.length <= newPhaseNo) {
			boolean[] newBegunPhases = new boolean[newPhaseNo+1];
			System.arraycopy(begunPhases, 0, newBegunPhases, 0, begunPhases.length);
			begunPhases = newBegunPhases;
		}
		begunPhases[newPhaseNo] = true;
		if (this.lastPhaseBegun < newPhaseNo) {
			this.lastPhaseBegun = newPhaseNo;
		}
		if (! begunLeft.isEmpty()) {
			logger.info("PipelinedHashJoin " + this.operatorId + " at " + this.nodeId + " is sending " + begunLeft.size() + " buffered left tuples");
			this.receiveTuples(WhichInput.LEFT, begunLeft);
			begunLeft = null;
		}
		if (! begunRight.isEmpty()) {
			logger.info("PipelinedHashJoin " + this.operatorId + " at " + this.nodeId + " is sending " + begunRight.size() + " buffered right tuples");
			this.receiveTuples(WhichInput.RIGHT, begunRight);
			begunRight = null;
		}
		try {
			for (int i = 0; i < numBuckets; ++i) {
				// Drop tuples from failed nodes
				if (! leftTuples[i].isEmpty()) {
					QpTupleBag<M> surviving = new QpTupleBag<M>(this.leftProjectedSchema, this.schemas, this.mdf);
					surviving.addFromDroppingFailed(leftTuples[i], this.failedNodes);
					leftTuples[i] = surviving;
				}
				if (! rightTuples[i].isEmpty()) {
					QpTupleBag<M> surviving = new QpTupleBag<M>(this.rightProjectedSchema, this.schemas, this.mdf);
					surviving.addFromDroppingFailed(rightTuples[i], this.failedNodes);
					rightTuples[i] = surviving;
				}

				if (this.leftSchemaIdCols != null) {
					stp = leftTuples[i].getSerializedTuplePositions();
					while (! stp.done) {
						Id id = Id.fromContent(this.leftProjectedSchema.getBytesForId(stp.data, stp.abstractTuplePos, stp.abstractTupleLength, false, leftSchemaIdCols, scratch));
						if (failedRanges.contains(id)) {
							++matchCount;
							rightTuples[i].joinWith(newPhaseNo, leftProjectedSchema, stp.data, stp.pos, toResend, this.recoveryLeftJoinMapping, this.recoveryLeftJoinComparator);							
						} else {
							++skipCount;
						}
						stp.advance();
					}
				} else if (this.rightSchemaIdCols != null) {
					stp = rightTuples[i].getSerializedTuplePositions();
					while (! stp.done) {
						Id id = Id.fromContent(this.rightProjectedSchema.getBytesForId(stp.data, stp.abstractTuplePos, stp.abstractTupleLength, false, rightSchemaIdCols, scratch));
						if (failedRanges.contains(id)) {
							++matchCount;
							leftTuples[i].joinWith(newPhaseNo, rightProjectedSchema, stp.data, stp.pos, toResend, this.recoveryRightJoinMapping, this.recoveryRightJoinComparator);							
						} else {
							++skipCount;
						}
						stp.advance();
					}					
				} else if (this.outputSchemaIdCols != null) {
					QpTupleBag<M> joined = new QpTupleBag<M>(this.outputSchema, this.schemas, this.mdf);
					stp = leftTuples[i].getSerializedTuplePositions();
					while (! stp.done) {
						rightTuples[i].joinWith(newPhaseNo, leftProjectedSchema, stp.data, stp.pos, joined, this.recoveryLeftJoinMapping, this.recoveryLeftJoinComparator);
					}
					Iterator<QpTuple<M>> it = joined.recyclingIterator();
					while (it.hasNext()) {
						QpTuple<M> t = it.next();
						Id id = t.getQPid(this.outputSchemaIdCols, scratch);
						if (failedRanges.contains(id)) {
							++matchCount;
							toResend.add(t);
						} else {
							++skipCount;
						}
					}
				}
			}

			for (InetSocketAddress node : newlyFailedNodes) {
				this.failedNodes.add(OutputBuffer.getBytes(node));
			}

			newFinishedPhaseNo = getCurrentFinishablePhase();
			if (newFinishedPhaseNo > this.lastPhaseFinished) {
				this.lastLeftPhaseFinished = newFinishedPhaseNo;
				sendFinished = true;
			}
		} finally {
			writeLock.unlock();
		}
		if (this.leftSchemaIdCols != null) {
			logger.info("PipelinedHashJoin " + this.operatorId + " at " + this.nodeId + " is resending using left tuples as filter for Ids, matched " + matchCount + ", skipped " + skipCount + " for ranges " + failedRanges);
		} else if (this.rightSchemaIdCols != null) {
			logger.info("PipelinedHashJoin " + this.operatorId + " at " + this.nodeId + " is resending using right tuples as filter for Ids, matched " + matchCount + ", skipped " + skipCount + " for ranges " + failedRanges);
		} else if (this.outputSchemaIdCols != null) {
			logger.info("PipelinedHashJoin " + this.operatorId + " at " + this.nodeId + " is resending using output tuples as filter for Ids, matched " + matchCount + ", skipped " + skipCount + " for ranges " + failedRanges);
		} else {
			logger.info("PipelinedHashJoin " + this.operatorId + " at " + this.nodeId + " is not configured to resend any tuples");
		}
		logger.info("PipelinedHashJoin " + this.operatorId + " at " + this.nodeId + " is resending " + toResend.size() + " tuples in phase " + newPhaseNo);
		if (! toResend.isEmpty()) {
			this.sendTuples(toResend);
		}
		if (sendFinished) {
			this.finishedSending(newFinishedPhaseNo);
			logger.info("PipelinedHashJoin #" + this.operatorId + " at " + this.nodeId + " has finished for phase # " + newFinishedPhaseNo);
		}
	}
}
