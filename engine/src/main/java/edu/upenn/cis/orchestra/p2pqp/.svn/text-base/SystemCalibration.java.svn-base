package edu.upenn.cis.orchestra.p2pqp;

import java.io.Serializable;
import java.util.Formatter;
import java.util.Locale;

public class SystemCalibration implements Serializable {
	private static final long serialVersionUID = 2L;
	// Number of tuples that can be inserted into a join hash table per second
	public final double joinStoresPerSecond;
	// Number of tuples that can be output by a join operator per second
	public final double joinProducePerSecond;
	
	// Number of aggregate input rows that can be stored and read back in a second
	public final double aggregateInputRowPerSecond;
	// Number of aggregate functions that can be processed for a single row in one second
	public final double aggregateProcessFuncPerSecond;
	
	
	/*
	 * To determine the expected time to evaluate a predicate, add together
	 * 
	 * predicateTuplesInputPerSecond * # of input tuples
	 * predicatesPerSecond * # of predicates per tuple * # of input tuples
	 * predicateTuplesOutputPerSecond * # of output tuples
	 */
	// Number of simple predicates that can be evaluated per second when doing nothing else
	public final double predicatesPerSecond;
	// Number of tuples that can be read in by the predicate evaluator per second when doing nothing else
	public final double predicateTuplesInputPerSecond;
	// Number of tuples that can be output by the predicate evaluator per second when doing nothing else
	public final double predicateTuplesOutputPerSecond;
	
	// Number of functions that can be evaluated per second
	public final double functionsPerSecond;
	// Number of tuples that the function evaluator can input & output per second
	public final double functionTuplesPerSecond;
	
	// Number of Pastry tuple ids that can be computed per second
	public final double tupleIdsPerSecond;
		
	// Number of pastry messages than can be sent per second
	public final double msgsSentPerSecond;
	
	// Number of pastry messages that can be delivered and processed per second
	public final double msgsDeliveredPerSecond;
	

	// Number of tuples passing the key filter than can be read per second
	// by the versioned scan
	public final double versionedScanPassPredPerSecond;
	// Number of tuples failing the key filter than can be read per second
	// by the versioned scan
	public final double versionedScanFailPredPerSecond;
	
	public final double tuplesProbedPerSecond;
	// Number of index record lookups per second
	public final double indexLookupsPerSecond;
	
	public final double fullTuplesSerializedPerSecond;
	public final double fullTuplesDeserializedPerSecond;
	public final double keyTuplesSerializedPerSecond;
	public final double keyTuplesDeserializedPerSecond;

	public final double indexedScanTuplesReadPerSecond;
	
	public SystemCalibration(double joinStores, double joinProduce, double aggregateInput, double aggregateProcess,
			double predicates, double predicateTuplesInput, double predicateTuplesOutput, double functions,
			double functionTuples, double tupleIds, double msgsSent,
			double versionedTuplesPassed, double versionedTuplesFailed,
			double tuplesProbed,
			double indexLookups, double fullTuplesSerialized,
			double fullTuplesDeserialized, double keyTuplesSerialized,
			double keyTuplesDeserialized, double indexedScanRead) {
		this.joinStoresPerSecond = joinStores;
		this.joinProducePerSecond = joinProduce;
		this.aggregateInputRowPerSecond = aggregateInput;
		this.aggregateProcessFuncPerSecond = aggregateProcess;
		this.predicatesPerSecond = predicates;
		this.predicateTuplesInputPerSecond = predicateTuplesInput;
		this.predicateTuplesOutputPerSecond = predicateTuplesOutput;
		this.functionsPerSecond = functions;
		this.functionTuplesPerSecond = functionTuples;
		this.tupleIdsPerSecond = tupleIds;
		this.msgsSentPerSecond = msgsSent;
		// TODO: actually figure out how many messages can be delivered per second
		this.msgsDeliveredPerSecond = msgsSent;
		this.versionedScanPassPredPerSecond = versionedTuplesPassed;
		this.versionedScanFailPredPerSecond = versionedTuplesFailed;
		this.tuplesProbedPerSecond = tuplesProbed;
		this.indexLookupsPerSecond = indexLookups;
		this.fullTuplesSerializedPerSecond = fullTuplesSerialized;
		this.fullTuplesDeserializedPerSecond = fullTuplesDeserialized;
		this.keyTuplesSerializedPerSecond = keyTuplesSerialized;
		this.keyTuplesDeserializedPerSecond = keyTuplesDeserialized;
		this.indexedScanTuplesReadPerSecond = indexedScanRead;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		Formatter f = new Formatter(sb, Locale.US);
		f.format("SystemCalibration(%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)",
				round(this.joinStoresPerSecond), round(this.joinProducePerSecond),
				round(this.aggregateInputRowPerSecond), round(this.aggregateProcessFuncPerSecond),
				round(this.predicatesPerSecond), round(this.predicateTuplesInputPerSecond),
				round(this.predicateTuplesOutputPerSecond), round(this.functionsPerSecond),
				round(this.functionTuplesPerSecond), round(this.tupleIdsPerSecond),
				round(this.msgsSentPerSecond),
				round(this.versionedScanPassPredPerSecond), round(this.versionedScanFailPredPerSecond),
				round(this.tuplesProbedPerSecond),
				round(this.indexLookupsPerSecond),
				round(this.fullTuplesSerializedPerSecond),
				round(this.fullTuplesDeserializedPerSecond),
				round(this.keyTuplesSerializedPerSecond),
				round(this.keyTuplesDeserializedPerSecond),
				round(this.indexedScanTuplesReadPerSecond));
		return sb.toString();
	}

	private int round(double d) {
		return (int) Math.round(d);
	}
	
	public double getPredicateLatency(double numInputTuples, double numPredicatesPerTuple, double selectivity) {
		return numInputTuples / predicateTuplesInputPerSecond + numInputTuples * numPredicatesPerTuple / predicatesPerSecond +
				numInputTuples * selectivity / predicateTuplesOutputPerSecond;
	}
	
	public double getFunctionLatency(double numInputTuples, int numFuncs) {
		return numInputTuples / functionTuplesPerSecond + numFuncs * numInputTuples / functionsPerSecond;
	}
	
	public double getJoinLatency(double inputSize, double outputSize) {
		return inputSize / joinStoresPerSecond + outputSize / joinProducePerSecond;
	}
	
	public double getAggregateLatency(double inputSize, double numFuncs) {
		return inputSize / aggregateInputRowPerSecond + inputSize * numFuncs / aggregateProcessFuncPerSecond;
	}
	
	public double getIdComputationTime(double numTuples) {
		return numTuples / tupleIdsPerSecond;
	}
		
	public double getMsgSendTime(double numMsgs) {
		return numMsgs / msgsSentPerSecond;
	}
	
	public double getMsgDeliverTime(double numMsgs) {
		return numMsgs / msgsDeliveredPerSecond;
	}
	
	public double getVersionedScanCost(double numRelationTuplesUpToRequestedEpoch, double keyPredSelectivity, double numReturned) {
		return numRelationTuplesUpToRequestedEpoch * (keyPredSelectivity / versionedScanPassPredPerSecond + (1 - keyPredSelectivity) / versionedScanFailPredPerSecond) +
			this.getFullTupleDeserializationTime(numReturned);
	}
	
	public double getIndexedScanCost(double numRelationTuplesAllVersions, double numReturnedTuples) {
		return numRelationTuplesAllVersions / indexedScanTuplesReadPerSecond + numReturnedTuples / fullTuplesDeserializedPerSecond;
	}
	
	public double getProbeCost(double numTuples) {
		return numTuples / tuplesProbedPerSecond;
	}
	
	public double getIndexLookupCost(double pageSize) {
		return (1 / indexLookupsPerSecond);
	}
	
	public double getPredEvalTime(double numTuples) {
		return numTuples / predicatesPerSecond;
	}
	
	public double getFullTupleSerializationTime(double numTuples) {
		return numTuples / fullTuplesSerializedPerSecond;
	}
	
	public double getFullTupleDeserializationTime(double numTuples) {
		return numTuples / fullTuplesDeserializedPerSecond;
	}
	
	public double getKeyTupleSerializationTime(double numTuples) {
		return numTuples / keyTuplesSerializedPerSecond;
	}
	
	public double getKeyTupleDeserializationTime(double numTuples) {
		return numTuples / keyTuplesDeserializedPerSecond;
	}
}
