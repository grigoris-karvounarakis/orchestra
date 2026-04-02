package edu.upenn.cis.orchestra.p2pqp;

import java.util.Collection;

import static edu.upenn.cis.orchestra.datamodel.IntType.getBytes;
import static edu.upenn.cis.orchestra.datamodel.IntType.getValFromBytes;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;

public class CountMetadataFactory implements MetadataFactory<Integer> {
	static private final Integer one = 1;
	static private final Integer zero = 0;
	public Integer addMetadata(Integer newMetadata, Integer oldMetadata) {
		return newMetadata + oldMetadata;
	}

	public Integer agg(HashAggregator<Integer> o, Collection<Integer> inputs) {
		return one;
	}

	public Integer applyFunctions(FunctionEvaluator<Integer> o, Integer m) {
		return m;
	}

	public Integer differenceMetadata(Integer newMetadata, Integer oldMetadata) {
		return newMetadata - oldMetadata;
	}

	public Integer fromBytes(Source findSchema, byte[] bytes,
			int offset, int length) {
		return getValFromBytes(bytes, offset);
	}

	public int getCardinality(Integer metadata) {
		return metadata;
	}

	public Class<Integer> getMetadataClass() {
		return Integer.class;
	}

	public boolean isZero(Integer m) {
		return ((int) m) == 0;
	}

	public Integer multiplyMetadata(Integer metadata1, Integer metadata2) {
		return metadata1 * metadata2;
	}

	public String printMetadata(Integer m) {
		return m.toString();
	}

	public Integer project(ProjectOperator<Integer> o, Integer m) {
		return m;
	}

	public Integer scan(Operator<Integer> o, QpTuple<?> tuple, Integer inStorage) {
		return one;
	}

	public byte[] toBytes(Integer m) {
		return getBytes((int) m);
	}

	public Integer zero() {
		return zero;
	}

	public boolean equals(Integer m1, Integer m2) {
		return m1 == m2;
	}

	public int hashCode(Integer metadata) {
		return metadata;
	}

	private static final CountMetadataFactory instance = new CountMetadataFactory();

	private CountMetadataFactory() {
	}
	
	
	public static CountMetadataFactory getInstance() {
		return instance;
	}
}
