package edu.upenn.cis.orchestra.p2pqp;

import java.util.Collection;

import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;


public class NullMetadataFactory implements MetadataFactory<Null> {
	private static final NullMetadataFactory instance;
	static {
		instance = new NullMetadataFactory();
	}
	
	public static NullMetadataFactory getInstance() {
		return instance;
	}
	public Class<Null> getMetadataClass() {
		return Null.class;
	}
	public Null addMetadata(Null newMetadata, Null oldMetadata) {
		throw new UnsupportedOperationException();
	}
	public Null agg(HashAggregator<Null> o, Collection<Null> inputs) {
		throw new UnsupportedOperationException();
	}
	public Null applyFunctions(FunctionEvaluator<Null> o, Null m) {
		throw new UnsupportedOperationException();
	}
	public Null differenceMetadata(Null newMetadata, Null oldMetadata) {
		throw new UnsupportedOperationException();
	}
	public Null fromBytes(Source findSchema, byte[] bytes,
			int offset, int length) {
		throw new UnsupportedOperationException();
	}
	public boolean isZero(Null m) {
		throw new UnsupportedOperationException();
	}
	public String printMetadata(Null m) {
		throw new UnsupportedOperationException();
	}
	public Null scan(Operator<Null> o, QpTuple<?> tuple, Null inStorage) {
		throw new UnsupportedOperationException();
	}
	public byte[] toBytes(Null m) {
		throw new UnsupportedOperationException();
	}
	public Null multiplyMetadata(Null metadata1, Null metadata2) {
		throw new UnsupportedOperationException();
	}
	public Null project(ProjectOperator<Null> o, Null m) {
		throw new UnsupportedOperationException();
	}
	public int getCardinality(Null metadata) {
		throw new UnsupportedOperationException();
	}
	public Null zero() {
		throw new UnsupportedOperationException();
	}
	public boolean equals(Null m1, Null m2) {
		throw new UnsupportedOperationException();
	}
	public int hashCode(Null metadata) {
		throw new UnsupportedOperationException();
	}
}
