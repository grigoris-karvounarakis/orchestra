package edu.upenn.cis.orchestra.p2pqp;
import java.util.Collection;
import java.util.Date;

import edu.upenn.cis.orchestra.datamodel.TimestampType;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;

public class DateMetaDataFactory implements MetadataFactory<Date> {

	private static final DateMetaDataFactory instance;
	

	static {
		instance = new DateMetaDataFactory();
	}
	
	public static DateMetaDataFactory getInstance() {
		return instance;
	}
	
	public Date agg(HashAggregator<Date> o, Collection<Date> inputs) {
		Date retval = null;
		for (Date input : inputs) {
			if (retval == null || retval.before(input)) {
				retval = input;
			}
		}
		return retval;
	}
	
	public String printMetadata(Date m) {
		return m.toString();
	}
	
	public int sizeOf(Date m) {
		return 0;
	}

	public Date fromBytes(Source findSchema, byte[] bytes, int offset, int length) {
		return TimestampType.getValFromBytes(bytes);
	}
	
	public byte[] toBytes(Date m) {
		return TimestampType.getBytes(m);
		
	}

	public boolean isZero(Date m) {
		return false;
	}

    public Date differenceMetadata(Date newMetadata, Date oldMetadata) {
		if (newMetadata.after(oldMetadata)) {
			return newMetadata;
		} else {
			return oldMetadata;
		}
    }

	public Class<Date> getMetadataClass() {
		return Date.class;
	}

	public Date addMetadata(Date newMetadata, Date oldMetadata) {
		if (newMetadata.after(oldMetadata)) {
			return newMetadata;
		} else {
			return oldMetadata;
		}
	}

	public Date scan(Operator<Date> o, QpTuple<?> tuple, Date inStorage) {
		return new Date();
	}

	public Date multiplyMetadata(Date metadata1, Date metadata2) {
		if (metadata1.after(metadata2)) {
			return metadata1;
		} else {
			return metadata2;
		}
	}

	public Date applyFunctions(FunctionEvaluator<Date> o, Date m) {
		return m;
	}

	public Date project(ProjectOperator<Date> o, Date m) {
		return m;
	}

	public int getCardinality(Date metadata) {
		return 1;
	}

	public Date zero() {
		throw new UnsupportedOperationException();
	}

	public boolean equals(Date m1, Date m2) {
		throw new UnsupportedOperationException();
	}

	public int hashCode(Date metadata) {
		throw new UnsupportedOperationException();
	}

}
