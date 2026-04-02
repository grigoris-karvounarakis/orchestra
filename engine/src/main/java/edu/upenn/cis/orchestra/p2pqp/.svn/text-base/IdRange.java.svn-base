package edu.upenn.cis.orchestra.p2pqp;

import java.io.Serializable;
import java.math.BigInteger;

import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;


public class IdRange implements Serializable, Comparable<IdRange> {
	private static final long serialVersionUID = 1L;
	private final boolean empty;
	private final Id ccw, cw;

	/**
	 *	Construct an empty IdRange
	 * 
	 */
	private IdRange(boolean empty) {
		this.empty = empty;
		ccw = null;
		cw = null;
	}

	private static final IdRange emptyRange = new IdRange(true);
	private static final IdRange fullRange = new IdRange(false);

	public static IdRange empty() {
		return emptyRange;
	}

	public static IdRange full() {
		return fullRange;
	}

	/**
	 * Construct a non-empty IdRange
	 * 
	 * @param ccw		The CCW bound, inclusive
	 * @param cw		The CW bound, exclusive 
	 */
	public IdRange(Id ccw, Id cw) {
		empty = false;
		if (ccw.equals(cw)) {
			this.ccw = null;
			this.cw = null;
		} else {
			this.ccw = ccw;
			this.cw = cw;
		}
	}

	public boolean contains(Id id) {
		if (isEmpty()) {
			return false;
		}
		if (isFull()) {
			return true;
		}
		boolean wraps = wraps();
		if (wraps) {
			return (ccw.compareTo(id) <= 0 || cw.compareTo(id) > 0);
		} else {
			return (ccw.compareTo(id) <= 0 && cw.compareTo(id) > 0);
		}
	}

	public boolean containsMSB(byte[] idBytes, int offset) {
		if (isEmpty()) {
			return false;
		}
		if (isFull()) {
			return true;
		}
		boolean wraps = wraps();
		if (wraps) {
			return (ccw.compareToMSB(idBytes, offset) <= 0 || cw.compareToMSB(idBytes, offset) > 0);
		} else {
			return (ccw.compareToMSB(idBytes, offset) <= 0 && cw.compareToMSB(idBytes, offset) > 0);
		}
	}


	public void serialize(OutputBuffer buf) {
		if (empty) {
			buf.writeBoolean(true);
		} else {
			buf.writeBoolean(false);
			if (ccw == null) {
				buf.writeBoolean(true);
			} else {
				buf.writeBoolean(false);
				ccw.serialize(buf);
				cw.serialize(buf);
			}
		}
	}

	public static IdRange deserialize(InputBuffer buf) {
		boolean empty = buf.readBoolean();
		if (empty) {
			return emptyRange;
		}
		boolean full = buf.readBoolean();
		if (full) {
			return fullRange;
		}
		Id ccw = Id.deserialize(buf);
		Id cw = Id.deserialize(buf);
		return new IdRange(ccw,cw);
	}

	public String toString() {
		if (empty) {
			return "[]";
		} else if (ccw == null) {
			return "ALL";
		} else {
			return "[" + ccw + "," + cw + ")";
		}
	}

	/**
	 * Return the CCW edge, inclusive
	 * 
	 * @return		The CCW edge
	 */
	public Id getCCW() {
		return ccw;
	}

	/**
	 * Return the CW edge, exclusive
	 * 
	 * @return		The CW edge
	 */
	public Id getCW() {
		return cw;
	}

	public boolean isEmpty() {
		return empty;
	}

	public boolean isFull() {
		if (empty) {
			return false;
		}
		return ccw == null;
	}

	boolean wraps() {
		return (ccw.compareTo(cw) > 0);
	}

	public boolean contains(IdRange range) {
		if (isFull()) {
			return true;
		} else if (range.isFull()) {
			return false;
		}
		if (wraps()) {
			if (range.wraps()) {
				return ccw.compareTo(range.ccw) <= 0 && cw.compareTo(range.cw) >= 0;
			} else {
				return (ccw.compareTo(range.ccw) <= 0 && ccw.compareTo(range.cw) < 0) ||
				(cw.compareTo(range.ccw) > 0 && cw.compareTo(range.cw) >= 0);
			}
		} else if (range.wraps()) {
			return false;
		} else {
			return (ccw.compareTo(range.ccw) <= 0 && cw.compareTo(range.cw) >= 0);
		}
	}

	public boolean intersects(IdRange range) {
		if (this.isEmpty() || range.isEmpty()) {
			return false;
		}
		if (this.isFull() || range.isFull()) {
			return true;
		}
		if (contains(range) || range.contains(this)) {
			return true;
		}
		return (range.contains(ccw) || contains(range.ccw));
	}

	public boolean equals(Object o) {
		IdRange range = (IdRange) o;
		if (o == null || o.getClass() != IdRange.class) {
			return false;
		}
		if (isFull()) {
			return range.isFull();
		} else if (range.isFull()) {
			return false;
		} else if (isEmpty()) {
			return range.isEmpty();
		} else if (range.isEmpty()) {
			return false;
		}
		return ccw.equals(range.ccw) && cw.equals(range.cw);
	}

	public IdRange intersect(IdRange range) {
		if (isFull()) {
			return range;
		} else if (range.isFull()) {
			return this;
		} else if (isEmpty() || range.isEmpty()) {
			return emptyRange;
		} else if (range.contains(this)) {
			return this;
		} else if (this.contains(range)) {
			return range;
		} else if (contains(range.ccw)) {
			return new IdRange(range.ccw, cw);
		} else if (range.contains(ccw)) {
			return new IdRange(ccw, range.cw);
		}
		return emptyRange;
	}

	public static final int estimatedSerializedLength = 2 * Id.idLengthBytes + 2;

	public int compareTo(IdRange range) {
		if (this.isEmpty()) {
			if (range.isEmpty()) {
				return 0;
			} else {
				return -1;
			}
		}
		if (this.isFull()) {
			if (range.isFull()) {
				return 0;
			} else {
				return 1;
			}
		}
		return ccw.compareTo(range.ccw);
	}

	public double getSize() {
		if (isFull()) {
			return Id.MAX.doubleValue();
		} else if (isEmpty()) {
			return 0.0;
		}
		
		if (wraps()) {
			return Id.MAX.doubleValue() - getCCW().doubleValue() + getCW().doubleValue();
		} else {
			return getCW().doubleValue() - getCCW().doubleValue();
		}
	}
	
	public BigInteger getIntegerSize() {
		if (isFull()) {
			return Id.MAX_BIGINT;
		} else if (isEmpty()) {
			return BigInteger.ZERO;
		} else {
			if (wraps()) {
				return Id.MAX_BIGINT.subtract(this.ccw.getBigInt()).add(cw.getBigInt());
			} else {
				return this.cw.getBigInt().subtract(this.ccw.getBigInt());
			}
		}
	}
	
	public IdRange[] split(int numParts) {
		if (this.isEmpty()) {
			return new IdRange[0];
		}
		IdRange[] retval = new IdRange[numParts];
		final BigInteger partSize = getIntegerSize().divide(BigInteger.valueOf(numParts));
		Id start = this.isFull() ? Id.ZERO : ccw;
		Id pos = start;
		for (int i = 0; i < numParts - 1; ++i) {
			Id end = pos.add(partSize);
			retval[i] = new IdRange(pos, end);
			pos = end;
		}
		retval[numParts-1] = new IdRange(pos, this.isFull() ? Id.ZERO : cw);
		return retval;
	}
}
