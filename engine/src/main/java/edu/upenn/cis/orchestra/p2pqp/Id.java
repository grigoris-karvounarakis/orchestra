package edu.upenn.cis.orchestra.p2pqp;

import static java.lang.Character.forDigit;
import static java.lang.Character.toUpperCase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import com.sleepycat.je.DatabaseEntry;

import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class Id implements Comparable<Id>, Serializable {
	private static final long serialVersionUID = 1L;
	static final int idLengthBytes = 20;
	// Represented as an unsigned big-endian integer
	private final byte[] id;
	static final Id ZERO = fromMSBBytes(new byte[idLengthBytes]);
	static final Id MAX;
	
	static final BigInteger MAX_BIGINT, NUM_IDS;
	static {
		byte[] MAX = new byte[idLengthBytes];
		for (int i = 0; i < MAX.length; ++i) {
			MAX[i] = (byte) 0xFF;
		}
		MAX_BIGINT = new BigInteger(1, MAX);
		NUM_IDS = MAX_BIGINT.add(BigInteger.ONE);
	}

	static {
		MAX = new Id(MAX_BIGINT);
	}
	private Id(byte[] id) {
		if (id.length != idLengthBytes) {
			throw new IllegalArgumentException("ID length must be " + idLengthBytes + " bytes");
		}
		this.id = id;
	}
	
	static Id fromMSBEntry(DatabaseEntry de) {
		return new Id(de.getData());
	}
	
	
	/**
	 * Construct an ID from a byte array holding the
	 * big-endian representation of an unsigned integer
	 * 
	 * @param bytes		The unsigned integer in big-endian representation			
	 * @return			The ID
	 */
	static Id fromMSBBytes(byte[] bytes) {
		byte[] id = new byte[bytes.length];
		System.arraycopy(bytes, 0, id, 0, bytes.length);
		return new Id(id);
	}
	
	/**
	 * Construct an ID from a byte array holding the
	 * big-endian representation of an unsigned integer
	 * 
	 * @param bytes		The unsigned integer in big-endian representation
	 * @param offset	The offset in <code>bytes</code> where the ID starts
	 * @return			The ID
	 */
	static Id fromMSBBytes(byte[] bytes, int offset) {
		byte[] id = new byte[idLengthBytes];
		System.arraycopy(bytes, offset, id, 0, idLengthBytes);
		return new Id(id);
	}

	/**
	 * Construct an ID from a byte array holding the
	 * little-endian representation of an unsigned integer
	 * 
	 * @param bytes		The unsigned integer in little-endian representation			
	 * @return			The ID
	 */
	static Id fromLSBBytes(byte[] bytes) {
		byte[] data = new byte[bytes.length];
		int half = data.length / 2;
		// Reverse the array to make is MSB-first
		for (int i = 0; i < half; ++i) {
			data[i] = bytes[data.length - i - 1];
			data[data.length - i - 1] = bytes[i];
		}
		return new Id(data);
	}
		
	/**
	 * Turn a byte representation of some content into
	 * an Id for the P2P network. This may perform hashing or
	 * some other unspecified scrambling before creating the
	 * actual Id
	 * 
	 * @param bytes 	The input to turn into an Id
	 * @return			An Id derived from <code>bytes</code>
	 */
	static Id fromContent(byte[] bytes) {
		return fromMSBBytes(hash(bytes));
	}
		
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		return Arrays.equals(id, ((Id) o).id);
	}

	private static ThreadLocal<MessageDigest> dispenser = new ThreadLocal<MessageDigest>() {
		protected MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance("SHA-1");
			} catch (NoSuchAlgorithmException nsae) {
				throw new RuntimeException("Couldn't get SHA-1 digester!");
			}
		}

		public MessageDigest get() {
			MessageDigest md = super.get();
			md.reset();
			return md;
		}
	};

	
	private static byte[] hash(byte[] material) {
		MessageDigest md = dispenser.get();
		md.update(material);
		return md.digest();
	}
	
	public static byte[] getSerializedFromContent(byte[] content) {
		return hash(content);
	}

	public int compareTo(Id other) {
		for (int i = 0; i < id.length; ++i) {
			int comp = (0xFF & id[i]) - (0xFF & other.id[i]);
			if (comp != 0) {
				return comp;
			}
		}
		return 0;
	}
	
	public int compareToMSB(byte[] idBytes, int offset) {
		for (int i = 0; i < id.length; ++i) {
			int comp = (0xFF & id[i]) - (0xFF & idBytes[offset + i]);
			if (comp != 0) {
				return comp;
			}
		}
		return 0;
	}
	
	public int hashCode() {
		return Arrays.hashCode(id);
	}
	
	public void serialize(OutputBuffer buf) {
		buf.writeBytesNoLength(id);
	}
	
	public static Id deserialize(InputBuffer buf) {
		byte[] bytes = buf.readBytes(idLengthBytes);
		return new Id(bytes);
	}
	
	public void writeTo(OutputBuffer out) {
		out.writeBytesNoLength(id);
	}
	
	public static Id readFrom(InputBuffer in) {
		byte[] bytes = in.readBytes(idLengthBytes);
		return new Id(bytes);
	}

	public void writeTo(DataOutput out) throws IOException {
		out.write(id);
	}
	
	public static Id readFrom(DataInput in) throws IOException {
		byte[] bytes = new byte[idLengthBytes];
		in.readFully(bytes);
		return new Id(bytes);
	}
	
	public String toString() {
		char idString[] = new char[idLengthBytes * 2];

		for (int i = 0; i < idLengthBytes; ++i) {
			idString[2*i] = toUpperCase(forDigit((id[i] & 0xFF) >>> 4, 16));
			idString[2*i+1] = toUpperCase(forDigit(id[i] & 0xF, 16));
		}
		
		return new String(idString);
	}
	
	public static Id fromString(String id) {
		if (id.length() != 2 * idLengthBytes) {
			throw new IllegalArgumentException("String must have length " + (2 * idLengthBytes));
		}
		
		byte[] bytes = new byte[idLengthBytes];
		for (int i = 0; i < idLengthBytes; ++i) {
			char high = id.charAt(2*i), low = id.charAt(2*i+1);
			byte highDigit = (byte) Character.digit(high, 16);
			byte lowDigit = (byte) Character.digit(low, 16);
			if (highDigit < 0 || lowDigit < 0) {
				throw new IllegalArgumentException("String must be a hexadecimal number");
			}
			bytes[i] = lowDigit;
			bytes[i] |= (highDigit << 4);
		}
		return new Id(bytes);
	}
	
	byte[] getMSBBytes() {
		byte[] retval = new byte[id.length];
		System.arraycopy(id, 0, retval, 0, retval.length);
		return retval;
	}
	
	byte[] getLSBBytes() {
		byte[] data = new byte[id.length];
		int half = data.length / 2;
		// Reverse the array to make is LSB-first
		for (int i = 0; i < half; ++i) {
			data[i] = id[id.length - i - 1];
			data[id.length - i - 1] = id[i];
		}
		return data;
	}
	
	void copyIntoMSB(byte[] dest, int offset) {
		System.arraycopy(id, 0, dest, offset, id.length);
	}
	
	private static final BigInteger TWO = BigInteger.valueOf(2);
	
	Id findHalfway(Id cwId) {
		BigInteger ccwBi = this.getBigInt(), cwBi = cwId.getBigInt();
		BigInteger diff = cwBi.subtract(ccwBi);
		if (diff.signum() < 0) {
			diff = diff.add(NUM_IDS);
		}
		BigInteger halfWay = ccwBi.add(diff.divide(TWO));
		if (halfWay.compareTo(MAX_BIGINT) >= 0) {
			halfWay = halfWay.subtract(NUM_IDS);
		}
		return new Id(halfWay);
	}
	
	BigInteger getBigInt() {
		return new BigInteger(1, id);
	}
	
	public double doubleValue() {
		return getBigInt().doubleValue();
	}
	
	Id(BigInteger bi) {
		if (bi.signum() < 0) {
			throw new IllegalArgumentException("BigInteger must be positive");
		}
		byte[] data = bi.toByteArray();
		id = new byte[idLengthBytes];
		int copyLength = data.length > idLengthBytes ? idLengthBytes : data.length;
		System.arraycopy(data, data.length - copyLength, id, idLengthBytes - copyLength, copyLength);
	}
	
	Id(String hexString) {
		this(new BigInteger(hexString,16));
	}
	
	Id getPredecessor() {
		BigInteger big = getBigInt();
		BigInteger pred = big.subtract(BigInteger.ONE);
		if (pred.signum() < 0) {
			pred = pred.add(NUM_IDS);
		}
		return new Id(pred);
	}
	
	void setDatabaseKey(DatabaseEntry de) {
		de.setData(id);
	}
	
	Id add(BigInteger increment) {
		BigInteger result = getBigInt().add(increment);
		if (result.compareTo(MAX_BIGINT) >= 0) {
			result = result.subtract(NUM_IDS);
		}
		return new Id(result);
	}
}
