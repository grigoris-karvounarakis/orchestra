package edu.upenn.cis.orchestra.p2pqp;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class BloomFilter implements Serializable, TupleSetFilter {
	private static final long serialVersionUID = 1L;
	// Sizes should be prime to make hash functions independent.
	// To avoid wasting space, we'll make them slightly smaller
	// than a multiple of 32
	private final static int sizes[] = { 31, 61, 89, 127, 251, 509, 631, 797, 1021,
			2039, 4093, 8191}; 
	private final int[] filter;
	private final int numBits;
	private final int numFuncs;
	private final int[] columns;
	private QpSchema relation;
	
	private final static double LOG_TWO = Math.log(2);
		
	public BloomFilter(int numBits, int expectedNumElements, QpSchema relation, int[] columns) {
		this(numBits, computeNumFuncs(numBits,expectedNumElements), relation, columns, true);
	}
		
	private static int computeNumFuncs(int numBits, int numElements) {
		if (numElements <= 0 || numBits <= 0) {
			throw new IllegalArgumentException("Both filter size and expected number of elements must be positive");
		}
		return (int) Math.ceil(((double) numBits) * LOG_TWO / numElements);
	}
		
	private BloomFilter(int numBits, int numFuncs, QpSchema relation, int[] columns, boolean dummy) {
		int index = Arrays.binarySearch(sizes, numBits);
		this.numFuncs = numFuncs;
		if (index < 0) {
			// Value is not in array
			int sizePos = -index - 1;
			if (sizePos < sizes.length) {
				numBits = sizes[sizePos];
			} else {
				Logger logger = Logger.getLogger(this.getClass());
				logger.warn("Request to create Bloom Filter with " + numBits + " bits exceeds known filter sizes, using " + sizes[sizes.length - 1] + " bits");
				numBits = sizes[sizes.length - 1];
			}
		}
		// No size is a multiple of 32 so this is safe
		int numFields = (numBits / Integer.SIZE) + 1;
		filter = new int[numFields];
		this.numBits = numBits;
		this.columns = new int[columns.length];
		System.arraycopy(columns, 0, this.columns, 0, columns.length);
		this.relation = relation;
	}
	
	public void add(QpTuple<?> t) {
		if (t.getSchema() != relation) {
			throw new IllegalArgumentException("Can only add tuples from relation " + relation.getName());
		}
		int hash1, hash2;
		if (columns == null) {
			hash1 = t.hashCode();
			hash2 = t.hashCode2();
		} else {
			hash1 = t.hashCode(columns);
			hash2 = t.hashCode2(columns);
		}
		
		for (int i = 0; i < numFuncs; ++i) {
			int pos = (int) ((hash1 + i * ((long) hash2)) % numBits);
			if (pos >= 0) {
				set(pos);
			} else {
				set(-pos);
			}
		}
	}
	
	public boolean eval(QpTuple<?> t) {
		if (t.getSchema() != relation) {
			throw new IllegalArgumentException("Can only evaluate tuples from relation " + relation);
		}
		BloomFilter tupleFilter = new BloomFilter(numBits, numFuncs, relation, columns, true);
		tupleFilter.add(t);
		for (int i = 0; i < filter.length; ++i) {
			if ((filter[i] & tupleFilter.filter[i]) != tupleFilter.filter[i]) {
				return false;
			}
		}
		return true;
	}
	
	private void set(int i) {
		int field = i / Integer.SIZE;
		int offset = i % Integer.SIZE;
		int mask = 1 << offset;
		filter[field] |= mask;
	}
	
	public int getNumBits() {
		return numBits;
	}
	
	byte[] getBytes() {
		ByteBufferWriter bbw = new ByteBufferWriter();
		getBytes(bbw);
		return bbw.getByteArray();
	}
	
	void getBytes(ByteBufferWriter bbw) {
			bbw.addToBuffer(relation.relId);
		bbw.addToBuffer(numBits);
		bbw.addToBuffer(numFuncs);
		bbw.addToBuffer(columns.length);
		for (int col : columns) {
			bbw.addToBuffer(col);
		}
		bbw.addToBuffer(filter.length);
		for (int field : filter) {
			bbw.addToBuffer(field);
		}
	}
	
	BloomFilter(QpSchema relation, byte[] data, int offset, int length) {
		ByteBufferReader bbr = new ByteBufferReader(null, data, offset, length);
		int relId = bbr.readInt();
		if (relId != relation.relId) {
			throw new IllegalArgumentException("Bloom filter is for relation with ID " + relId + ", but supplied schema for relation " + relation.getName() + " has ID " + relation.relId);
		}
		this.relation = relation;
		numBits = bbr.readInt();
		numFuncs = bbr.readInt();
		final int numCols = bbr.readInt();
		columns = new int[numCols];
		for (int i = 0; i < numCols; ++i) {
			columns[i] = bbr.readInt();
		}
		int numFilterFields = bbr.readInt();
		filter = new int[numFilterFields];
		int pos = 0;
		while (pos < numFilterFields) {
			filter[pos] = bbr.readInt();
			++pos;
		}
	}
	
	BloomFilter(QpSchema relation, byte[] data) {
		this(relation, data, 0, data.length);
	}

	public void changeRelation(QpSchema relation, int[] columns) {
		if (columns.length != this.columns.length) {
			throw new IllegalArgumentException("Cannot change number of columns");
		}
		this.relation = relation;
		System.arraycopy(columns, 0, this.columns, 0, columns.length);
	}

	public Set<Integer> getColumns() {
		Set<Integer> retval = new HashSet<Integer>(columns.length);
		for (int col : columns) {
			retval.add(col);
		}
		return retval;
	}

	public QpSchema getRelation() {
		return relation;
	}
	
	public void serialize(Document doc, Element e) {
		e.setAttribute("numBits", Integer.toString(numBits));
		e.setAttribute("numFuncs", Integer.toString(numFuncs));
		e.setAttribute("relationId", Integer.toString(relation.relId));
		Element cols = DomUtils.addChild(doc, e, "cols");
		writeList(doc, cols, "col", "pos", columns);
		Element fields = DomUtils.addChild(doc, e, "fields");
		writeList(doc, fields, "field", "value", filter);
	}
	
	public static BloomFilter deserialize(QpSchema relation, Element e) throws XMLParseException {
		int numBits = Integer.parseInt(getAttribute(e, "numBits"));
		int numFuncs = Integer.parseInt(getAttribute(e, "numFuncs"));
		String relationIdStr = getAttribute(e,"relationId");
		if (relationIdStr == null) {
			throw new XMLParseException("Missing relatition ID");
		}
		try {
			int relationId = Integer.parseInt(relationIdStr);
			if (relation.relId != relationId) {
				throw new XMLParseException("Bloom filter is for relation with ID " + relationId + ", but supplied schema for relation " + relation.getName() + " has ID " + relation.relId);
			}
		} catch (NumberFormatException nfe) {
			throw new XMLParseException("Invalid relation id", nfe);
		}
				
		Element colsEl = DomUtils.getChildElementByName(e, "cols");
		if (colsEl == null) {
			throw new XMLParseException("Missing child cols tag", e);
		}
		int[] columns = readList(colsEl,"col","pos");
		
		Element fieldsEl = DomUtils.getChildElementByName(e, "fields");
		if (fieldsEl == null) {
			throw new XMLParseException("Missing child fields tag", e);
		}
		int[] fields = readList(fieldsEl,"field","value");
		
		return new BloomFilter(fields, numBits, numFuncs, columns, relation);
	}
	
	private BloomFilter(int[] filter, int numBits, int numFuncs, int[] columns, QpSchema relation) {
		this.filter = filter;
		this.numBits = numBits;
		this.numFuncs = numFuncs;
		this.columns = new int[columns.length];
		System.arraycopy(columns, 0, this.columns, 0, this.columns.length);
		this.relation = relation;
	}
	
	private static void writeList(Document doc, Element parent, String tagName, String attName, int[] values) {
		for (int value : values) {
			Element el = DomUtils.addChild(doc, parent, tagName);
			el.setAttribute(attName, Integer.toString(value));
		}
	}
	
	private static int[] readList(Element parent, String tagName, String attName) throws XMLParseException {
		List<Integer> values = new ArrayList<Integer>();
		List<Element> els = DomUtils.getChildElementsByName(parent, tagName);
		
		for (Element el : els) {
			values.add(Integer.parseInt(getAttribute(el,attName)));
		}
		return fromList(values);
	}
		
	private static int[] fromList(List<Integer> values) {
		int[] retval = new int[values.size()];
		int pos = 0;
		for (int value : values) {
			retval[pos++] = value;
		}
		return retval;
	}
	
	static String getAttribute(Element el, String attName) throws XMLParseException {
		String attVal = el.getAttribute(attName);
		if (attVal.length() == 0) {
			throw new XMLParseException("Missing " + attName + " attribute", el);
		}
		return attVal;
	}

	public String toString() {
		return "BloomFilter";
	}
}
