package edu.upenn.cis.orchestra.p2pqp;

import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.predicate.Byteification;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.predicate.XMLification;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class FilterSerialization {
	private static final byte PREDICATE = 1, BLOOMFILTER = 2, CONJUNCTION = 4;
	private FilterSerialization() {
		
	}
	
	@SuppressWarnings("unchecked")
	public static <M> byte[] getBytes(Filter<? super QpTuple<M>> f, AbstractRelation ts) {
		if (f == null) {
			return null;
		} else if (ts == null) {
			throw new NullPointerException();
		} else if (f instanceof Predicate) {
			byte[] filterBytes = Byteification.getPredicateBytes(ts, (Predicate) f);
			byte[] retval = new byte[filterBytes.length + 1];
			retval[0] = PREDICATE;
			System.arraycopy(filterBytes, 0, retval, 1, filterBytes.length);
			return retval;
		} else if (f instanceof BloomFilter) {
			ByteBufferWriter bbw = new ByteBufferWriter();
			bbw.addToBuffer(BLOOMFILTER);
			BloomFilter bf = ((BloomFilter) f);
			if (bf.getRelation() != ts) {
				throw new IllegalArgumentException("Filter is for relation " + bf.getRelation().getName() + ", but supplied relation is " + ts.getName());
			}
			bf.getBytes(bbw);
			return bbw.getByteArray();
		} else if (f instanceof FilterConjunction) {
			ByteBufferWriter bbw = new ByteBufferWriter();
			bbw.addToBuffer(CONJUNCTION);
			for (Filter<? super QpTuple<?>> ff : ((FilterConjunction<QpTuple<?>>) f).filters) {
				bbw.addToBuffer(getBytes(ff, ts));
			}
			return bbw.getByteArray();
		} else {
			throw new RuntimeException("Don't know how to encode filter of type " + f.getClass().getName());
		}
	}
	
	public static Filter<? super QpTuple<?>> fromBytes(QpSchema ns, byte[] bytes) {
		if (bytes == null) {
			return null;
		}
		return fromBytes(ns, bytes, 0, bytes.length);
	}
	
	public static Filter<? super QpTuple<?>> fromBytes(QpSchema ns, byte[] bytes, int offset, int length) {
		if (bytes == null) {
			return null;
		}
		if (ns == null) {
			throw new NullPointerException();
		}
		Filter<? super QpTuple<?>> f;
		if (bytes[offset] == PREDICATE) {
			try {
				f = Byteification.getPredicateFromBytes(ns, bytes, offset + 1, length - 1);
			} catch (Exception e) {
				throw new RuntimeException("Error decoding serialized predicate", e);
			}
		} else if (bytes[0] == BLOOMFILTER) {
			f = new BloomFilter(ns, bytes, offset + 1, length - 1);		
		} else if (bytes[0] == CONJUNCTION) {
			ByteBufferReader bbr = new ByteBufferReader(null, bytes, offset + 1, length - 1);
			List<Filter<? super QpTuple<?>>> filters = new ArrayList<Filter<? super QpTuple<?>>>();
			while (! bbr.hasFinished()) {
				byte[] filterBytes = bbr.readByteArray();
				filters.add(fromBytes(ns, filterBytes));
			}
			return new FilterConjunction<QpTuple<?>>(filters);
		} else {
			throw new RuntimeException("Don't know how to decode filter of type " + bytes[0]);
		}
		return f;
	}
	
	@SuppressWarnings("unchecked")
	public static void serialize(Document d, Element e, Filter<? super QpTuple<?>> f, QpSchema ns) {
		if (f instanceof BloomFilter) {
			e.setAttribute("type", "bloomFilter");
			((BloomFilter) f).serialize(d, e);
		} else if (f instanceof Predicate) {
			e.setAttribute("type", "predicate");
			XMLification.serialize((Predicate) f, d, e, ns); 
		} else if (f instanceof FilterConjunction) {
			e.setAttribute("type", "conjunction");
			for (Filter<? super QpTuple<?>> ff : ((FilterConjunction<QpTuple<?>>) f).filters) {
				Element el = DomUtils.addChild(d, e, "filter");
				serialize(d,el,ff,ns);
			}
		} else {
			throw new IllegalArgumentException("Don't know how to serialize a " + f.getClass().getName());
		}
	}
	

	public static Filter<? super QpTuple<?>> deserialize(Element e, QpSchema ns) throws XMLParseException {
		String type = e.getAttribute("type");
		if (type.length() == 0) {
			throw new XMLParseException("Missing type attribute to indicate filter type", e);
		}

		if (type.equals("bloomFilter")) {
			return BloomFilter.deserialize(ns, e);
		} else if (type.equals("predicate")) {
			return XMLification.deserialize(e, ns);
		} else if (type.equals("conjunction")) {
			List<Element> children = DomUtils.getChildElements(e);
			List<Filter<? super QpTuple<?>>> conjunction = new ArrayList<Filter<? super QpTuple<?>>>(children.size());
			for (Element childEl : children) {
				if (! childEl.getTagName().equals("filter")) {
					throw new XMLParseException("Expected only 'filter' elements", childEl);
				}
				conjunction.add(deserialize(childEl, ns));
			}
			return new FilterConjunction<QpTuple<?>>(conjunction);
		} else {
			throw new XMLParseException("Don't know how to deserialize a " + type, e);
		}
	}
}
