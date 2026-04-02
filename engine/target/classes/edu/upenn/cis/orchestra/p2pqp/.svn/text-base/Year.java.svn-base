package edu.upenn.cis.orchestra.p2pqp;

import edu.upenn.cis.orchestra.datamodel.Date;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractImmutableTuple;
import edu.upenn.cis.orchestra.datamodel.DateType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;

public class Year extends Function {
	private static final long serialVersionUID = 1L;
	private static Year nullInstance = null, instance = null;

	private final boolean allowNulls;

	public static synchronized Year getInstance(boolean allowNulls) {
		if (allowNulls) {
			if (nullInstance == null) {
				nullInstance = new Year(true);
			}
			return nullInstance;
		} else {
			if (instance == null) {
				instance = new Year(false);
			}
			return instance;
		}
	}

	private Year(boolean allowNulls) {
		this.allowNulls = allowNulls;
	}

	public boolean equals(Object o) {
		return (o instanceof Year) && ((Year) o).allowNulls == allowNulls;
	}

	public synchronized byte[] evaluateFunction(AbstractImmutableTuple<?> input, byte[][] inputFields, int[] inputCols)
	throws ValueMismatchException, EvaluateException {
		if (inputCols.length != 1) {
			throw new IllegalArgumentException("Year takes one input");
		}
		if (inputCols[0] == Integer.MIN_VALUE) {
			return null;
		}
		
		byte[] data;
		if (inputCols[0] >= 0) {
			data = new byte[IntType.bytesPerInt];
			input.putBytes(data, 0);
			int year = Date.getYearFromBytes(data, 0, data.length);
			IntType.putBytes(year, data, 0);
			return data;
		} else {
			data = inputFields[-1 * inputCols[0] - 1];
			int year = Date.getYearFromBytes(data, 0, data.length);
			return IntType.getBytes(year);
		}
	}

	public int getNumInputs() {
		return 1;
	}

	public Type getOutputType() {
		return (allowNulls ? new IntType(true, false) : new IntType(false, false));
	}

	public int hashCode() {
		return 1776 + (allowNulls ? 1 : 2);
	}

	public boolean isValidForInput(int pos, Type inputType) {
		if (pos != 0) {
			throw new IllegalArgumentException("Year takes one argument");
		}
		return inputType.canPutInto(DateType.DATE);
	}


	protected void subclassSerialize(Document d, Element e,
			Map<String, QpSchema> schemas) {
		if (allowNulls) {
			e.setAttribute("allowNulls", Boolean.toString(true));
		}
	}

	public static Year deserialize(Element e, Map<String,QpSchema> schemas) {
		boolean allowNulls = Boolean.parseBoolean(e.getAttribute("allowNulls"));
		return getInstance(allowNulls);
	}
}
