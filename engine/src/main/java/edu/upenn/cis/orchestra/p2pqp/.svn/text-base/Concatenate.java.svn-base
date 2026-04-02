package edu.upenn.cis.orchestra.p2pqp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractImmutableTuple;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class Concatenate extends Function {
	private static final long serialVersionUID = 1L;
	private final boolean allowVaryingInputs;
	private final byte[][] betweenInputs;
	private final int[] inputLengths;
	private final StringType outputType;
	private final boolean allowNulls;
	private final int constantSerializedLength;
	
	public Concatenate(boolean allowVaryingInputs, boolean allowNulls, List<String> betweenInputs, List<Integer> inputLengths) {
		if (betweenInputs.size() != (inputLengths.size() + 1)) {
			throw new IllegalArgumentException("betweenInputs must have one more entry that inputLengths");
		}
		this.betweenInputs = new byte[betweenInputs.size()][];
		int constantSerializedLength = 0;
		for (int i = 0; i < this.betweenInputs.length; ++i) {
			if (betweenInputs.get(i) != null) {
				this.betweenInputs[i] = betweenInputs.get(i).getBytes(StringType.serializationCharset);
				constantSerializedLength += this.betweenInputs[i].length;
			}
		}
		this.inputLengths = new int[inputLengths.size()];
		for (int i = 0; i < this.inputLengths.length; ++i) {
			this.inputLengths[i] = inputLengths.get(i);
		}
		this.allowVaryingInputs = allowVaryingInputs;
		int length = 0;
		for (int inputLength : inputLengths) {
			length += inputLength;
		}
		for (String s : betweenInputs) {
			if (s != null) {
				length += s.length();
			}
		}
		
		this.allowNulls = allowNulls;
		outputType = new StringType(allowNulls, false, allowVaryingInputs, length);
		this.constantSerializedLength = constantSerializedLength;
	}
	
	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Concatenate c = (Concatenate) o;
		if (allowNulls != c.allowNulls) {
			return false;
		}
		
		if (allowVaryingInputs != c.allowVaryingInputs) {
			return false;
		}
		if (inputLengths.length != c.inputLengths.length) {
			return false;
		}
		
		if (! Arrays.equals(inputLengths, c.inputLengths)) {
			return false;
		}
		
		return (Arrays.equals(betweenInputs, c.betweenInputs));
	}

	@Override
	public byte[] evaluateFunction(AbstractImmutableTuple<?> input, byte[][] inputFields, int[] inputCols) throws ValueMismatchException, EvaluateException {
		if (inputCols.length != inputLengths.length) {
			throw new EvaluateException("Incorrect number of inputs");
		}
		for (int i = 0; i < inputCols.length; ++i) {
			if (inputCols[i] == Integer.MIN_VALUE || (inputCols[i] >= 0 && input.isNull(inputCols[i]))) {
				if (this.allowNulls) {
					return null;
				} else {
					throw new ValueMismatchException(new StringType(false,false,allowVaryingInputs,inputLengths[i]), null);
				}
			}
		}
		int length = this.constantSerializedLength;
		for (int i = 0; i < inputCols.length; ++i) {
			if (inputCols[i] >= 0) {
				length += input.getSerializedLength(inputCols[i]);
			} else {
				length += inputFields[-1 * inputCols[i] - 1].length;
			}
		}
		byte[] retval = new byte[length];
		int pos = 0;
		for (int i = 0; i < inputCols.length; ++i) {
			if (betweenInputs[i] != null) {
				final byte[] string = betweenInputs[i];
				final int stringLength = string.length;
				for (int j = 0; j < stringLength; ++j) {
					retval[pos++] = string[j];
				}
			}
			if (inputCols[i] >= 0) {
				pos += input.copyInto(retval, pos, inputCols[i]);
			} else {
				final byte[] string = inputFields[-1 * inputCols[i] - 1];
				final int stringLength = string.length;
				for (int j = 0; j < stringLength; ++j) {
					retval[pos++] = string[j];
				}
			}
		}
		if (betweenInputs[inputCols.length] != null) {
			final byte[] string = betweenInputs[inputCols.length];
			final int stringLength = string.length;
			for (int j = 0; j < stringLength; ++j) {
				retval[pos++] = string[j];
			}
		}
		return retval;
	}

	@Override
	public int getNumInputs() {
		return inputLengths.length;
	}

	@Override
	public Type getOutputType() {
		return outputType;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(inputLengths) + 37 * Arrays.hashCode(betweenInputs) + (allowVaryingInputs ? 79 : 0);
	}

	@Override
	public boolean isValidForInput(int pos, Type inputType) {
		if (pos >= inputLengths.length) {
			throw new IllegalArgumentException("Function does not have " + (pos + 1) + " inputs");
		}
		if (! (inputType instanceof StringType)) {
			return false;
		}
		StringType st = (StringType) inputType;
		if (((! allowVaryingInputs) && st.isVariableLength()) || st.getLength() > inputLengths[pos]) {
			return false;
		}
		if (this.allowNulls) {
			return true;
		} else {
			return (!(st.isNullable() || st.isLabeledNullable()));
		}
	}

	@Override
	protected void subclassSerialize(Document d, Element e,
			Map<String, QpSchema> schemas) {
		e.setAttribute("allowVaryingInputs", Boolean.toString(allowVaryingInputs));
		final int numInputs = inputLengths.length;
		
		for (int i = 0; i < numInputs; ++i) {
			if (betweenInputs[i] != null) {
				Element literal = DomUtils.addChild(d, e, "literal");
				literal.setAttribute("value", new String(betweenInputs[i], StringType.serializationCharset));
			} else {
				Element literal = DomUtils.addChild(d, e, "literal");
				literal.setAttribute("value", "");
			}
			Element input = DomUtils.addChild(d, e, "input");
			input.setAttribute("length", Integer.toString(inputLengths[i]));
		}
		if (betweenInputs[numInputs] != null) {
			Element string = DomUtils.addChild(d, e, "literal");
			string.setAttribute("value", new String(betweenInputs[numInputs], StringType.serializationCharset));
		}
		
		if (this.allowNulls) {
			e.setAttribute("allowNulls", Boolean.toString(true));
		}
	}
	
	public static Concatenate deserialize(Element e, Map<String,QpSchema> schemas) throws XMLParseException {
		boolean allowVaryingInputs = Boolean.parseBoolean(getAttribute(e, "allowVaryingInputs"));
		boolean allowNulls = Boolean.parseBoolean(e.getAttribute("allowNulls"));
		List<Element> els = DomUtils.getChildElements(e);
		List<Integer> inputLengths = new ArrayList<Integer>();
		List<String> betweenInputs = new ArrayList<String>();
		
		boolean gotBetween = false;
		for (Element el : els) {
			String tag = el.getTagName();
			if (tag.equals("literal")) {
				if (gotBetween) {
					throw new XMLParseException("Cannot have two consecutive literals in Concatenate function", el);
				} else {
					betweenInputs.add(getAttribute(el, "value"));
					gotBetween = true;
				}
			} else if (tag.equals("input")) {
				try {
					int length = Integer.parseInt(getAttribute(el,"length"));
					inputLengths.add(length);
				} catch (NumberFormatException nfe) {
					throw new XMLParseException("Invalid string length", nfe, el);
				}
				if (! gotBetween) {
					betweenInputs.add(null);
				}
				gotBetween = false;
			}
		}
		
		if (betweenInputs.size() != (inputLengths.size() + 1)) {
			throw new IllegalArgumentException("betweenInputs must have one more entry that inputLengths");
		}
		
		return new Concatenate(allowVaryingInputs, allowNulls, betweenInputs, inputLengths);
	}

}
