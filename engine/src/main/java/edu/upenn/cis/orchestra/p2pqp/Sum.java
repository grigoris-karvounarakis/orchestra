package edu.upenn.cis.orchestra.p2pqp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractImmutableTuple;
import edu.upenn.cis.orchestra.datamodel.DoubleType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.LongType;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class Sum extends Function {
	private static final long serialVersionUID = 1L;
	private final boolean allowDoubles, allowLongs;
	private final boolean hasConstantValue;
	private final int constantInt;
	private final double constantDouble;
	private final long constantLong;
	private final int[] multiplicities;
	private final boolean allowNulls;

	public Sum(List<Integer> multiplicities, boolean allowNulls,boolean allowDoubles, boolean allowLongs) {
		this(multiplicities, allowNulls, allowDoubles, allowLongs, null);
	}

	public Sum(List<Integer> multiplicities, boolean allowNulls, boolean allowDoubles, boolean allowLongs, Number constantValue) {
		this.allowNulls = allowNulls;
		this.multiplicities = new int[multiplicities.size()];
		int pos = 0;
		for (Integer mult : multiplicities) {
			this.multiplicities[pos++] = mult;
		}
		this.allowDoubles = allowDoubles;
		this.allowLongs = allowLongs;

		if (constantValue == null) {
			constantDouble = 0;
			constantLong = 0;
			constantInt = 0;
			hasConstantValue = false;
		} else if (constantValue instanceof Integer) {
			if (allowDoubles) {
				constantInt = 0;
				constantLong = 0;
				constantDouble = (Integer) constantValue;
			} else if (allowLongs) {
				constantInt = 0;
				constantDouble = 0;
				constantLong = (Integer) constantValue;
			} else {
				constantDouble = 0;
				constantLong = 0;
				constantInt = (Integer) constantValue;
			}
			hasConstantValue = true;
		} else if (constantValue instanceof Double) {
			if (! allowDoubles) {
				throw new IllegalArgumentException("Cannot specify a double constant value if doubles are not allowed");
			}
			constantInt = 0;
			constantLong = 0;
			constantDouble = (Double) constantValue;
			hasConstantValue = true;
		} else if (constantValue instanceof Long) {
			if (! allowLongs) {
				throw new IllegalArgumentException("Cannot specify a long constant value if longs are not allowed");
			}
			if (allowDoubles) {
				constantInt = 0;
				constantLong = 0;
				constantDouble = (Long) constantValue;
			} else {
				constantInt = 0;
				constantDouble = 0;
				constantLong = (Long) constantValue;
			}
			hasConstantValue = true;
		} else {
			throw new IllegalArgumentException("Constant value must be an integer, double, or long");
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Sum s = (Sum) o;
		return (allowNulls == s.allowNulls && allowDoubles == s.allowDoubles && allowLongs == s.allowLongs && 
				constantInt == s.constantInt && constantLong == s.constantLong && constantDouble == s.constantDouble &&
				Arrays.equals(multiplicities, s.multiplicities));
	}

	@Override
	public byte[] evaluateFunction(AbstractImmutableTuple<?> input, byte[][] inputFields, int[] inputCols) throws ValueMismatchException,
	EvaluateException {
		// TODO: reimplement Sum
		throw new UnsupportedOperationException("Need to implement");
		/*
		if (input.length != multiplicities.length) {
			throw new EvaluateException("Wrong number of inputs");
		}
		int pos = 0;
		if (allowDoubles) {
			double sum = constantDouble;
			for (Object o : input) {
				int mult = multiplicities[pos++];
				if (o == null) {
					return null;
				}
				Number n = (Number) o;
				sum += mult * n.doubleValue();
			}
			return sum;
		} else if (allowLongs) {
			long sum = constantLong;
			for (Object o : input) {
				int mult = multiplicities[pos++];
				if (o == null) {
					return null;
				}
				Number n = (Number) o;
				sum += mult * n.longValue();
			}
			return sum;
		} else {
			int sum = constantInt;
			for (Object o : input) {
				int mult = multiplicities[pos++];
				if (o == null) {
					return null;
				}
				Number n = (Number) o;
				sum += mult * n.intValue();
			}
			return sum;
			
		}
		*/
	}

	@Override
	public Type getOutputType() {
		if (allowDoubles) {
			return new DoubleType(allowNulls, false);
		} else if (allowLongs) {
			return new LongType(allowNulls, false);
		} else {
			return new IntType(allowNulls, false);
		}
	}

	@Override
	public int hashCode() {
		int hashCode = constantInt + 37 * (new Long(constantLong)).hashCode() + 67 * (new Double(constantDouble)).hashCode() + 127 * Arrays.hashCode(multiplicities);
		if (allowDoubles) {
			hashCode += 257;
		}
		if (allowLongs) {
			hashCode += 521;
		}
		return hashCode;
	}

	@Override
	public int getNumInputs() {
		return multiplicities.length;
	}

	@Override
	public boolean isValidForInput(int pos, Type inputType) {
		if (pos >= multiplicities.length) {
			throw new IllegalArgumentException("Function does not have " + (pos + 1) + " inputs");
		}
		if ((! this.allowNulls) && (inputType.isNullable() || inputType.isLabeledNullable())) {
			return false;
		}
		if (inputType instanceof IntType) {
			return true;
		} else if (inputType instanceof DoubleType) {
			return allowDoubles;
		} else if (inputType instanceof LongType) {
			return allowLongs;
		} else {
			return false;
		}
	}

	@Override
	protected void subclassSerialize(Document d, Element e,
			Map<String, QpSchema> schemas) {
		for (int mult : multiplicities) {
			Element input = DomUtils.addChild(d, e, "input");
			input.setAttribute("multiplicity", Integer.toString(mult));
		}
		e.setAttribute("allowDoubles", Boolean.toString(allowDoubles));
		e.setAttribute("allowLongs", Boolean.toString(allowLongs));
		if (hasConstantValue) {
			if (allowDoubles) {
				e.setAttribute("constantValue", Double.toString(constantDouble));
			} else if (allowLongs) {
				e.setAttribute("constantValue", Long.toString(constantLong));
			} else  {
				e.setAttribute("constantValue", Integer.toString(constantInt));
			}
		}
		if (this.allowNulls) {
			e.setAttribute("allowNulls", Boolean.toString(true));
		}
	}

	public static Sum deserialize(Element e, Map<String,QpSchema> schemas) throws XMLParseException {
		try {
			boolean allowDoubles = Boolean.parseBoolean(getAttribute(e, "allowDoubles"));
			boolean allowLongs = Boolean.parseBoolean(getAttribute(e, "allowLongs"));
			boolean allowNulls = Boolean.parseBoolean(e.getAttribute("allowNulls"));
			String constantValueString = e.getAttribute("constantValue");
			Number constantValue = null;
			if (constantValueString.length() > 0) {
				if (allowDoubles) {
					constantValue = Double.parseDouble(constantValueString);
				} else if (allowLongs) {
					constantValue = Long.parseLong(constantValueString);
				} else {
					constantValue = Integer.parseInt(constantValueString);
				}
			}

			List<Element> inputs = DomUtils.getChildElementsByName(e, "input");
			List<Integer> mults = new ArrayList<Integer>(inputs.size());
			for (Element input : inputs) {
				String mult = input.getAttribute("multiplicity");
				if (mult.length() == 0) {
					throw new XMLParseException("Missing multiplicity attribute", input);
				}
				try {
					mults.add(Integer.parseInt(mult));
				} catch (NumberFormatException nfe) {
					throw new XMLParseException(nfe, input);
				}
			}
			
			return new Sum(mults, allowNulls, allowDoubles, allowLongs, constantValue);
		} catch (NumberFormatException nfe) {
			throw new XMLParseException("Invalid number", nfe, e);
		}
	}

}
