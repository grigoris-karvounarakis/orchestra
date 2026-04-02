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

public class Product extends Function {
	private static final long serialVersionUID = 1L;

	private final boolean allowDoubles, allowLongs;
	private final boolean hasConstantValue;
	private final int constantInt;
	private final double constantDouble;
	private final long constantLong;
	private final int[] powers;
	private final boolean allowNulls;

	public Product(List<Integer> powers, boolean allowNulls, boolean allowDoubles, boolean allowLongs) {
		this(powers, allowNulls, allowDoubles, allowLongs, null);
	}

	public Product(List<Integer> powers, boolean allowNulls, boolean allowDoubles, boolean allowLongs, Number constantValue) {
		this.allowNulls = allowNulls;
		this.powers = new int[powers.size()];
		int pos = 0;
		for (Integer power : powers) {
			this.powers[pos++] = power;
		}
		this.allowDoubles = allowDoubles;
		this.allowLongs = allowLongs;

		if (constantValue == null) {
			constantDouble = 1.0;
			constantLong = 1L;
			constantInt = 1;
			hasConstantValue = false;
		} else if (constantValue instanceof Integer) {
			if (allowDoubles) {
				constantInt = 1;
				constantLong = 1L;
				constantDouble = (Integer) constantValue;
			} else if (allowLongs) {
				constantInt = 1;
				constantDouble = 1.0;
				constantLong = (Integer) constantValue;
			} else {
				constantDouble = 1.0;
				constantLong = 1L;
				constantInt = (Integer) constantValue;
			}
			hasConstantValue = true;
		} else if (constantValue instanceof Double) {
			if (! allowDoubles) {
				throw new IllegalArgumentException("Cannot specify a double constant value if doubles are not allowed");
			}
			constantInt = 1;
			constantLong = 1L;
			constantDouble = (Double) constantValue;
			hasConstantValue = true;
		} else if (constantValue instanceof Long) {
			if (! allowLongs) {
				throw new IllegalArgumentException("Cannot specify a long constant value if longs are not allowed");
			}
			if (allowDoubles) {
				constantInt = 1;
				constantLong = 1L;
				constantDouble = (Long) constantValue;
			} else {
				constantInt = 1;
				constantDouble = 1.0;
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
		Product p = (Product) o;
		return (allowNulls == p.allowNulls && allowDoubles == p.allowDoubles && allowLongs == p.allowLongs && 
				constantInt == p.constantInt && constantLong == p.constantLong && constantDouble == p.constantDouble &&
				Arrays.equals(powers, p.powers));
	}

	@Override
	public byte[] evaluateFunction(AbstractImmutableTuple<?> input, byte[][] inputFields, int[] inputCols)
			throws ValueMismatchException, EvaluateException {
		// TODO: reimplement product
		throw new UnsupportedOperationException("Need to reimplement");
		/*
		if (input.length != powers.length) {
			throw new EvaluateException("Wrong number of inputs");
		}
		int pos = 0;
		if (allowDoubles) {
			double product = constantDouble;
			for (Object o : input) {
				int power = powers[pos++];
				if (o == null) {
					if (this.allowNulls) {
						return null;
					} else {
						throw new ValueMismatchException();
					}
				}
				double val = ((Number) o).doubleValue();
				if (power > 0) {
					for (int i = 0; i < power; ++i) {
						product *= val;
					}
				} else if (power < 0) {
					for (int i = 0; i > power; --i) {
						product /= val;
					}
				}
			}
			return product;
		} else if (allowLongs) {
			long product = constantLong;
			for (Object o : input) {
				int power = powers[pos++];
				if (o == null) {
					if (this.allowNulls) {
						return null;
					} else {
						throw new ValueMismatchException();
					}
				}
				long val = ((Number) o).longValue();
				if (power > 0) {
					for (int i = 0; i < power; ++i) {
						product *= val;
					}
				} else if (power < 0) {
					for (int i = 0; i > power; --i) {
						product /= val;
					}
				}
			}
			return product;
		} else {
			int product = constantInt;
			for (Object o : input) {
				int power = powers[pos++];
				if (o == null) {
					if (this.allowNulls) {
						return null;
					} else {
						throw new ValueMismatchException();
					}
				}
				int val = ((Number) o).intValue();
				if (power > 0) {
					for (int i = 0; i < power; ++i) {
						product *= val;
					}
				} else if (power < 0) {
					for (int i = 0; i > power; --i) {
						product /= val;
					}
				}
			}
			return product;
			
		}
		*/
	}

	@Override
	public int getNumInputs() {
		return powers.length;
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
		int hashCode = constantInt + 37 * (new Long(constantLong)).hashCode() + 67 * (new Double(constantDouble)).hashCode() + 127 * Arrays.hashCode(powers);
		if (allowDoubles) {
			hashCode += 257;
		}
		if (allowLongs) {
			hashCode += 521;
		}
		return hashCode;
	}

	@Override
	public boolean isValidForInput(int pos, Type inputType) {
		if (pos >= powers.length) {
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
		for (int power : powers) {
			Element input = DomUtils.addChild(d, e, "input");
			input.setAttribute("power", Integer.toString(power));
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

	public static Product deserialize(Element e, Map<String,QpSchema> schemas) throws XMLParseException {
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
			List<Integer> powers = new ArrayList<Integer>(inputs.size());
			for (Element input : inputs) {
				String power = input.getAttribute("power");
				if (power.length() == 0) {
					throw new XMLParseException("Missing power attribute", input);
				}
				try {
					powers.add(Integer.parseInt(power));
				} catch (NumberFormatException nfe) {
					throw new XMLParseException(nfe, input);
				}
			}
			
			return new Product(powers, allowNulls, allowDoubles, allowLongs, constantValue);
		} catch (NumberFormatException nfe) {
			throw new XMLParseException("Invalid number", nfe, e);
		}
	}

}
