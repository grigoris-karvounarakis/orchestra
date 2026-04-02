package edu.upenn.cis.orchestra.p2pqp;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractImmutableTuple;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.util.XMLParseException;

public abstract class Function {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	abstract public Type getOutputType();
	
	public abstract int getNumInputs();
	
	public abstract boolean isValidForInput(int pos, Type inputType);
	
	/**
	 * Evaluate this function
	 * 
	 * @param input						A tuple from which to take inputs
	 * @param inputFields				An array of serialized inputs
	 * @param inputCols					The indices in input and inputFields of the inputs to this function,
	 * 									indices into inputFields have one added to them and are then negated. A value of Integer.MIN_VALUE means a null input
	 * @return							The result of evaluating the function
	 * @throws ValueMismatchException
	 * @throws EvaluateException
	 */
	public abstract byte[] evaluateFunction(AbstractImmutableTuple<?> input, byte[][] inputFields, int[] inputCols) throws ValueMismatchException, EvaluateException;
	
	public static class IllegalArgumentType extends Exception {
		private static final long serialVersionUID = 1L;
		Type t;
		public IllegalArgumentType(Type t, String fName) {
			super("Type " + t + " is not a valid input to function " + fName);
			this.t = t;
		}
	}
	
	public static class EvaluateException extends Exception {
		private static final long serialVersionUID = 1L;
		public EvaluateException(String what) {
			super(what);
		}
		public EvaluateException(String what, Throwable why) {
			super(what,why);
		}
	}
	
	abstract public boolean equals(Object o);
	
	abstract public int hashCode();
	
	/**
	 * Load schemas after being serialized and deserialized.
	 * 
	 * @param schemas
	 */
	public void loadSchemas(Source schemas) {
		
	}
	
	public Element serialize(Document d, Map<String,QpSchema> schemas) {
		String tagName = findTag.get(this.getClass());
		if (tagName == null) {
			throw new IllegalStateException("Cannot find tag for class " + this.getClass().getName());
		}
		Element e = d.createElement(tagName);
		subclassSerialize(d,e,schemas);
		return e;
	}
	
	protected abstract void subclassSerialize(Document d, Element e, Map<String,QpSchema> schemas);
	
	public static Function deserialize(Element el, Map<String,QpSchema> schemas) throws XMLParseException {
		Class<? extends Function> c = findClass.get(el.getTagName());
		if (c == null) {
			throw new XMLParseException("Cannot find class to deserialize query plan node", el);
		}
		try {
			Method m = c.getDeclaredMethod("deserialize", Element.class, Map.class);
			Object o = m.invoke(null, el, schemas);
			return (Function) o;
		} catch (NoSuchMethodException e) {
			throw new XMLParseException("Could not find deserialize method for class " + c.getName(), el);
		} catch (IllegalAccessException e) {
			throw new XMLParseException("Error invoking deserialize method on class " + c.getName(), el);
		} catch (InvocationTargetException e) {
			if (e.getCause() instanceof XMLParseException) {
				throw (XMLParseException) e.getCause();
			} else {
				throw new XMLParseException("Error invoking deserialize method on class " + c.getName(), e.getCause());
			}
		}
	}
	
	private final static Map<Class<? extends Function>,String> findTag = new HashMap<Class<? extends Function>,String>();
	private final static Map<String,Class<? extends Function>> findClass = new HashMap<String,Class<? extends Function>>();

	private static void addClass(String tag, Class<? extends Function> c) {
		findTag.put(c, tag);
		findClass.put(tag, c);
	}
	
	static {
		addClass("sum", Sum.class);
		addClass("concatenate", Concatenate.class);
		addClass("maxIntForPred", MaxIntForPredicate.class);
		addClass("product", Product.class);
		addClass("year",Year.class);
	}
	
	protected static String getAttribute(Element el, String attName) throws XMLParseException {
		if (! el.hasAttribute(attName)) {
			throw new XMLParseException("Missing " + attName + " attribute", el);
		}
		String attVal = el.getAttribute(attName);
		return attVal;
	}


}
