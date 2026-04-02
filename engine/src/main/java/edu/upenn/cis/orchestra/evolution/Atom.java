package edu.upenn.cis.orchestra.evolution;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

public class Atom {
	protected int m_name;
	protected int[] m_attributes;
	protected HashSet<Integer> m_variables;

	public static boolean isConstant(int attribute) {
		return attribute < 0;
	}
	
	public static int decodeConstantValue(int value) {
		assert isConstant(value);
		return -value;
	}
	
	public static int encodeConstantValue(int value) {
		assert !isConstant(value);
		return -value;
	}
	
	public Atom(int name, int... attributes) {
		m_name = name;
		m_attributes = attributes;
		m_variables = null;
	}

	@Deprecated
	public Atom(int name, Vector<Integer> attributes) {
		m_name = name;
		int len = attributes.size();
		m_attributes = new int[len];
		for (int i = 0; i < len; i++) {
			m_attributes[i] = attributes.get(i);
		}
		m_variables = null;
	}
	
	public Atom(int name, int arity) {
		m_name = name;
		m_attributes = new int[arity];
		for (int i = 0; i < arity; i++) {
			m_attributes[i] = i;
		}
	}
	
	@Deprecated
	public Atom rename(HashMap<Integer,Integer> renaming) {
		// if renaming is the identity on variables of this
		// atom, just return this atom
		boolean identity = true;
		for (Integer att : m_attributes) {
			if (!isConstant(att) && !renaming.get(att).equals(att)) {
				identity = false;
				break;
			}
		}
		if (identity) {
			return this;
		}
		int[] attributes = new int[m_attributes.length];
		for (int i = 0; i < m_attributes.length; i++) {
			if (isConstant(m_attributes[i])) {
				attributes[i] = m_attributes[i];
			} else {
				attributes[i] = renaming.get(m_attributes[i]);
			}
		}
		return new Atom(m_name, attributes);
	}

	public Atom rename(int[] renaming) {
		// if renaming is the identity on variables of this
		// atom, just return this atom
		boolean identity = true;
		for (Integer att : m_attributes) {
			if (!isConstant(att) && renaming[att] != att) {
				identity = false;
				break;
			}
		}
		if (identity) {
			return this;
		}
		int[] attributes = new int[m_attributes.length];
		for (int i = 0; i < m_attributes.length; i++) {
			if (isConstant(m_attributes[i])) {
				attributes[i] = m_attributes[i];
			} else {
				attributes[i] = renaming[m_attributes[i]];
			}
		}
		return new Atom(m_name, attributes);
	}
	
	public int getName() {
		return m_name;
	}
	
	public int[] getAttributes() {
		return m_attributes;
	}
	
	public int getAttribute(int index) {
		return m_attributes[index];
	}
	
	public int getArity() {
		return m_attributes.length;
	}
	
	@Deprecated
	public Set<Integer> getVariables() {
		if (m_variables == null) {
			m_variables = new HashSet<Integer>();
			for (int s : m_attributes) {
				m_variables.add(s);
			}
		}
		return m_variables;
	}
	
	public boolean equals(Atom atom) {
		if (m_name != atom.m_name) {
			return false;
		}
		assert(getArity() == atom.getArity());
		for (int i = 0; i < getArity(); i++) {
			if (m_attributes[i] != atom.m_attributes[i]) {
				return false;
			}
		}
		return true;
	}

	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append(Utils.TOKENIZER.getString(m_name));
		buf.append("(");
		for (int i = 0; i < m_attributes.length; i++) {
			if (i > 0) {
				buf.append(",");
			}
			if (isConstant(m_attributes[i])) {
				buf.append("'");
				buf.append(-m_attributes[i]);
				buf.append("'");
			} else {
				buf.append(m_attributes[i]);
			}
		}
		buf.append(")");
		return buf.toString();
//		return Utils.TOKENIZER.getString(m_name) + "(" + Utils.atos(m_attributes, ",") + ")";
	}
	
	static public Atom parse(String str, Tokenizer tok) {
		int open = str.indexOf("(");
		int close = str.indexOf(")");
		String name = str.substring(0, open);
		String rest = str.substring(open+1, close);
		int n = Utils.TOKENIZER.getInteger(name);
		String[] attributes = Utils.split(rest, ",");
		int[] ints = new int[attributes.length];
		for (int i = 0; i < attributes.length; i++) {
			if (attributes[i].charAt(0) == '\'') {
				// a constant
				assert attributes[i].charAt(attributes[i].length()-1) == '\'';
				ints[i] = -Integer.parseInt(attributes[i].substring(1,attributes[i].length()-1));
			} else {
				// a variable
				ints[i] = tok.getInteger(attributes[i]);
			}
		}
		return new Atom(n, ints);
	}
}
