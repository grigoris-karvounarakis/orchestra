package edu.upenn.cis.orchestra.evolution;

import java.util.HashMap;
import java.util.Vector;

public class Tokenizer {
	private HashMap<String,Integer> m_str2int;
	private Vector<String> m_int2str;
	
	public Tokenizer() {
		m_str2int = new HashMap<String,Integer>();
		m_int2str = new Vector<String>();
	}
	
	public Tokenizer(Tokenizer old) {
		m_str2int = new HashMap<String,Integer>(old.m_str2int);
		m_int2str = new Vector<String>(old.m_int2str);
	}
	
	public int getInteger(String str) {
		Integer value = m_str2int.get(str);
		if (value == null) {
			value = m_int2str.size();
			m_str2int.put(str, value);
			m_int2str.add(str);
		}
		return value;
	}
	
	public String getString(int value) {
		return m_int2str.get(value);
	}
}
