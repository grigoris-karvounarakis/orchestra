package edu.upenn.cis.orchestra.evolution;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;


public class Utils {
	static public String atos(Object[] a) {
		return atos(a, ", ");
	}

	static public String atos(Object[] a, String sep) {
		if (a.length == 0) {
			return "";
		} else if (a.length == 1) {
			return a[0].toString();
		} else {
			StringBuffer buf = new StringBuffer(a[0].toString());
			for (int i = 1; i < a.length; i++) {
				buf.append(sep);
				buf.append(a[i].toString());
			}
			return buf.toString();
		}
	}
	
	static public String atos(int[] a, String sep) {
		if (a.length == 0) {
			return "";
		} else if (a.length == 1) {
			return Integer.toString(a[0]);
		} else {
			StringBuffer buf = new StringBuffer(Integer.toString(a[0]));
			for (int i = 1; i < a.length; i++) {
				buf.append(sep);
				buf.append(Integer.toString(a[i]));
			}
			return buf.toString();
		}
	}
	
	static public String[] split(String str, String regex) {
		// a version of split that returns a zero-length
		// array when the input string is zero-length
		if (str.length() == 0) {
			return new String[0];
		} else {
			return str.split(regex);
		}
	}
	
	static public boolean contains(Object[] array, Object item) {
		for (Object obj : array) {
			if (obj.equals(item)) {
				return true;
			}
		}
		return false;
	}

	static public boolean contains(int[] array, int item) {
		for (int obj : array) {
			if (obj == item) {
				return true;
			}
		}
		return false;
	}
	
	static public Tokenizer TOKENIZER = new Tokenizer();
	
	static public HashMap<String,String> readBlocks(BufferedReader reader) throws IOException {
		HashMap<String,String> blocks = new HashMap<String,String>();
		String name = null;
		StringBuffer buf = new StringBuffer();
		String line;
		boolean first = true;
		while ((line = nextLine(reader)) != null) {
			if (line.startsWith(":")) {
				// block name line, add current block to map (if we have one) 
				if (name != null) {
					blocks.put(name, buf.toString());
					buf.setLength(0);
				}
				// remember name of new block
				name = line.substring(1).trim();
				// if block is ENDTEST, we're done
				if (name.compareTo("ENDTEST") == 0) {
					return blocks;
				}
				first = true;
			} else if (line.trim().isEmpty()) {
				continue;
			} else {
				if (first) {
					first = false;
				} else {
					buf.append('\n');
				}
				buf.append(line);
			}
		}
		assert(false);	// OOPS, hit end of file in the middle of a block
		return null;
	}

	static public String nextLine(BufferedReader reader) throws IOException {
		// skip comment lines
		String line;
		while ((line = reader.readLine()) != null) {
			if (!line.startsWith("#")) {
				return line;
			}
		}
		return null;
	}
	
	static public boolean skipBlanks(BufferedReader reader) throws IOException {
		reader.mark(1028);
		String line;
		while ((line = nextLine(reader)) != null) {
			if (line.trim().isEmpty()) {
				reader.mark(1028);
			} else {
				reader.reset();
				return true;
			}
		}
		return false;
	}
}
