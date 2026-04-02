package edu.upenn.cis.orchestra.evolution;

import java.util.HashMap;

public class Schema {
	protected HashMap<Integer,Integer> m_arities;
	protected int[] m_relations;
	
	public Schema match(String prefix, String suffix) {
		HashMap<Integer,Integer> map = new HashMap<Integer,Integer>();
		for (int i : m_arities.keySet()) {
			String key = Utils.TOKENIZER.getString(i);
			if (key.startsWith(prefix) && key.endsWith(suffix)) {
				map.put(i, m_arities.get(i));
			}
		}
		return new Schema(map);
	}
	
	public Schema(HashMap<Integer,Integer> arities) {
		m_arities = arities;
		m_relations = new int[arities.size()];
		int pos = 0;
		for (int relation : arities.keySet()) {
			m_relations[pos++] = relation;
		}
	}
	
	public HashMap<Integer,Integer> getArities() {
		return m_arities;
	}
	
	public int getArity(String relation) {
		return m_arities.get(relation);
	}
	
	public int getSize() {
		return m_relations.length;
	}
	
	public int getRelation(int index) {
		return m_relations[index];
	}
	
	public String toString() {
		if (m_arities.isEmpty()) {
			return "";
		} else {
			StringBuffer buf = new StringBuffer();
			for (int key : m_arities.keySet()) {
				buf.append(Utils.TOKENIZER.getString(key));
				buf.append("(");
				buf.append(m_arities.get(key));
				buf.append("), ");
			}
			buf.setLength(buf.length()-2);
			return buf.toString();
		}
	}
	
	static public Schema parse(String str) {
		HashMap<Integer,Integer> relations = new HashMap<Integer,Integer>();
		str = str.replaceAll("[ \t]+", "");
		if (str.length() != 0) {
			for (String relation : str.split(",")) {
				int open = relation.indexOf("(");
				int close = relation.indexOf(")");
				assert(close == relation.length()-1);
				String name = relation.substring(0,open);
				String arity = relation.substring(open+1,close);
				relations.put(Utils.TOKENIZER.getInteger(name), Integer.parseInt(arity));
			}
		}
		return new Schema(relations);
	}
}
