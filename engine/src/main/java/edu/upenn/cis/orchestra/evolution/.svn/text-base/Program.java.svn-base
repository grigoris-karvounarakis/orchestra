package edu.upenn.cis.orchestra.evolution;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

public class Program {
	protected Union[] m_views;
	protected Schema m_schema;
	
	private void computeSchema() {
		HashMap<Integer,Integer> map = new HashMap<Integer,Integer>();
		for (Union view : m_views) {
			for (SignedRule rule : view.getRules()) {
				Rule r = rule.getRule();
				Atom head = r.getHead();
				map.put(head.getName(), head.getArity());
				for (Atom atom : r.getBody()) {
					map.put(atom.getName(), atom.getArity());
				}
			}
		}
		m_schema = new Schema(map);
	}
	
	public Schema getSources() {
		HashMap<Integer,Integer> relations = getSchema().getArities();
		HashMap<Integer,Integer> sources = new HashMap<Integer,Integer>();
		for (int key : relations.keySet()) {
			if (getView(Utils.TOKENIZER.getString(key)) == null) {
				sources.put(key, relations.get(key));
			}
		}
		return new Schema(sources);
	}
	
	public Schema getDerived() {
		HashMap<Integer,Integer> relations = getSchema().getArities();
		HashMap<Integer,Integer> views = new HashMap<Integer,Integer>();
		for (int key : relations.keySet()) {
			if (getView(Utils.TOKENIZER.getString(key)) != null) {
				views.put(key, relations.get(key));
			}
		}
		return new Schema(views);
	}
	
	public Schema getSchema() {
		if (m_schema == null) {
			computeSchema();
		}
		return m_schema;
	}
	
	public Program(Union... views) {
		m_views = views;
		sortViews();
	}
	
	static final private Union[] NULLARYUNION = new Union[0];
	
	public Program(Vector<Union> views) {
		m_views = views.toArray(NULLARYUNION);
		sortViews();
	}
	
	public Program(Rule... rules) {
		SignedRule[] sr = new SignedRule[rules.length];
		for (int i = 0; i < rules.length; i++) {
			sr[i] = new SignedRule(rules[i], true);
		}
		computeViews(sr); 
	}
	
	public Program deleteView(int index) {
		Union[] views = new Union[m_views.length-1];
		System.arraycopy(m_views, 0, views, 0, index);
		System.arraycopy(m_views, index+1, views, index, m_views.length-index-1);
		return new Program(views);
	}

	public Program addViews(Union... views) {
		Union[] total = new Union[m_views.length+views.length];
		System.arraycopy(m_views, 0, total, 0, m_views.length);
		System.arraycopy(views, 0, total, m_views.length, views.length);
		return new Program(total);
	}

	public Program addViews(Vector<Union> views) {
		return addViews(views.toArray(new Union[0]));
	}

	protected void computeViews(SignedRule[] rules) {
		HashMap<Integer,Vector<SignedRule>> map = new HashMap<Integer,Vector<SignedRule>>();
		for (SignedRule r : rules) {
			int name = r.getRule().getHead().getName();
			if (!map.containsKey(name)) {
				map.put(name, new Vector<SignedRule>());
			}
			map.get(name).add(r);
		}
		m_views = new Union[map.size()];
		int i = 0;
		for (Vector<SignedRule> vr : map.values()) {
			m_views[i++] = new Union(vr);
		}
		sortViews();
	}
	
	protected Object[] getChunks(String s1) {
		// count the chunks
		if (s1.length() == 0) {
			return new Object[0];
		}
		int count = 1;
		boolean wasdigit = false;
		for (int i = 0; i < s1.length(); i++) {
			boolean isdigit = Character.isDigit(s1.charAt(i));
			if (i > 0 && wasdigit != isdigit) {
				count++;
			}
			wasdigit = isdigit;
		}
		Object[] chunks = new Object[count];
		count = 0;
		int start = 0;
		for (int i = 0; i < s1.length(); i++) {
			boolean isdigit = Character.isDigit(s1.charAt(i));
			if (i > 0 && wasdigit != isdigit) {
				String substring = s1.substring(start,i);
				if (wasdigit) {
					chunks[count] = Integer.parseInt(substring);
				} else {
					chunks[count] = substring;
				}
				count++;
				start = i;
			}
			wasdigit = isdigit;
		}
		if (start < s1.length()) {
			String substring = s1.substring(start);
			if (wasdigit) {
				chunks[count] = Integer.parseInt(substring);
			} else {
				chunks[count] = substring;
			}
			count++;
		}
		assert count == chunks.length;
		return chunks;
	}
	
	protected int compareAlphaNumeric(String s1, String s2) {
		if (s1.compareTo(s2) == 0) {
			return 0;
		}
		Object[] o1 = getChunks(s1);
		Object[] o2 = getChunks(s2);
		for (int i = 0; i < o1.length && i < o2.length; i++) {
			if (o1[i] instanceof String) {
				if (o2[i] instanceof String) {
					int cmp = ((String)o1[i]).compareTo((String)o2[i]);
					if (cmp != 0) {
						return cmp;
					}
				} else {
					return 1;
				}
			} else if (o2[i] instanceof String) {
				return -1;
			} else if ((Integer)o1[i] < (Integer)o2[i]) {
				return -1;
			} else if ((Integer)o1[i] > (Integer)o2[i]) {
				return 1;
			}
		}
		assert o1.length != o2.length;
		return o1.length < o2.length ? -1 : 1;
	}

	protected void sortViews() {
		Comparator<Union> compare = new Comparator<Union>() {
			public int compare(Union left, Union right) {
				String s1 = Utils.TOKENIZER.getString(left.getName());
				String s2 = Utils.TOKENIZER.getString(right.getName());
				return compareAlphaNumeric(s1,s2);
			}
		};
		Arrays.sort(m_views, compare);
	}
	
	public Program(SignedRule... rules) {
		computeViews(rules);
	}
	
	public Union[] getViews() {
		return m_views;
	}
	
	public Union getView(int index) {
		return m_views[index];
	}
	
	public String toString() {
		return Utils.atos(m_views, "\n");
	}
	
	public Union getView(String name) {
		for (Union r : m_views) {
			if (name.compareTo(Utils.TOKENIZER.getString(r.getName())) == 0) {
				return r;
			}
		}
		return null;
	}
	
	public Union unfoldQuery(Union query) {
		boolean done = false;
		while (!done) {
			Union last = query;
			for (Union view : m_views) {
				query = query.unfoldView(view);
			}
			done = (last == query);
		}
		return query;
	}
	
	public Program unfoldAll() {
		Union[] rules = new Union[m_views.length];
		for (int i = 0; i < m_views.length; i++) {
			rules[i] = unfoldQuery(m_views[i]);
		}
		return new Program(rules);
	}
	
	public boolean testEquivalent(Union u1, Union u2) {
		u1 = unfoldQuery(u1);
		u2 = unfoldQuery(u2);
		return u1.findIsomorphism(u2) != null;
	}
	
	public boolean testEquivalent(Rule r1, Rule r2) {
		return testEquivalent(new Union(r1), new Union(r2));
	}

	public boolean testEquivalent(SignedRule r1, SignedRule r2) {
		return testEquivalent(new Union(r1), new Union(r2));
	}
	
	static public Program parse(String str) {
		str = str.replaceAll("[ \\t]+", "");
		String[] lines = str.split("\n");
		HashMap<Integer,Vector<SignedRule>> map = new HashMap<Integer,Vector<SignedRule>>();
		for (String line : lines) {
			if (line.length() > 0 && !line.startsWith("#")) {
				SignedRule rule = SignedRule.parse(line);
				int name = rule.getRule().getHead().getName();
				if (!map.containsKey(name)) {
					map.put(name, new Vector<SignedRule>());
				}
				map.get(name).add(rule);
			}
		}
		Set<Integer> keys = map.keySet();
		Union[] unions = new Union[keys.size()];
		int i = 0;
		for (int name : keys) {
			unions[i++] = new Union(map.get(name));
		}
		return new Program(unions);
	}
}
