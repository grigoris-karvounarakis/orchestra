package edu.upenn.cis.orchestra.evolution;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class Rule {
	protected Atom m_head;
	protected Atom[] m_body;
	protected int m_varcount;
	protected int m_hashCode;
	protected String m_sql;
	protected int[] m_equiv;

	public int hashCode() {
		if (m_hashCode == 0) {
			m_hashCode += m_varcount * 31;
			m_hashCode += m_head.m_attributes.length * 31 * 31;
			for (Atom atom : m_body) {
				m_hashCode += 31 * 31 * 31 * 31 * atom.m_name;
				m_hashCode += 31 * 31 * 31 * 31 * 31 * atom.m_attributes.length;
			}
			m_hashCode *= m_body.length * 17;
			if (m_hashCode == 0) {
				m_hashCode = 1;
			}
		}
		return m_hashCode;
	}

	static private final IntMap s_namemap = new IntMap(4);
	private void sortBody() {
		s_namemap.clear();
		for (Atom atom : m_body) {
			int freq = s_namemap.get(atom.m_name);
			if (freq == -1) {
				freq = 1;
			} else {
				freq++;
			}
			s_namemap.put(atom.m_name, freq);
		}
		Comparator<Atom> compare = new Comparator<Atom>() {
			public int compare(Atom left, Atom right) {
				int equal = s_namemap.get(right.m_name) - s_namemap.get(left.m_name);
				if (equal == 0) {
					equal = right.m_name - left.m_name;
				}
				return equal;
			}
		};
		Arrays.sort(m_body, compare);
	}
	
	private boolean markOccurs(Atom atom) {
		boolean identity = true;
		for (int variable : atom.m_attributes) {
			if (!Atom.isConstant(variable)) {
				int value = s_namemap.get(variable);
				if (value == -1) {
					value = m_varcount++;
					s_namemap.put(variable, value);
					if (variable != value) {
						identity = false;
					}
				}
			}
		}
		return identity;
	}
	
	private void canonicalRename(Map<Integer,Integer> equivs) {
		s_namemap.clear();
		m_varcount = 0;
		boolean identity = markOccurs(m_head);
		for (Atom atom : m_body) {
			identity &= markOccurs(atom);
		}
		assert(m_varcount == s_namemap.getDistinctKeys());
		if (!identity) {
			m_head = m_head.rename(s_namemap.getArray());
			for (int i = 0; i < m_body.length; i++) {
				m_body[i] = m_body[i].rename(s_namemap.getArray());
			}
		}
		m_equiv = new int[m_varcount];
		for (int i = 0; i < m_varcount; i++) {
			m_equiv[i] = i;
		}
		if (equivs != null) {
			for (int key : equivs.keySet()) {
				key = s_namemap.get(key);
				int value = Atom.encodeConstantValue(equivs.get(key));
				m_equiv[key] = value;
			}
		}
	}
	
	public int getVarcount() {
		return m_varcount;
	}
	
	public boolean unsatisfiable() {
		return m_body.length == 0;
	}

	private static final Atom[] NULLARYATOM = new Atom[0];
	
	public Rule(Atom head, Atom... body) {
		m_head = head;
		m_body = body;
		canonicalRename(null);
		sortBody();
	}

	public Rule(Atom head, Vector<Atom> body, Vector<Atom> supps, Map<Integer,Integer> equivs) {
		m_head = head;
		m_body = body.toArray(NULLARYATOM); 
		canonicalRename(equivs);
		sortBody();
	}
	
	public Rule(Atom head, boolean rename, Atom... body) {
		m_head = head;
		m_body = body;
		if (rename) {
			canonicalRename(null);
			sortBody();
		}
	}

	public Rule(Atom head, boolean rename, Vector<Atom> body) {
		m_head = head;
		m_body = body.toArray(NULLARYATOM); 
		if (rename) {
			canonicalRename(null);
			sortBody();
		}
	}

	public Atom getHead() {
		return m_head;
	}
	
	public Atom[] getBody() {
		return m_body;
	}
	
	public Atom getAtom(int index) {
		return m_body[index];
	}
	
	public Rule toBooleanCQ(int name) {
		Atom head = new Atom(name, new int[0]);
		return new Rule(head, false, m_body);
	}

	protected Atom[] appendAtom(Atom[] array, Atom atom) {
		Atom[] appended = new Atom[array.length+1];
		System.arraycopy(array, 0, appended, 0, array.length);
		appended[array.length] = atom;
		return appended;
	}
	
	public Rule addJoin(Atom atom) {
		Atom[] body = appendAtom(m_body, atom);
		return new Rule(m_head, body); 
	}

	public int findNthOccurrence(int name, int n) {
		for (int i = 0; i < m_body.length; i++) {
			if (m_body[i].getName() == name) {
				if (n == 0) {
					return i;
				} else {
					n--;
				}
			}
		}
		return -1;
	}
	
	public Atom getNthOccurrence(int name, int n) {
		int i = findNthOccurrence(name, n);
		if (i == -1) {
			return null;
		} else {
			return m_body[i];
		}
	}

	public RuleMorphism findHomomorphism(Rule target) {
		return RuleMorphism.first(this, target, MorphismType.HOMOMORPHISM);
	}

	public RuleMorphism findIsomorphism(Rule target) {
		return RuleMorphism.first(this, target, MorphismType.ISOMORPHISM);
	}
	
	public RuleMorphism findSubstitution(Rule target) {
		return RuleMorphism.first(this, target, MorphismType.SUBSTITUTION);
	}
	
	@Deprecated
	public Set<Integer> getVariables() {
		// NB careful if you change this to cache 
		// as some callers party on the returned set
		HashSet<Integer> set = new HashSet<Integer>();
		for (int s : m_head.getAttributes()) {
			set.add(s);
		}
		for (Atom a : m_body) {
			for (int s : a.getAttributes()) {
				set.add(s);
			}
		}
		return set;
	}

	@Deprecated
	public Rule rename(HashMap<Integer,Integer> rho) {
		Atom head = m_head.rename(rho);
		Atom[] body = new Atom[m_body.length];
		for (int i = 0; i < body.length; i++) {
			body[i] = m_body[i].rename(rho);
		}
		return new Rule(head, body);
	}
	
	public Rule renameHead(int name) {
		Atom head = new Atom(name, m_head.m_attributes);
		return new Rule(head, m_body);
	}
	
	public Rule foldView(Rule view) {
		// attempt to rewrite part of the query using the view
		// if this is not possible, return query unchanged
		RuleMorphism m = view.findSubstitution(this);
		if (m == null) {
			return this;
		}
		return applySubstitution(m);
	}

	public static Rule applySubstitution(RuleMorphism m) {
		Rule query = m.target;
		Rule view = m.source;
		Atom[] body = new Atom[query.m_body.length - view.m_body.length + 1];
		int index = 0;
		for (int i = 0; i < query.m_body.length; i++) {
			if (!m.hasAtom(i)) {
				body[index++] = query.m_body[i];
			}
		}
		assert(index == body.length-1);
		body[index] = view.m_head.rename(m.varMap);
		return new Rule(query.m_head, body);
	}

	protected int getRepresentative(Equivalence<Integer> equiv, int i) {
		// pick the smallest by value (favors constants over variables,
		// and query variables over view variables)
		int rep = Integer.MAX_VALUE;
		for (int member : equiv.get(i)) {
			if (member < rep) {
				rep = member;
			}
		}
		return rep;
	}

	protected Boolean isConsistent(Equivalence<Integer> equiv) {
		// returns true iff no equivalence class contains two distinct
		// constants
		for (int term : equiv.keySet()) {
			if (Atom.isConstant(term)) {
				for (int member : equiv.get(term)) {
					if (member != term && Atom.isConstant(member)) {
						return false;
					}
				}
			}
		}
		return true;
	}

	public Rule unfoldView(Rule view) {
		// unfolds the first occurrence of view in query
		// if view does not occur in query, returns query unchanged
		Atom vh = view.getHead();
		int index = findNthOccurrence(vh.getName(), 0);
		if (index == -1) {
			return this;
		}
		// figure out if any equivalences are forced by constants or 
		// repeated variables in view head
		Equivalence<Integer> equiv = new Equivalence<Integer>();
		for (int i = 0; i < m_varcount; i++) {
			equiv.addToUniverse(i);
		}
		for (int i = 0; i < view.m_varcount; i++) {
			equiv.addToUniverse(i + m_varcount);
		}
		for (int att : m_body[index].m_attributes) {
			if (Atom.isConstant(att)) {
				equiv.addToUniverse(att);
			}
		}
		for (int att : view.m_head.m_attributes) {
			if (Atom.isConstant(att)) {
				equiv.addToUniverse(att);
			}
		}
		// build equivalence classes
		for (int i = 0; i < m_body[index].m_attributes.length; i++) {
			int t1 = m_body[index].m_attributes[i];
			int t2 = view.m_head.m_attributes[i];
			if (!Atom.isConstant(t2)) {
				t2 += m_varcount;
			}
			equiv.equate(t1, t2);
		}
		// check for consistency (i.e., did we end up equating
		// distinct constants)
		if (!isConsistent(equiv)) {
			// return the unsatisfiable query (empty body)
			return new Rule(new Atom(m_head.getName(),m_head.getArity()), new Atom[0]);
		}

		// create arrays for renamings
		int[] qr = new int[m_varcount];
		int[] vr = new int[view.m_varcount];
		for (int i = 0; i < m_varcount; i++) {
			qr[i] = getRepresentative(equiv, i);
		}
		for (int i = 0; i < view.m_varcount; i++) {
			vr[i] = getRepresentative(equiv, i + m_varcount);
		}
		// perform the unfolding using the renaming arrays
		Atom head = m_head.rename(qr);
		Atom[] body = new Atom[m_body.length+view.m_body.length-1];
		for (int i = 0; i < index; i++) {
			body[i] = m_body[i].rename(qr);
		}
		for (int i = index+1; i < m_body.length; i++) {
			body[i-1] = m_body[i].rename(qr);
		}
		for (int i = 0; i < view.m_body.length; i++) {
			body[i+m_body.length-1] = view.m_body[i].rename(vr);
		}
		return new Rule(head,body);
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append(m_head.toString());
		buf.append(" :- ");
		if (unsatisfiable()) {
			buf.append("false");
		} else {
			buf.append(Utils.atos(m_body));
		}
		boolean first = true;
		for (int i = 0; i < m_varcount; i++) {
			if (Atom.isConstant(m_equiv[i])) {
				if (first) {
					buf.append("; ");
					first = false;
				} else {
					buf.append(", ");
				}
				buf.append(i);
				buf.append("~");
				buf.append("'");
				buf.append(Atom.decodeConstantValue(m_equiv[i]));
				buf.append("'");
			}
		}
		return buf.toString();
	}
	
	static public Rule parse(String str) {
		Tokenizer tok = new Tokenizer();
		str = str.replaceAll("[ \\t]+", "");
		int colon = str.indexOf(":-");
		String first = str.substring(0, colon);
		Atom head = Atom.parse(first, tok);
		String[] parts = str.substring(colon+2).split(";");
		assert parts.length == 1 || parts.length == 2;
		String[] rest = parts[0].split("\\),");
		Vector<Atom> body = new Vector<Atom>();
		Vector<Atom> supps = new Vector<Atom>();
		for (int i = 0; i < rest.length; i++) {
			if (rest[i].compareToIgnoreCase("false") == 0) {
				// the unsatisfiable query
				body.clear();
				break;
			}
			boolean supp = rest[i].startsWith("supp");
			if (supp) {
				rest[i] = rest[i].substring(4);
			}
			Atom a;
			if (i < rest.length-1) {
				a = Atom.parse(rest[i]+")", tok);
			} else {
				a = Atom.parse(rest[i], tok);
			}
			if (supp) {
				supps.add(a);
			} else {
				body.add(a);
			}
		}
		HashMap<Integer,Integer> map = null;
		if (parts.length == 2) {
			// parse the equivalences
			map = new HashMap<Integer,Integer>();
			String[] equivs = parts[1].split(",");
			for (String eq : equivs) {
				int twiddle = eq.indexOf("~");
				int key = tok.getInteger(eq.substring(0, twiddle));
				assert eq.charAt(twiddle+1) == '\'' && eq.charAt(eq.length()-1) == '\'';
				String number = eq.substring(twiddle+2,eq.length()-1);
				int value = Integer.parseInt(number);
				map.put(key, value);
			}
		}
		return new Rule(head, body, supps, map);
	}
	
	protected HashMap<Integer, Vector<Pair<Integer,Integer>>> getOccurrences(Atom[] array) {
		HashMap<Integer, Vector<Pair<Integer,Integer>>> map = new HashMap<Integer, Vector<Pair<Integer,Integer>>>();
		for (Integer s : getVariables()) {
			map.put(s, new Vector<Pair<Integer,Integer>>());
		}
		for (int i = 0; i < array.length; i++) {
			int[] a = array[i].getAttributes();
			for (int j = 0; j < a.length; j++) {
				map.get(a[j]).add(new Pair<Integer,Integer>(i,j));
			}
		}
		return map;
	}
	
	protected void appendVariable(StringBuffer buf, Pair<Integer,Integer> p) {
		buf.append("R");
		buf.append(p.first);
		buf.append(".A");
		buf.append(p.second);
	}
	
	public String toSQL() {
		return toSQL(true);
	}
	
	public String toSQL(boolean positive) {
		if (m_sql != null) {
			return m_sql;
		}
		HashMap<Integer, Vector<Pair<Integer,Integer>>> bodyMap = getOccurrences(m_body);
		StringBuffer buf = new StringBuffer();
		buf.append("select ");
		int[] a = m_head.getAttributes();
		assert(a.length > 0); 
		if (unsatisfiable()) {
			// the unsatisfiable query
			for (int i = 0; i < a.length; i++) {
				if (i > 0) {
					buf.append(", ");
				}
				buf.append("0 as A");
				buf.append(i);
			}
			buf.append(", 0 as count");
			buf.append("\nfrom EXPLAIN_STATEMENT");	// just some table that's guaranteed to exist
			buf.append("\nwhere 0 = 1");
			return buf.toString();
		}
		for (int i = 0; i < a.length; i++) {
			if (i > 0) {
				buf.append(", ");
			}
			if (Atom.isConstant(a[i])) {
				buf.append(Atom.decodeConstantValue(a[i]));
			} else {
				appendVariable(buf, bodyMap.get(a[i]).get(0));
			}
			buf.append(" as A");
			buf.append(i);
		}
		buf.append(", ");
		if (!positive) {
			buf.append("-1*");
		}
		for (int i = 0; i < m_body.length; i++) {
			if (i > 0) {
				buf.append("*");
			}
			buf.append("R");
			buf.append(i);
			buf.append(".count");
		}
		buf.append(" as count");
		buf.append("\n from ");
		for (int i = 0; i < m_body.length; i++) {
			if (i > 0) {
				buf.append(", ");
			}
			buf.append(Utils.TOKENIZER.getString(m_body[i].getName()));
			buf.append(" as R");
			buf.append(i);
		}
		buf.append("\n where ");
		boolean first = true;
		for (int s : getVariables()) {
			Vector<Pair<Integer,Integer>> v = bodyMap.get(s);
			if (Atom.isConstant(s)) {
				for (int i = 0; i < v.size(); i++) {
					if (first) {
						first = false;
					} else {
						buf.append(" and ");
					}
					appendVariable(buf, v.get(i));
					buf.append(" = ");
					buf.append(Atom.decodeConstantValue(s));
				}
			} else for (int i = 1; i < v.size(); i++) {
				if (first) {
					first = false;
				} else {
					buf.append(" and ");
				}
				appendVariable(buf, v.get(i-1));
				buf.append(" = ");
				appendVariable(buf, v.get(i));
			}
		}
		if (first) {
			buf.setLength(buf.length() - "\n where ".length());
		}
		m_sql = buf.toString();
		return m_sql;
	}
}
