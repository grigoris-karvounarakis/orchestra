package edu.upenn.cis.orchestra.evolution;

import java.util.Arrays;
import java.util.Vector;

public class Union {

	protected SignedRule [] m_rules;
	protected int m_name;
	protected int m_arity;
	protected int m_hashCode;

	public Union reverse() {
		// Useful for performance tests
		SignedRule[] rules = new SignedRule[m_rules.length];
		for (int i = 0; i < m_rules.length; i++) {
			rules[i] = m_rules[m_rules.length-i-1];
		}
		return new Union(m_name, m_arity, rules);
	}
	
	public Union subunion(int start, int count) {
		if (start == 0 && count == m_rules.length) {
			return this;
		}
		SignedRule[] rules = new SignedRule[count];
		System.arraycopy(m_rules, start, rules, 0, count);
		return new Union(rules);
	}
	
	public Union(int name, int arity, Rule... rules) {
		SignedRule[] sr = new SignedRule[rules.length];
		for (int i = 0; i < rules.length; i++) {
			sr[i] = new SignedRule(rules[i], true);
		}
		initialize(name, arity, sr);
	}

	public Union(Rule... rules) {
		this(rules[0].getHead().getName(), rules[0].getHead().getArity(), rules);
	}

	public int hashCode() {
		if (m_hashCode == 0) {
			m_hashCode = m_arity * 31 + m_rules.length * 1327;
			for (SignedRule rule : m_rules) {
				m_hashCode += rule.isPositive() ? 0 : 1051;
				m_hashCode += rule.getRule().hashCode();				
			}
			if (m_hashCode == 0) {
				m_hashCode = 1;
			}
		}
		return m_hashCode;
	}
	
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj instanceof Union) {
			return findIsomorphism((Union)obj) != null;
		}
		return false;
	}
	
	protected void initialize(int name, int arity, SignedRule[] rules) {
		m_name = name;
		m_arity = arity;
		int count = 0;
		for (SignedRule rule : rules) {
			if (!rule.getRule().unsatisfiable()) {
				count++;
			}
		}
		if (count == rules.length) {
			m_rules = rules;
		} else {
			SignedRule[] rules2 = new SignedRule[count];
			for (SignedRule rule : rules) {
				if (!rule.getRule().unsatisfiable()) {
					rules2[--count] = rule;
				}
			}
			m_rules = rules2;
		}
	}
	
	public Union(int name, int arity, SignedRule... rules) {
		initialize(name, arity, rules);
	}

	public Union(SignedRule... rules) {
		this(rules[0].getRule().getHead().getName(), rules[0].getRule().getHead().getArity(), rules);
	}

	public Union(int name, int arity, Vector<SignedRule> rules) {
		this(name, arity, rules.toArray(new SignedRule[0]));
	}
	
	public Union(Vector<SignedRule> rules) {
		this(rules.get(0).getRule().getHead().getName(), 
				rules.get(0).getRule().getHead().getArity(), rules);
	}

	public int getName() {
		return m_name;
	}
	
	public int getArity() {
		return m_arity;
	}
	
	public SignedRule[] getRules() {
		return m_rules;
	}
	
	public String toString() {
		if (m_rules.length == 0) {
			Atom head = new Atom(m_name, m_arity);
			return head.toString() + " :- false";
		}
		return Utils.atos(m_rules, "\n");
	}
	
	public String toSQL() {
		StringBuffer buf = new StringBuffer();
		if (m_rules.length == 0) {
			Atom a = new Atom(m_name, m_arity);
			Rule r = new Rule(a);
			buf.append(r.toSQL());
		} else for (int i = 0; i < m_rules.length; i++) {
			if (i > 0) {
				buf.append("\nunion all\n");
			}
			buf.append(m_rules[i].toSQL());
		}
		return buf.toString();
	}
	
	public Union toBooleanUCQ(int name) {
		SignedRule[] rules = new SignedRule[m_rules.length];
		for (int i = 0; i < rules.length; i++) {
			rules[i] = m_rules[i].toBooleanCQ(name);
		}
		return new Union(name, 0, rules);
	}
	
	public Union shallowMinimize() {
		int discarded = 0;
		boolean[] discard = new boolean[m_rules.length];
		for (int i = 0; i < m_rules.length; i++) {
			if (discard[i]) {
				continue;
			}
			for (int j = i+1; j < m_rules.length; j++) {
				if (discard[j]) {
					continue;
				} else if (m_rules[i].findIsomorphism(m_rules[j], false) != null) {
					discard[i] = discard[j] = true;
					discarded += 2;
					break;
				}
			}
		}
		if (discarded == 0) {
			return this;
		}
		SignedRule[] rules = new SignedRule[m_rules.length-discarded];
		int position = 0;
		for (int i = 0; i < m_rules.length; i++) {
			if (!discard[i]) {
				rules[position++] = m_rules[i];
			}
		}
		return new Union(m_name, m_arity, rules);
	}
	
	public Union difference(int name, Union subtrahend) {
		SignedRule[] rules = new SignedRule[m_rules.length + subtrahend.m_rules.length];
		System.arraycopy(m_rules, 0, rules, 0, m_rules.length);
		for (int i = 0; i < subtrahend.m_rules.length; i++) {
			rules[m_rules.length + i] = subtrahend.m_rules[i].flipSign();
		}
		return new Union(name, m_arity, rules);
	}
	
	public Union addRules(SignedRule... rules) {
		SignedRule[] both = new SignedRule[m_rules.length + rules.length];
		System.arraycopy(m_rules, 0, both, 0, m_rules.length);
		System.arraycopy(rules, 0, both, m_rules.length, rules.length);
		return new Union(m_name, m_arity, both);
	}

	public Union mash(Union view, RuleMorphism morph, int source, int target) {
		assert morph.type == MorphismType.SUBSTITUTION;
		// source is index in view
		// target is index in this
		SignedRule sub = view.m_rules[source].applySubstitution(m_rules[target], morph);
		Union left = addRules(sub);
		Union right = new Union(m_name, m_arity, sub).unfoldView(view);
		return left.difference(m_name, right);
	}
	
	public static Union parse(String str) {
		String[] lines = str.split("\n");
		if (lines.length >= 1 && lines[0].trim().length() > 0) {
			SignedRule[] rules = new SignedRule[lines.length];
			for (int i = 0; i < rules.length; i++) {
				rules[i] = SignedRule.parse(lines[i]);
			}
			Atom head = rules[0].getRule().getHead();
			return new Union(head.getName(), head.getArity(), rules);
		}
		return null;
	}

	protected void recursiveUnfold(Union view, SignedRule rule, Vector<SignedRule> unfolded) {
		if (rule.getRule().getNthOccurrence(view.getName(), 0) == null) {
			unfolded.add(rule);
		} else for (SignedRule r : view.m_rules) {
			SignedRule ru = rule.unfoldView(r);
			recursiveUnfold(view, ru, unfolded);
		}
	}
	
	public Union unfoldView(Union view) {
		int name = view.getName();
		boolean occurs = false;
		for (SignedRule r : m_rules) {
			if (r.getRule().getNthOccurrence(name, 0) != null) {
				occurs = true;
				break;
			}
		}
		if (occurs == false) {
			return this;
		}
		Vector<SignedRule> unfolded = new Vector<SignedRule>();
		for (SignedRule r : m_rules) {
			recursiveUnfold(view, r, unfolded);
		}
//		// now cancel out pairs of redundant rules R and ~R
//		int i = 0;
//		while (i < unfolded.size()) {
//			RuleMorphism m = null;
//			for (int j = i+1; j < unfolded.size(); j++) {
//				m = unfolded.get(i).findIsomorphism(unfolded.get(j).flipSign());
//				if (m != null) {
//					// cancel these two out
//					unfolded.remove(j);
//					unfolded.remove(i);
//					break;
//				}
//			}
//			if (m == null) {
//				i++;
//			}
//		}
		return new Union(m_name, m_arity, unfolded);
	}
	
	public Union foldView(Union view) {
		UnionMorphism m = view.findSubstitution(this);
		if (m != null) {
			return applySubstitution(m);
		}
		return this;
	}

	public static Union applySubstitution(UnionMorphism m) {
		Union query = m.target;
		Union view = m.source;
		SignedRule[] rules = new SignedRule[query.m_rules.length-view.m_rules.length+1];
		int index = 0;
		rules[index++] = new SignedRule(Rule.applySubstitution(m.morphMap[0]), m.positive);
		for (int i = 0; i < query.m_rules.length; i++) {
			if (!Utils.contains(m.ruleMap, i)) {
				rules[index++] = query.m_rules[i];
			}
		}
		assert index == rules.length;
		Union union = new Union(query.m_name, query.m_arity, rules);
		return union.shallowMinimize();
	}
	
	public UnionMorphism findHomomorphism(Union target) {
		return firstMorphism(target, MorphismType.HOMOMORPHISM);
	}

	public UnionMorphism findIsomorphism(Union target) {
		if (hashCode() != target.hashCode()) {
			return null;
		}
//		return firstMorphism(target, MorphismType.ISOMORPHISM);
		if (m_rules.length == target.m_rules.length) {
			int len = m_rules.length;
			int[] ruleMap = new int[len];
			Arrays.fill(ruleMap, -1);
			boolean [] taken = new boolean[len];
			RuleMorphism[] morphMap = new RuleMorphism[len];
			for (int i = 0; i < m_rules.length; i++) {
				boolean success = false;
				for (int j = 0; !success && j < m_rules.length; j++) {
					if (!taken[j]) {
						RuleMorphism m = m_rules[i].findIsomorphism(target.m_rules[j]);
						if (m != null) {
							ruleMap[i] = j;
							taken[j] = true;
							morphMap[i] = m;
							success = true;
						}
					}
				}
				if (!success) {
					return null;
				}
			}
			return new UnionMorphism(this, target, MorphismType.ISOMORPHISM, ruleMap, morphMap, true);
		}
		return null;
	}
	
	public UnionMorphism findSubstitution(Union target) {
		return firstMorphism(target, MorphismType.SUBSTITUTION);
	}
	
	public UnionMorphism nextMorphism(UnionMorphism m) {
		m = new UnionMorphism(m);
		if (m.type == MorphismType.SUBSTITUTION) {
			if (nextSubstitution(m)) {
				return m;
			}
		} else if (next(m, m.morphMap.length-1)) {
			return m;
		}
		return null;
	}

	protected boolean isSafeSubstitution(UnionMorphism m) {
		int size = m.morphMap.length;
		if (size < 2) {
			return true;
		}
		RuleMorphism m1 = m.morphMap[size-2];
		RuleMorphism m2 = m.morphMap[size-1];
		Rule r1 = Rule.applySubstitution(m1);
		Rule r2 = Rule.applySubstitution(m2);
		return r1.findIsomorphism(r2) != null;
	}

	protected boolean isSafe(UnionMorphism m, int pos) {
		if (pos > 0) {
			RuleMorphism m1 = m.morphMap[pos-1];
			RuleMorphism m2 = m.morphMap[pos];
			Rule r1 = Rule.applySubstitution(m1);
			Rule r2 = Rule.applySubstitution(m2);
			return r1.findIsomorphism(r2) != null;
		}
		return true;
	}
	
	protected boolean isInjective(UnionMorphism m, int pos) {
		int index = m.ruleMap[pos];
		for (int i = 0; i < pos; i++) {
			if (m.ruleMap[i] == index) {
				return false;
			}
		}
		return true;
	}

	protected boolean bumpMorphism(UnionMorphism m, int pos) {
		if (m.ruleMap[pos] == -1) {
			return false;
		}
		SignedRule source = m.source.m_rules[pos];
		SignedRule target = m.target.m_rules[m.ruleMap[pos]];
		
		if (m.morphMap[pos] == null) {
			m.morphMap[pos] = source.firstMorphism(target, m.type, m.positive);
			return m.morphMap[pos] != null;
		} else {
			return m.morphMap[pos].next();
		}
	}
	
	protected boolean bumpIt(UnionMorphism m, int pos) {
		// try bumping morphism
		if (bumpMorphism(m, pos)) {
			return true;
		}
		// try bumping target
		SignedRule source = m.source.m_rules[pos];
		boolean injective = m.type != MorphismType.HOMOMORPHISM;
		for (int i = m.ruleMap[pos]+1; i < m.target.m_rules.length; i++) {
			m.ruleMap[pos] = i;
			if (!injective || isInjective(m, pos)) {
				SignedRule target = m.target.m_rules[i];
				m.morphMap[pos] = source.firstMorphism(target, m.type, m.positive);
				if (m.morphMap[pos] != null) {
					return true;
				}
			}
		}
		m.ruleMap[pos] = -1;
		return false;
	}

	protected boolean bumpDigit(UnionMorphism m, int pos) {
		while (true) {
			if (bumpIt(m, pos) == false) {
				return false;
			} else if (m.type != MorphismType.SUBSTITUTION || isSafe(m, pos)) {
				return true;
			}
		}
	}

	protected boolean nextSubstitution(UnionMorphism m) {
		int len = m.source.m_rules.length;
		// repeatedly bump first morphism, then look for a safe completion
		while (bumpDigit(m,0)) {
			boolean success = true;
			for (int i = 1; success && i < len; i++) {
				success = bumpDigit(m, i);
			}
			if (success) {
				return true;
			}
		}
		if (m.positive) {
			// can't find any more positive substitutions
			// now look for negative substitutions
			m.positive = false;
			return nextSubstitution(m);
		}
		// can't find any more substitutions
		return false;
	}

	protected boolean next(UnionMorphism m, int pos) {
		int len = m.source.m_rules.length;
		while (true) {
			if (bumpDigit(m, pos)) {
				if (pos == len-1) {
					return true;
				} else {
					pos++;
				}
			} else if (pos == 0) {
				if (m.positive && m.type != MorphismType.ISOMORPHISM) {
					m.positive = false;
				} else {
					return false;
				}
			} else {
				pos--;
			}
		}
	}

	public static boolean areEquivalent(Union u1, Union u2, Program p) {
		u1 = p.unfoldQuery(u1);
		u2 = p.unfoldQuery(u2);
		u1 = u1.shallowMinimize();
		u2 = u2.shallowMinimize();
		return u1.findIsomorphism(u2) != null;
	}
	
	public Union deepMinimize(Program prog) {
		if (m_rules.length == 0) {
			return this;
		}
		Union shallow = shallowMinimize();
		Subset discarded = new Subset(shallow.m_rules.length);
		Subset subset = new Subset(shallow.m_rules.length);
		while (subset.next()) {
			if (subset.overlaps(discarded)) {
				continue;
			}
			int size = subset.size();
			SignedRule[] r1 =  new SignedRule[size];
			int p1 = 0;
			for (int i = 0; i < shallow.m_rules.length; i++) {
				if (subset.getBit(i)) {
					r1[p1++] = shallow.m_rules[i];
				}
			}
			Union u1 = new Union(r1);
			u1 = prog.unfoldQuery(u1);
			u1 = u1.shallowMinimize();
			if (u1.m_rules.length == 0) {
				for (int j = 0; j < shallow.m_rules.length; j++) {
					if (subset.getBit(j)) {
						discarded.setBit(j);
					}
				}
			}
		}
		int len = discarded.size();
		if (len == 0) {
			return shallow;
		}
		SignedRule[] rules = new SignedRule[shallow.m_rules.length - len];
		int pos = 0;
		for (int i = 0; i < shallow.m_rules.length; i++) {
			if (!discarded.getBit(i)) {
				rules[pos++] = shallow.m_rules[i];
			}
		}
		assert pos == rules.length;
		return new Union(rules);
	}
	
	public UnionMorphism firstMorphism(Union target, MorphismType type) {
		int len1 = m_rules.length;
		int len2 = target.m_rules.length;
		if (type == MorphismType.ISOMORPHISM) {
			if (len1 != len2) {
				return null;
			}
		} else if (type != MorphismType.HOMOMORPHISM) {
			if (len1 > len2) {
				return null;
			}
		} else {
			// TODO: early exit if signs are incompatible
		}
		UnionMorphism m = new UnionMorphism(this, target, type, true);
		if (type == MorphismType.SUBSTITUTION) {
			if (nextSubstitution(m)) {
				return m;
			}
		} else if (next(m, 0)) {
			return m;
		}
		return null;
	}
	
	public String toSQL(boolean inPlace) {
		return inPlace ? inPlaceToSQL() : toSQL();
	}
	
	protected String inPlaceToSQL() {
		StringBuffer buf = new StringBuffer();
		buf.append("insert into ");
		buf.append(m_name);
		buf.append('\n');
		boolean first = true;
		for (SignedRule rule : m_rules) {
			Rule r = rule.getRule();
			if (r.findNthOccurrence(m_name, 0) > 0) {
				assert r.m_body.length == 1;
			} else {
				if (first) {
					first = false;
				} else {
					buf.append("\nunion all\n");
				}
				buf.append(rule.toSQL());
			}
		}
		assert !first;
		return buf.toString();
	}
}
