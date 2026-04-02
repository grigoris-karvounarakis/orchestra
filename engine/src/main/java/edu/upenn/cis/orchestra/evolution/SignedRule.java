package edu.upenn.cis.orchestra.evolution;

public class SignedRule {
	protected Rule m_rule;
	protected boolean m_positive;	// true means +, false means -
	
	public SignedRule(Rule rule, boolean positive) {
		m_rule = rule;
		m_positive = positive;
	}
	
	public SignedRule(SignedRule signed) {
		m_rule = signed.m_rule;
		m_positive = signed.m_positive;
	}
	
	public SignedRule toBooleanCQ(int name) {
		return new SignedRule(m_rule.toBooleanCQ(name), m_positive);
	}
	
	public SignedRule unfoldView(SignedRule view) {
		return new SignedRule(m_rule.unfoldView(view.m_rule), m_positive == view.m_positive);
	}
	
	static public SignedRule parse(String str) {
		str = str.trim();
		if (str.startsWith("~")) {
			return new SignedRule(Rule.parse(str.substring(1).trim()), false);
		} else {
			return new SignedRule(Rule.parse(str), true);
		}
	}
	
	public SignedRule applySubstitution(SignedRule target, RuleMorphism morph) {
		assert(morph.type == MorphismType.SUBSTITUTION);
		assert(morph.source == m_rule);
		assert(morph.target == target.m_rule);
		boolean positive = m_positive == target.m_positive;
		Rule sub = Rule.applySubstitution(morph);
		return new SignedRule(sub, positive);
	}

	public RuleMorphism findIsomorphism(SignedRule target) {
		return firstMorphism(target, MorphismType.ISOMORPHISM, true);
	}
	
	public RuleMorphism findIsomorphism(SignedRule target, boolean positive) {
		return firstMorphism(target, MorphismType.ISOMORPHISM, positive);
	}
	
	public RuleMorphism firstMorphism(SignedRule target, MorphismType type, boolean positive) {
		if ((m_positive == target.m_positive) == positive) {
			return RuleMorphism.first(m_rule, target.m_rule, type);
		}
		return null;
	}
	
	public SignedRule flipSign() {
		return new SignedRule(m_rule, !m_positive);
	}
	
	public SignedRule renameHead(int name) {
		return new SignedRule(m_rule.renameHead(name), m_positive);
	}
	
	public Rule getRule() {
		return m_rule;
	}
	
	public boolean isPositive() {
		return m_positive;
	}
	
	public String toSQL() {
		return m_rule.toSQL(m_positive);
	}
	
	public String toString() {
		return m_positive ? m_rule.toString() : "~" + m_rule.toString();
	}
}
