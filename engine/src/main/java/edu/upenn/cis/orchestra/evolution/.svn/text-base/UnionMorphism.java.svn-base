package edu.upenn.cis.orchestra.evolution;

import java.security.InvalidParameterException;
import java.util.Arrays;


public class UnionMorphism {
	public Union source;
	public Union target;
	public MorphismType type;
	public int[] ruleMap;
	public RuleMorphism[] morphMap;
	public boolean positive;
	
	public UnionMorphism(Union source, Union target, MorphismType type, boolean positive) {
		this.source = source;
		this.target = target;
		this.type = type;
		this.ruleMap = new int[source.m_rules.length];
		this.morphMap = new RuleMorphism[source.m_rules.length];
		Arrays.fill(ruleMap, 0, ruleMap.length, -1);
		this.positive = positive;
	}
	
	public UnionMorphism(Union source, Union target, MorphismType type, int[] ruleMap, RuleMorphism[] morphMap, boolean positive) {
		this.source = source;
		this.target = target;
		this.type = type;
		this.ruleMap = ruleMap;
		this.morphMap = morphMap;
		if (ruleMap.length != morphMap.length) {
			throw new InvalidParameterException("Mismatched lengths");
		}
		this.positive = positive;
	}

	public UnionMorphism(UnionMorphism m) {
		this.source = m.source;
		this.target = m.target;
		this.type = m.type;
		int len = source.m_rules.length;
		this.ruleMap = new int[len];
		this.morphMap = new RuleMorphism[len];
		System.arraycopy(m.ruleMap, 0, ruleMap, 0, len);
		System.arraycopy(m.morphMap, 0, morphMap, 0, len);
		this.positive = m.positive;
	}

	public UnionMorphism retarget(Union retarget) {
		assert(false);	// unimplemented
		return null;
	}

	public String toString() {
		return (positive ? "" : "~") + ruleMap + " " + morphMap;
	}
}
