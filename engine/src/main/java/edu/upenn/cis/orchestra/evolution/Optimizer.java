package edu.upenn.cis.orchestra.evolution;

import java.util.HashMap;
import java.util.Vector;



public class Optimizer {

//	protected Union rewriteSemijoin(Union query, Rule view, int rule) {
//		Rule[] rules = query.getRules();
//		Rule boolRule = rules[rule].toBooleanCQ("Q");
//		Rule boolView = view.toBooleanCQ("V");
//		RuleMorphism m = boolView.findHomomorphism(boolRule);
//		if (m != null) {
//			Atom viewAtom = view.getHead().rename(m.varMap);
//			Rule joined = rules[rule].addJoin(viewAtom, true);
//			Rule[] copy = rules.clone();
//			copy[rule] = joined;
//			return new Union(copy);
//		}
//		return null;
//	}
	
//	protected Pair<Union,Double> recurseSemijoin(Union query, Program p, boolean inPlace, Pair<Union,Double> current) throws WrappedSQLException {
//		Pair<Union,Double> best = current;
//		Union[] views = p.getRules();
//		Rule[] rules = query.getRules();
//		for (int i = 0; i < views.length; i++) {
//			if (isUseful(views[i])) {
//				Rule[] viewRules = views[i].getRules();
//				for (int j = 0; j < viewRules.length; j++) {
//					for (int k = 0; k < rules.length; k++) {
//						Union rewritten = rewriteSemijoin(query, viewRules[j], k);
//						if (rewritten != null) {
//							double estimate = m_db.estimateCost(rewritten.toSQL(inPlace));
//							if (better(estimate, current.second)) {
//								Pair<Union,Double> plan = new Pair<Union,Double>(rewritten,estimate);
//								plan = recurseSemijoin(rewritten, p, inPlace, plan);
//								if (plan.second < best.second) {
//									best = plan;
//								}
//							}
//						}
//					}
//				}
//			}
//		}
//		return best;
//	}

	public enum Flags { 
		GREEDY			(0x00000001),
		SEMIEXHAUSTIVE	(0x00000002),
		EXHAUSTIVE		(0x00000004),
		INPLACE			(0x00000008),
		COMPACTIFY		(0x00000010),
		MASHING			(0x00000020),
		DELTAS			(0x00000040),
		LIMITED			(0x00000080),
		BENEFITS		(0x00000100);
		
		private final int bit;
		Flags(int bit) 	{ this.bit = bit; }
		int bit() 		{ return bit; }
		public static boolean set(int flags, Flags flag) {
			return (flag.bit & flags) != 0;
		}
		int or(int flags, Flags flag) { return flags | flag.bit(); }
		int and(int flags, Flags flag) { return flags & flag.bit(); }
		public static int parse(String line) {
			int flags = 0;
			String[] parts = line.split("\\s*\\|\\s*");
			for (String part : parts) {
				boolean found = false;
				for (Flags flag : Flags.values()) {
					if (part.compareTo(flag.name()) == 0) {
						flags |= flag.bit;
						found = true;
						break;
					}
				}
				assert(found);
			}
			return flags;
		}

		public static String toString(int flags) {
			StringBuffer buf = new StringBuffer();
			for (Flags flag : Flags.values()) {
				if (set(flags, flag)) {
					if (buf.length() != 0) {
						buf.append(" | ");
					}
					buf.append(flag.name());
				}
			}
			return buf.toString();
		}
		
		public static int create(Flags... flags) {
			int value = 0;
			for (Flags flag : flags) {
				value |= flag.bit;
			}
			return value;
		}
	}

	protected Database m_db;

	public Program orderByBenefit(Program prog, int flags) throws WrappedSQLException {
		Union[] views = prog.getViews();
		double[][] benefit = new double[views.length][views.length];
		for (int i = 0; i < views.length; i++) {
			Program v = new Program(views[i]);
			for (int j = 0; j < views.length; j++) {
				if (i == j) {
					benefit[i][j] = 0.0;
				} else {
					double base = m_db.estimateCost(views[j], flags);
					Pair<Union,Double> rewrite = optimize(views[j], v, flags, null, new HashMap<Union,Double>(), 0);
					assert(rewrite.second <= base);
					benefit[i][j] = base - rewrite.second;
				}
			}
		}
		Union[] reordered = new Union[views.length];
		boolean[] done = new boolean[views.length];
		for (int i = 0; i < views.length; i++) {
			int index = mostBeneficial(benefit, done);
			reordered[i] = views[index];
			done[index] = true;
		}
		return new Program(reordered);
	}
	
	private int mostBeneficial(double[][] benefit, boolean[] done) {
		assert(benefit.length == done.length);
		int best = -1;
		double score = -1;
		for (int i = 0; i < benefit.length; i++) {
			if (done[i]) {
				continue;
			}
			double candidate = 0;
			for (int j = 0; j < benefit.length; j++) {
				if (!done[j]) {
					candidate += benefit[i][j];
				}
			}
			if (candidate > score) {
				best = i;
				score = candidate;
			}
		}
		assert(best >= 0 && best < benefit.length);
		return best;
	}
	
	public Optimizer(Database db) {
		m_db = db;
	}

	protected boolean isUseful(Union view) {
		SignedRule[] rules = view.getRules();
		return rules.length > 1 || rules[0].getRule().getBody().length > 1;
	}
	
	protected static boolean allSet(int flags, int which) {
		return (flags & which) == which;
	}

	protected static boolean someSet(int flags, int which) {
		return (flags & which) != 0;
	}

	protected static final int LIMIT = 2;	// limit for # of rewritings considered per view
	protected static final int MAXDEPTH = 6;
	
	protected Pair<Union,Double> optimize(Union query, Program p, int flags, Pair<Union,Double> previous, HashMap<Union,Double> explored, int depth) throws WrappedSQLException {
		Double cost = explored.get(query);
		if (cost != null) {
			return new Pair<Union,Double>(query,cost);
		}
		cost = m_db.estimateCost(query, flags);
		explored.put(query, cost);
		if (depth >= MAXDEPTH) {
			return new Pair<Union,Double>(query,cost);
		}
		Pair<Union,Double> current = new Pair<Union,Double>(query,cost);
		if (previous != null && previous.second < cost && !Flags.set(flags, Flags.EXHAUSTIVE)) {
			// cost is growing, stop exploring this branch
			return current;
		}
		Pair<Union,Double> best = current;
		Union[] views = p.getViews();
		int rewrites = 0;
		for (int i = 0; i < views.length; i++) {
			boolean finished = false;
			if (Flags.set(flags, Flags.EXHAUSTIVE) || isUseful(views[i])) {
				UnionMorphism m = views[i].findSubstitution(query);
				while (m != null && !finished) {
					Union folded = Union.applySubstitution(m);
					rewrites++;
					if (rewrites > LIMIT && Flags.set(flags, Flags.LIMITED)) {
						finished = true;
					}
					Pair<Union,Double> plan;
					if (Flags.set(flags, Flags.GREEDY)) {
						plan = new Pair<Union,Double>(folded, m_db.estimateCost(folded, flags));
					} else {
						plan = optimize(folded, p, flags, current, explored, depth+1);
					}
					if (plan.second < best.second) {
						best = plan;
					}
					m = views[i].nextMorphism(m);
				}
				if (!finished && Flags.set(flags, Flags.MASHING)) {
					for (int j = 0; !finished && j < views[i].m_rules.length; j++) {
						for (int k = 0; !finished && k < query.m_rules.length; k++) {
							boolean positive = query.m_rules[k].isPositive() == views[i].m_rules[j].isPositive(); 
							RuleMorphism morph = views[i].m_rules[j].firstMorphism(query.m_rules[k], 
									MorphismType.SUBSTITUTION, positive);
							boolean haveMorph = morph != null; 
							while (haveMorph && !finished) {
								Union mashed = query.mash(views[i], morph, j, k);
								mashed = mashed.deepMinimize(p);
								Pair<Union,Double> plan;
								if (Flags.set(flags, Flags.GREEDY)) {
									plan = new Pair<Union,Double>(mashed, m_db.estimateCost(mashed, flags));
								} else {
									plan = optimize(mashed, p, flags, current, explored, depth+1);
								}
								if (plan.second < best.second) {
									best = plan;
								}
								++rewrites;
								if (rewrites > LIMIT && Flags.set(flags, Flags.LIMITED)) {
									finished = true;
								}
								haveMorph = morph.next();
							}
						}
					}
				}
			}
		}
		if (Flags.set(flags, Flags.GREEDY) && best.first != current.first) {
			Pair<Union,Double> plan = optimize(best.first, p, flags, current, explored, depth+1);
			if (plan.second < best.second) {
				best = plan;
			}
		}
		return best;
	}
	
	public Statistic optimizedUpdate(Program views, Union query, int flags) throws WrappedSQLException {
		m_db.clearEstimates();
		HashMap<Union,Double> map = new HashMap<Union,Double>();
		long time = System.currentTimeMillis();
		Pair<Union,Double> rewritten = optimize(query, views, flags, null, map, 0);
		time = System.currentTimeMillis() - time;
		long optTime = m_db.getOptimizerTime();
		int optCalls = m_db.getOptimizerCalls();
		int cacheHits = m_db.getCacheHits();
		Statistic stat = unoptimizedUpdate(query, rewritten.first, flags);
		stat.processorTime = time - optTime;
		stat.optimizerTime = optTime;
		stat.optimizerCalls = optCalls;
		stat.cacheHits = cacheHits;
		return stat;
	}

	public Statistic unoptimizedUpdate(Union orig, Union opt, int flags) throws WrappedSQLException {
		Statistic stat = new Statistic();
		stat.origPlan = orig;
		stat.optPlan = opt;
		stat.origCost = m_db.estimateCost(orig, flags);
		stat.optCost = m_db.estimateCost(opt, flags);
		m_db.resetView(orig);
		stat.origExecuteTime = m_db.executeView(orig);
		m_db.resetView(opt);
		stat.optExecuteTime = m_db.executeView(opt);
		return stat;
	}
	
	protected Pair<Union,Union> chooseDelta(Program prog, Union query, int flags) throws WrappedSQLException {
		String name = Utils.TOKENIZER.getString(query.getName()) + "_delta";
		int n = Utils.TOKENIZER.getInteger(name);
		Union base = new Union(n, query.m_arity, new SignedRule[0]); 
		Union best = query;
		double cheapest = m_db.estimateCost(query, flags);
		for (Union view : prog.getViews()) {
			Union diff = query.difference(n, view);
			double cost = m_db.estimateCost(diff, flags);
			if (cost < cheapest) {
				base = view;
				best = diff;
				cheapest = cost;
			}
		}
		return new Pair<Union,Union>(base,best);
	}
	
	public Union simpleUnion(int name, Union first, Union second) {
		assert first.getArity() == second.getArity();
		int arity = first.getArity();
		Atom head = new Atom(name, arity); 
		Rule r1 = new Rule(head, new Atom(first.getName(), arity));
		Rule r2 = new Rule(head, new Atom(second.getName(), arity));
		if (first.getRules().length == 0) {
			return new Union(name, arity, r2);
		} else if (second.getRules().length == 0) {
			return new Union(name, arity, r1);
		} else {
			return new Union(name, arity, r1, r2);
		}
	}

	public Statistic[] optimizedUpdate(Program before, Program after, int flags) throws WrappedSQLException {
		before = before.unfoldAll();
		after = after.unfoldAll();
		Vector<Statistic> stats = new Vector<Statistic>(after.m_views.length);
		if (Flags.set(flags, Flags.BENEFITS)) {
			System.out.println("Ordering by benefit...");
			after = orderByBenefit(after, flags);
		}
		for (int i = 0; i < after.m_views.length; i++) {
			Union query = after.getView(i);
			String qname = Utils.TOKENIZER.getString(query.getName());
			System.out.println("Optimizing and updating view " + i + " of " + after.m_views.length + " (" + qname + ")...");
			if (Flags.set(flags, Flags.DELTAS)) {
				Pair<Union,Union> pair = chooseDelta(before, query, flags);
				Statistic stat = optimizedUpdate(before, pair.second, flags);
				stats.add(stat);
				Union q2 = simpleUnion(query.getName(), pair.first, pair.second);
				stat = unoptimizedUpdate(query, q2, flags);
				stats.add(stat);
				if (pair.second.getRules().length != 0) {
					before = before.addViews(query, pair.second);
				}
			} else {
				Statistic stat = optimizedUpdate(before, query, flags);
				stats.add(stat);
				before = before.addViews(query);
			}
		}
		return stats.toArray(new Statistic[0]);
	}	
}





