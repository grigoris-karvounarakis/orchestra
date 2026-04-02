package edu.upenn.cis.orchestra.optimization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.Date;
import edu.upenn.cis.orchestra.datamodel.ForeignKey;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.optimization.AndNode.AndNodeType;
import edu.upenn.cis.orchestra.optimization.QueryPlanGenerator.CreatedQP;
import edu.upenn.cis.orchestra.optimization.QueryPlanGenerator.LocalCost;
import edu.upenn.cis.orchestra.optimization.RelationMetadata.ForeignKeyDef;
import edu.upenn.cis.orchestra.optimization.RelationMetadata.FunctionalDependencies;
import edu.upenn.cis.orchestra.optimization.RelationTypes.MaterializedView;
import edu.upenn.cis.orchestra.optimization.Type.TypeError;
import edu.upenn.cis.orchestra.util.CombinedIterator;
import edu.upenn.cis.orchestra.util.Pair;
import edu.upenn.cis.orchestra.util.SubsetIterator;
import edu.upenn.cis.orchestra.util.Triple;

/**
 * @author netaylor
 *
 * @param <P>			Physical properties of query results
 * @param <QP>			Query plan
 * @param <C>			Cost of executing a query plan
 * @param <S>			The schema of stored and intermediate relations
 */
public class Optimizer<P extends PhysicalProperties,QP,C,S extends AbstractRelation> implements OperatorIdSource {
	final RelationTypes<? extends P,? extends S> rt;
	final boolean bushy;

	// Mapping from an expression to the id of the canonical form of that expression,
	// which appears in the andOrGraph, and morphism that maps the expression's atom
	// variables to the canonical expression's
	private final Map<Expression,ExpIdAndMorphism> getCanonicalExp;
	// Mapping from expression ids to the Expressions they represent
	private final Map<Integer,Expression> getExpForId;
	// Next id to assign to an expression
	private int nextExpId;

	// Mapping from expression ids to their data. It serves as its own lock
	private final Map<Pair<Integer,AndNodeType>,List<AndNode>> andOrGraph;

	private final Map<Integer,RelationMetadata> expressionMetadata;

	private final Map<Triple<Integer,P,AndNodeType>,TableEntry> table;
	private final Map<Triple<Integer,P,AndNodeType>,Collection<? extends LocalCost<C,P,S,QP>>> localCosts;

	// If the table contains an entry for a key, then it is currently being computed
	// When it is done, the thread that finishes it will notifyAll on the
	// value for that key in this map
	private final Map<Triple<Integer,P,AndNodeType>,Object> currentComputing;

	private Map<ViewSummary,Collection<MaterializedView<? extends P, ? extends S>>> findViews;

	private final QueryPlanGenerator<P,C,S,QP> queryPlanGenerator;
	private final PhysicalPropertiesFactory<P> propFactory;

	private final boolean takeMax;
	private final C zero;
	final int numProcessingThreads;

	private class TableEntry {
		public final C cost;
		public final LocalCost<C,P,S,QP> lc;
		public final List<TableEntry> inputs;
		public final int expId;

		TableEntry(int expId) {
			cost = null;
			lc = null;
			inputs = null;
			this.expId = expId;
		}

		TableEntry(int expId, C cost, LocalCost<C,P,S,QP> lc, List<TableEntry> inputs) {
			if (cost == null || lc == null || inputs == null) {
				throw new NullPointerException();
			}
			this.cost = cost;
			this.lc = lc;
			this.inputs = inputs;
			this.expId = expId;
		}

		CreatedQP<S,QP,C> getCreatedQP(SchemaFactory<? extends S> schemaFactory) {
			// Cannot cache since if the same subplan is used multiple times in
			// a query we need unique operator IDs
			List<CreatedQP<S,QP,C>> inputQPs = new ArrayList<CreatedQP<S,QP,C>>(inputs.size());
			for (TableEntry entry : inputs) {
				inputQPs.add(entry.getCreatedQP(schemaFactory));
			}
			CreatedQP<S,QP,C> created = lc.createQP(Optimizer.this, schemaFactory, inputQPs);
			queryPlanGenerator.setExpectedCard(created.qp, getExpectedCardinality(expId));
			return created;
		}
	}


	static class ExpIdAndMorphism {
		final int expId;
		// Morphism mapping to canonical expression 
		final Morphism morphism;

		ExpIdAndMorphism(Morphism morphism, int expId) {
			if (morphism == null) {
				this.morphism = null;
			} else {
				this.morphism = morphism.duplicate();
				this.morphism.finish();
			}
			this.expId = expId;
		}

		public String toString() {
			if (morphism == null) {
				return Integer.toString(expId);
			} else {
				return "[" + expId + "," + morphism + "]";
			}
		}
	}

	public static class OrNodeAndMorphism {
		public final List<AndNode> andNodes;
		public final Morphism morphism;

		OrNodeAndMorphism(List<AndNode> andNodes, Morphism morphism) {
			this.andNodes = andNodes;
			this.morphism = morphism;
		}
	}

	public Optimizer(int numProcessingThreads, boolean bushy, RelationTypes<? extends P,? extends S> rt,
			QueryPlanGenerator<P,C,S,QP> qpg, PhysicalPropertiesFactory<P> ppf) {
		this.rt = rt;
		this.bushy = bushy;
		getCanonicalExp = new HashMap<Expression,ExpIdAndMorphism>();
		getExpForId = new HashMap<Integer,Expression>();
		andOrGraph = new HashMap<Pair<Integer,AndNodeType>,List<AndNode>>();
		table = new HashMap<Triple<Integer,P,AndNodeType>,TableEntry>();
		currentComputing = new HashMap<Triple<Integer,P,AndNodeType>,Object>();
		this.queryPlanGenerator = qpg;
		this.propFactory = ppf;

		takeMax = qpg.takeMaxOfMultipleInputs();
		zero = qpg.getIdentity();
		this.numProcessingThreads = numProcessingThreads;
		if (numProcessingThreads == 1) {
			expressionMetadata = new HashMap<Integer,RelationMetadata>();
			localCosts = new HashMap<Triple<Integer,P,AndNodeType>,Collection<? extends LocalCost<C,P,S,QP>>>();			
		} else {
			expressionMetadata = Collections.synchronizedMap(new HashMap<Integer,RelationMetadata>());
			localCosts = Collections.synchronizedMap(new HashMap<Triple<Integer,P,AndNodeType>,Collection<? extends LocalCost<C,P,S,QP>>>());
		}

		Collection<String> viewNames = rt.getMaterializedViewNames();
		findViews = new HashMap<ViewSummary,Collection<MaterializedView<? extends P,? extends S>>>(viewNames.size());
		for (String viewName : viewNames) {
			MaterializedView<? extends P,? extends S> mv = rt.getMaterializedView(viewName);
			ViewSummary vs = new ViewSummary(mv.exp);
			Collection<MaterializedView<? extends P,? extends S>> views = findViews.get(vs);
			if (views == null) {
				views = new ArrayList<MaterializedView<? extends P,? extends S>>();
				findViews.put(vs, views);
			}
			views.add(mv);
		}
	}

	public CreatedQP<S,QP,C> createQueryPlan(Query query, P props, SchemaFactory<? extends S> schemaFactory) throws InterruptedException {
		return createQueryPlan(query,null,props,schemaFactory);
	}

	public CreatedQP<S,QP,C> createQueryPlan(Query query, S querySchema, P props, SchemaFactory<? extends S> schemaFactory) throws InterruptedException {
		CreatedQP<S,QP,C> plan = getPlan(query.getExpression(), props, schemaFactory);
		return queryPlanGenerator.createQueryRoot(query.head, querySchema, schemaFactory, this, props, plan);
	}

	synchronized ExpIdAndMorphism addToGraph(Expression exp) {
		ExpIdAndMorphism already = getCanonicalExp.get(exp);
		if (already != null) {
			return already;
		} else if (exp.isScan()) {
			int id = nextExpId++;
			already = new ExpIdAndMorphism(null, id);
			getCanonicalExp.put(exp, already);
			getExpForId.put(id, exp);
			return already;
		}
		List<Morphism> morphisms = exp.computePermutationMappings();
		final int numMorphisms = morphisms.size();
		List<Expression> mapped = new ArrayList<Expression>(numMorphisms);
		List<Morphism> inverses = new ArrayList<Morphism>(numMorphisms);
		for (Morphism mapping : morphisms) {
			try {
				mapped.add(exp.applyMorphism(mapping, rt));
			} catch (VariableNotInMapping vnim) {
				throw new RuntimeException("Permuation mappings not computed correctly", vnim);
			}
			inverses.add(Morphism.createInverse(mapping));
		}
		int expId = nextExpId++;
		for (int i = 0; i < numMorphisms; ++i) {
			getCanonicalExp.put(mapped.get(i), new ExpIdAndMorphism(inverses.get(i), expId));
		}
		getExpForId.put(expId, exp);
		return new ExpIdAndMorphism(null, expId);
	}

	synchronized Expression getExpressionForId(int id) {
		return getExpForId.get(id);
	}

	synchronized ExpIdAndMorphism getCanonicalExp(Expression e) {
		return getCanonicalExp.get(e);
	}



	private abstract static class RelationOrFunction {
	}

	private static class Relation extends RelationOrFunction {
		String relName;
		int occ;
		Relation(String relName, int occ) {
			this.relName = relName;
			this.occ = occ;
		}

		public int hashCode() {
			return relName.hashCode() + 37 * occ;
		}

		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}

			Relation r = (Relation) o;
			return (relName.equals(r.relName) && occ == r.occ);
		}

		public String toString() {
			return relName + "[" + occ + "]";
		}
	}

	private static class FunctionInstance extends RelationOrFunction {
		Function f;
		FunctionInstance(Function f) {
			this.f = f;
		}

		public int hashCode() {
			return f.hashCode();
		}

		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}

			FunctionInstance fi = (FunctionInstance) o;
			return f.equals(fi.f);
		}

		public String toString() {
			return f.toString();
		}
	}

	private boolean usesOnlyIncludedFunctions(Set<RelationOrFunction> rofs) {
		for (RelationOrFunction rof : rofs) {
			if (rof instanceof Relation) {
				throw new IllegalArgumentException("Must call method with a set of only function calls");
			}
			FunctionInstance fi = (FunctionInstance) rof;
			for (Variable v : fi.f.getInputVariables()) {
				if (v instanceof Function) {
					FunctionInstance input = new FunctionInstance((Function) v);
					if (! rofs.contains(input)) {
						return false;
					}
				}
			}
		}

		return true;
	}

	private boolean isConnected(Set<? extends RelationOrFunction> rofs, Map<Variable,EquivClass> findEquivClass) {
		Set<Relation> rs = new HashSet<Relation>();
		Set<FunctionInstance> fs = new HashSet<FunctionInstance>();
		for (RelationOrFunction rof : rofs) {
			if (rof instanceof FunctionInstance) {
				fs.add((FunctionInstance) rof);
			} else {
				rs.add((Relation) rof);
			}
		}

		UnionFind<RelationOrFunction> uf = new UnionFind<RelationOrFunction>();
		// Unify using all equalities between relational atoms
		for (Relation r : rs) {
			final int size = rt.getNumColumns(r.relName);
			for (int i = 0; i < size; ++i) {
				EquivClass ec = findEquivClass.get(new AtomVariable(r.relName, r.occ, i, rt));
				if (ec == null) {
					continue;
				}
				for (Variable v : ec) {
					if (v instanceof AtomVariable) {
						AtomVariable av = (AtomVariable) v;
						Relation rr = new Relation(av.relation, av.occurrence);
						if (rs.contains(rr)) {
							uf.union(r, rr);
						}
					}
				}
			}
		}
		// Unify using equalities referring to a function call
		// if its inputs are in the same connected component
		while (! fs.isEmpty()) {
			boolean changed = false;
			Iterator<FunctionInstance> i = fs.iterator();
			FUNC: while (i.hasNext()) {
				FunctionInstance fi = i.next();
				// Representative of the connected components of the inputs
				// to f
				RelationOrFunction rep = null;
				for (Variable v : fi.f.getInputVariables()) {
					RelationOrFunction vRep = null;
					if (v instanceof Function) {
						vRep = uf.find(new FunctionInstance((Function) v));
					} else if (v instanceof AtomVariable) {
						AtomVariable av = (AtomVariable) v;
						vRep = uf.find(new Relation(av.relation, av.occurrence));
					} else if (v instanceof LiteralVariable) {
						continue;
					} else if (v instanceof EquivClass) {
						for (Variable ecv : ((EquivClass) v)) {
							RelationOrFunction ourRep;
							if (ecv instanceof EquivClass) {
								throw new RuntimeException("An EquivClass should not directly contain another EquivClass");
							} else if (ecv instanceof LiteralVariable) {
								continue;
							} else if (ecv instanceof Aggregate) {
								throw new RuntimeException("Encountered unexpected aggregate " + ecv);
							} else if (ecv instanceof Function) {
								FunctionInstance ffi = new FunctionInstance((Function) ecv);
								if (! fs.contains(ffi)) {
									continue;
								}
								ourRep = uf.find(ffi);
							} else if (ecv instanceof AtomVariable) {
								AtomVariable av = (AtomVariable) ecv;
								Relation r = new Relation(av.relation, av.occurrence);
								if (! rs.contains(r)) {
									continue;
								}
								ourRep = uf.find(r);
							} else {
								throw new RuntimeException("Don't know what to do with variable " + ecv);
							}
							if (vRep == null) {
								vRep = ourRep; 
							} else if (! vRep.equals(ourRep)) {
								throw new RuntimeException("Variables from equiv class " + v + " are in different equiv classes, " + vRep + " and " + ourRep);
							}
						}
						if (vRep == null) {
							// Cannot find something that must be equal to an input variable
							// to a function we must compute
							return false;
						}
					} else {
						throw new RuntimeException("Don't know what to do with variable " + v);
					}
					if (rep == null) {
						rep = vRep;
					} else if (! vRep.equals(rep)) {
						// Input are in diffent connected components, so
						// we can't apply this function (at least for now)
						continue FUNC;
					}
				}
				changed = true;
				i.remove();
				if (rep == null) {
					// zero-ary function
					continue;
				}
				// All of the inputs must be in the same connected component
				uf.union(fi, rep);
				EquivClass ec = findEquivClass.get(fi.f);
				if (ec != null) {
					for (Variable v : ec) {
						if (v instanceof Function) {
							uf.union(fi, new FunctionInstance((Function) v));
						} else if (v instanceof AtomVariable) {
							AtomVariable av = (AtomVariable) v;
							uf.union(fi, new Relation(av.relation, av.occurrence));
						}
					}
				}
			}

			if (! changed) {
				// There is a function with inputs in two different
				// connected components
				return false;
			}
		}

		RelationOrFunction rep = null;
		for (RelationOrFunction rof : rofs) {
			RelationOrFunction rofRep = uf.find(rof);
			if (rep == null) {
				rep = rofRep;
			} else if (! rep.equals(rofRep)) {
				// There is more than one connected component
				return false;
			}
		}

		// All relations and functions are in the same connected component
		return true;
	}

	private void createViewNodes(Expression e, List<AndNode> andNodes) {
		ViewSummary vs = new ViewSummary(e);

		Collection<MaterializedView<? extends P,? extends S>> views = findViews.get(vs);

		if (views == null) {
			return;
		}
		
		try {
			VIEW: for (MaterializedView<?,?> view : views) {
				Expression viewExpNoMorph = view.exp;
				if (e.groupBy == null && viewExpNoMorph.groupBy != null) {
					continue;
				}
				if (e.groupBy != null && viewExpNoMorph.groupBy == null) {
					continue;
				}
				MORPHISM: for (Morphism m : view.exp.computePermutationMappings()) {
					Expression viewExp = viewExpNoMorph.applyMorphism(m, rt);
					// TODO: allow extra foreign-key joins
					if (! viewExp.relAtoms.equals(e.relAtoms)) {
						continue;
					}
					Set<Predicate> toApply = new HashSet<Predicate>();

					// Check that all predicates/equivalences from
					// the view hold in the query. Record predicates from
					// the query that don't hold in the view so that we can
					// apply them
					Set<Variable> viewHead = new HashSet<Variable>(viewExp.head);
					Set<EquivClass> viewECs = new HashSet<EquivClass>(viewExp.equivClasses);
					Set<Predicate> viewPreds = new HashSet<Predicate>(viewExp.predicates);
					Set<Variable> groupBy;
					if (viewExp.groupBy == null) {
						groupBy = null;
					} else {
						groupBy = new HashSet<Variable>(viewExp.groupBy);
					}
					Map<Variable,Variable> findOrigVariable = new HashMap<Variable,Variable>();
					for (EquivClass ec : viewExp.equivClasses) {
						findOrigVariable.put(ec, ec);
					}

					for (Function f : viewExp.functions) {
						findOrigVariable.put(f, f);
					}

					for (Aggregate a : viewExp.aggregates) {
						findOrigVariable.put(a, a);
					}

					Map<Variable,EquivClass> replace = new HashMap<Variable,EquivClass>();

					// Process equiv classes that are entirely disjoint from ones in the
					// view
					EC: for (EquivClass ec : e.equivClasses) {
						for (Variable v : ec) {
							if (viewExp.findEquivClass.containsKey(v)) {
								continue EC;
							}
						}
						Variable rep = null;
						for (Variable v : ec) {
							if ((!(v instanceof LiteralVariable)) && (! viewExp.head.contains(v))) {
								// Can't evaluate predicate
								continue MORPHISM;
							}
							if (rep == null) {
								rep = v;
							} else {
								toApply.add(new Predicate(v, Predicate.Op.EQ, rep));
							}
							replace.put(v, ec);
						}
						findOrigVariable.put(ec, rep);
						viewECs.add(ec);
					}

					boolean unapplied = false;
					MATCH_LOOP: while (replace != null) {
						unapplied = false;
						// Apply the results from the previous iteration
						Set<Variable> newHead = new HashSet<Variable>(viewHead.size());
						for (Variable v : viewHead) {
							Variable vv = v.replaceVariable(replace, true);
							newHead.add(vv == null ? v : vv);
						}
						viewHead = newHead;

						Set<Predicate> newPreds = new HashSet<Predicate>(viewPreds.size());
						for (Predicate p : viewPreds) {
							Predicate pp = p.replaceVariable(replace, true);
							newPreds.add(pp == null ? p : pp);
						}
						viewPreds = newPreds;

						Set<EquivClass> newECs = new HashSet<EquivClass>(viewECs.size());
						HashMap<Variable,Variable> newFindOrig = new HashMap<Variable,Variable>(findOrigVariable.size());
						for (EquivClass ec : viewECs) {
							Variable v = ec.replaceVariable(replace, true);
							EquivClass newEc;
							if (v == null) {
								newEc = ec;
							} else if (v instanceof EquivClass) {
								newEc = (EquivClass) v;
							} else {
								throw new RuntimeException("EquivClass became variable");
							}
							newECs.add(newEc);
							newFindOrig.put(newEc, findOrigVariable.get(ec));
						}
						for (Map.Entry<Variable, Variable> me : findOrigVariable.entrySet()) {
							Variable key = me.getKey(), value = me.getValue();
							if (key instanceof EquivClass) {
								// Handled above
								continue;
							}
							Variable vv = key.replaceVariable(replace, true);
							if (vv == null) {
								vv = key;
							}
							newFindOrig.put(vv, value);
						}
						viewECs = newECs;
						findOrigVariable = newFindOrig;

						if (groupBy != null) {
							Set<Variable> newGroupBy = new HashSet<Variable>(groupBy.size());
							for (Variable v : groupBy) {
								Variable vv = v.replaceVariable(replace, true);
								newGroupBy.add(vv == null ? v : vv);
							}
							groupBy = newGroupBy;
						}

						replace = null;

						EC_LOOP: for (EquivClass viewEc : viewECs) {
							EquivClass expEc = null;
							for (Variable v : viewEc) {
								EquivClass vEc = e.findEquivClass.get(v);
								if (vEc != null) {
									if (expEc == null) {
										expEc = vEc;
									} else if (! expEc.equals(vEc)) {
										// Same EC in view, different EC in exp
										continue MORPHISM;
									}
								}
							}
							if (expEc == null) {
								// In EC in view, not in EC in expression
								continue EC_LOOP;
							}
							if (! expEc.equals(viewEc)) {
								Variable orig = findOrigVariable.get(viewEc);
								for (Variable v : expEc) {
									if (! viewEc.contains(v)) {
										// Variable cannot be in another equiv class in the
										// expression
										if ((v instanceof LiteralVariable || viewHead.contains(v)) &&
												(orig instanceof LiteralVariable || viewHead.contains(orig))) {
											toApply.add(new Predicate(v, Predicate.Op.EQ, orig));
										} else {
											unapplied = true;
										}
									}
								}
								replace = Collections.singletonMap((Variable) viewEc, expEc);
								continue MATCH_LOOP;
							}
						}
					}

					if (unapplied) {
						continue MORPHISM;
					}

					for (Predicate p : viewPreds) {
						if (e.predicates.contains(p)) {
							continue;
						}
						boolean found = false;
						for (Predicate expP : e.predicates) {
							if (expP.implies(p)) {
								found = true;
								break;
							}
						}
						if (! found) {
							continue MORPHISM;
						}
						Variable v1, v2;
						if (p.var1 instanceof LiteralVariable) {
							v1 = p.var1;
						} else {
							Variable vv = findOrigVariable.get(p.var1);
							v1 = vv == null ? p.var1 : vv;
						}
						if (p.var2 instanceof LiteralVariable) {
							v2 = p.var2;
						} else {
							Variable vv = findOrigVariable.get(p.var2);
							v2 = vv == null ? p.var2 : vv;
						}
						toApply.add(new Predicate(v1, p.op, v2));
					}

					if (groupBy != null && (! groupBy.equals(e.groupBy))) {
						continue MORPHISM;
					}

					for (Variable v : e.head) {
						if (! viewHead.contains(v)) {
							continue MORPHISM;
						}
					}

					if (toApply.isEmpty()) {
						andNodes.add(new ViewNode(view.schema.getName(), m));
					} else {
						ExpIdAndMorphism beforePreds = addToGraph(viewExp);
						andNodes.add(new ViewNode(toApply, beforePreds, view.schema.getName(), m));
					}
					continue VIEW;

				}
			}
		} catch (Type.TypeError te) {
			throw new RuntimeException(te);
		} catch (VariableRemoved vr) {
			throw new RuntimeException(vr);
		}
	}


	private void createJoinAndFuncNodes(Expression e, List<AndNode> andNodes, boolean bushy) {
		Set<Relation> relations = new HashSet<Relation>();
		for (Map.Entry<String, Integer> me : e.relAtoms.entrySet()) {
			final String rel = me.getKey();
			final int numOcc = me.getValue();
			for (int i = 1; i <= numOcc; ++i) {
				relations.add(new Relation(rel, i));
			}
		}

		Set<FunctionInstance> functions = new HashSet<FunctionInstance>();

		for (Function f : e.functions) {
			functions.add(new FunctionInstance(f));
		}

		Set<RelationOrFunction> rf = new HashSet<RelationOrFunction>(relations.size() + functions.size());
		rf.addAll(relations);
		rf.addAll(functions);

		Iterator<Set<RelationOrFunction>> subsets = null;
		if (bushy) {
			subsets = new SubsetIterator<RelationOrFunction>(rf, false);
		} else {
			// Try all possible combinations of functions,
			// and all joins against a single table
			subsets = new SubsetIterator<RelationOrFunction>(functions,true);
			List<Set<RelationOrFunction>> rList = new ArrayList<Set<RelationOrFunction>>();
			for (RelationOrFunction r : relations) {
				rList.add(Collections.singleton(r));
			}
			List<Iterator<Set<RelationOrFunction>>> its = new ArrayList<Iterator<Set<RelationOrFunction>>>();
			its.add(subsets);
			its.add(rList.iterator());
			subsets = new CombinedIterator<Set<RelationOrFunction>>(its);
		}
		SUBSET: while (subsets.hasNext()) {
			Set<RelationOrFunction> left = subsets.next();
			Set<RelationOrFunction> right = new HashSet<RelationOrFunction>(rf.size() - left.size());
			for (RelationOrFunction rof : rf) {
				if (! left.contains(rof)) {
					right.add(rof);
				}
			}

			if (left.isEmpty() || right.isEmpty()) {
				continue SUBSET;
			}

			boolean leftAllFunc = (! left.isEmpty());
			boolean rightAllFunc = (! right.isEmpty());


			for (RelationOrFunction rof : left) {
				if (rof instanceof Relation) {
					leftAllFunc = false;
					break;
				}
			}

			for (RelationOrFunction rof : right) {
				if (rof instanceof Relation) {
					rightAllFunc = false;
					break;
				}
			}

			Set<RelationOrFunction> allFunc = null;

			// Make sure the sub-expressions don't contain a cross-product or a
			// function without some of its arguments
			if (leftAllFunc) {
				if (! usesOnlyIncludedFunctions(left)) {
					continue;
				}
				allFunc = left;
			} else {
				if (! isConnected(left, e.findEquivClass)) {
					continue;
				}

			}

			if (rightAllFunc) {
				if (! usesOnlyIncludedFunctions(right)) {
					continue;
				}
				allFunc = right;
			} else {
				if (! isConnected(right, e.findEquivClass)) {
					continue;
				}
			}


			if (allFunc == null) {
				// We're doing a join
				if (e.groupBy != null && e.groupBy.isEmpty()) {
					// Cannot do a foreign key join with no grouping attributes
					continue SUBSET;
				}

				if (left.isEmpty() || right.isEmpty()) {
					// Can't do a join against an empty expression
					continue SUBSET;
				}

				Set<Function> leftFuncs = new HashSet<Function>();
				Set<Function> rightFuncs = new HashSet<Function>();

				// First compute morphisms from this expression
				// into the two subexpressions
				Map<String,Integer> leftRelCount = new HashMap<String,Integer>();
				Morphism lhsMorphism = new Morphism(), rhsMorphism = new Morphism();
				for (RelationOrFunction rof : left) {
					if (rof instanceof FunctionInstance) {
						leftFuncs.add(((FunctionInstance) rof).f);
						continue;
					}
					Relation r = (Relation) rof;
					int thisRelCount;
					if (leftRelCount.containsKey(r.relName)) {
						thisRelCount = leftRelCount.get(r.relName);
						++thisRelCount;
						leftRelCount.put(r.relName, thisRelCount);
					} else {
						thisRelCount = 1;
						leftRelCount.put(r.relName, thisRelCount);
					}
					lhsMorphism.mapOccurrence(r.relName, r.occ, thisRelCount);
				}
				Map<String,Integer> rightRelCount = new HashMap<String,Integer>();
				for (RelationOrFunction rof : right) {
					if (rof instanceof FunctionInstance) {
						rightFuncs.add(((FunctionInstance) rof).f);
						continue;
					}
					Relation r = (Relation) rof;
					int thisRelCount;
					if (rightRelCount.containsKey(r.relName)) {
						thisRelCount = rightRelCount.get(r.relName);
						++thisRelCount;
						rightRelCount.put(r.relName, thisRelCount);
					} else {
						thisRelCount = 1;
						rightRelCount.put(r.relName, thisRelCount);
					}
					rhsMorphism.mapOccurrence(r.relName, r.occ, thisRelCount);
				}

				// Determine the predicates between the subexpressions that we can
				// use as the join condition

				// These contain mapped variables
				Map<Variable,EquivClass> leftFindEc = new HashMap<Variable,EquivClass>();
				Map<Variable,EquivClass> rightFindEc = new HashMap<Variable,EquivClass>();

				Set<EquivClass> lhsClasses = new HashSet<EquivClass>(), rhsClasses = new HashSet<EquivClass>();
				List<Variable> lhsJoinVars = new ArrayList<Variable>(), rhsJoinVars = new ArrayList<Variable>(); 
				Set<EquivClass> joinEquivClasses = new HashSet<EquivClass>();
				for (EquivClass ec : e.equivClasses) {
					Pair<EquivClass,EquivClass> ecs = ec.splitEquivClass(lhsMorphism, rhsMorphism, leftFuncs, rightFuncs, lhsJoinVars, rhsJoinVars, joinEquivClasses, rt);
					EquivClass leftEc = ecs.getFirst();
					EquivClass rightEc = ecs.getSecond();
					if (leftEc != null) {
						lhsClasses.add(leftEc);
						for (Variable v : leftEc) {
							leftFindEc.put(v,leftEc);
						}
					}
					if (rightEc != null) {
						rhsClasses.add(rightEc);
						for (Variable v : rightEc) {
							rightFindEc.put(v, rightEc);
						}
					}
				}

				// Make sure we only have one copy of each equiv class in use so the rest can
				// be garbage collected
				Set<EquivClass> newLhsClasses = new HashSet<EquivClass>(lhsClasses.size());
				Set<EquivClass> newRhsClasses = new HashSet<EquivClass>(rhsClasses.size());

				try {
					for (EquivClass ec : lhsClasses) {
						Variable v = ec.replaceVariable(leftFindEc, false);
						EquivClass ecc;
						if (v == null) {
							ecc = ec;
						} else {
							if (v instanceof EquivClass) {
								ecc = (EquivClass) v;
							} else {
								throw new RuntimeException("Variables in equiv class shouldn't vanish during clean-up: " + ec + " becaome " + v);
							}
						}
						newLhsClasses.add(ecc);
					}

					for (EquivClass ec : rhsClasses) {
						Variable v = ec.replaceChildVariable(rightFindEc, false);
						EquivClass ecc;
						if (v == null) {
							ecc = ec;
						} else {
							if (v instanceof EquivClass) {
								ecc = (EquivClass) v;
							} else {
								throw new RuntimeException("Variables in equiv class shouldn't vanish during clean-up: " + ec + " becaome " + v);
							}
						}
						newRhsClasses.add(ecc);
					}
				} catch (VariableRemoved vr) {
					throw new RuntimeException("Shouldn't catch VariableRemoved without null mapping", vr);
				}

				lhsClasses = newLhsClasses;
				rhsClasses = newRhsClasses;

				// Partition the predicates into those over the left,
				// those of the right
				// Those that can be pushed into either are greedily pushed into both

				Set<Predicate> leftPreds = new HashSet<Predicate>(), rightPreds = new HashSet<Predicate>();
				Set<Predicate> toEvalPreds = new HashSet<Predicate>();
				Set<Predicate> pushedPreds = new HashSet<Predicate>();
				for (Predicate p : e.predicates) {
					if (p.var1 instanceof Aggregate || p.var2 instanceof Aggregate) {
						throw new IllegalArgumentException("Encountered unexpected aggregate variable");
					}
					// Compute the mapped names of the predicate variables
					Variable mlv1 = null, mlv2 = null, mrv1 = null, mrv2 = null;
					try {
						try {
							mlv1 = p.var1.applyMorphism(lhsMorphism, rt);
							if (mlv1 == null) {
								mlv1 = p.var1;
							}
							Variable temp = mlv1.replaceVariable(leftFindEc, true);
							if (temp != null) {
								mlv1 = temp;
							}
						} catch (VariableNotInMapping vnim) {
						}
						try {
							mlv2 = p.var2.applyMorphism(lhsMorphism, rt);
							if (mlv2 == null) {
								mlv2 = p.var2;
							}
							Variable temp = mlv2.replaceVariable(leftFindEc, true);
							if (temp != null) {
								mlv2 = temp;
							}
						} catch (VariableNotInMapping vnim) {
						}
						try {
							mrv1 = p.var1.applyMorphism(rhsMorphism, rt);
							if (mrv1 == null) {
								mrv1 = p.var1;
							}
							Variable temp = mrv1.replaceVariable(rightFindEc, true);
							if (temp != null) {
								mrv1 = temp;
							}
						} catch (VariableNotInMapping vnim) {
						}
						try {
							mrv2 = p.var2.applyMorphism(rhsMorphism, rt);
							if (mrv2 == null) {
								mrv2 = p.var2;
							}
							Variable temp = mrv2.replaceVariable(rightFindEc, true);
							if (temp != null) {
								mrv2 = temp;
							}
						} catch (VariableNotInMapping vnim) {
						}
					} catch (VariableRemoved vr) {
						throw new RuntimeException("Shouldn't catch VariableRemoved without null mapping", vr);
					}

					try {
						Predicate leftPred = (mlv1 == null || mlv2 == null) ? null : new Predicate(mlv1, p.op, mlv2);
						Predicate rightPred = (mrv1 == null || mrv2 == null) ? null : new Predicate(mrv1, p.op, mrv2);
						if (leftPred == null && rightPred == null) {
							toEvalPreds.add(p);
						} else {
							pushedPreds.add(p);
							if (leftPred != null) {
								leftPreds.add(leftPred);
							}
							if (rightPred != null) {
								rightPreds.add(rightPred);
							}
						}
					} catch (Type.TypeError te) {
						throw new RuntimeException("Caught type error while creating predicates", te);
					}
				}

				Set<Function> leftVars = new HashSet<Function>(), rightVars = new HashSet<Function>();
				for (Function f : e.functions) {
					FunctionInstance fi = new FunctionInstance(f);
					try {
						if (left.contains(fi)) {
							Function leftFunc;
							Variable temp = f.applyMorphism(lhsMorphism, rt);
							if (temp == null) {
								leftFunc = f;
							} else if (temp instanceof Function) {
								leftFunc = (Function) temp;
							} else {
								throw new RuntimeException("Function became constant");
							}
							temp = leftFunc.replaceChildVariable(leftFindEc, false);
							if (temp != null) {
								if (temp instanceof Function) {
									leftFunc = (Function) temp;
								} else {
									throw new RuntimeException("Function became constant");
								}
							}
							leftVars.add(leftFunc);
						} else if (right.contains(fi)) {
							Function rightFunc;
							Variable temp = f.applyMorphism(rhsMorphism, rt);
							if (temp == null) {
								rightFunc = f;
							} else if (temp instanceof Function) {
								rightFunc = (Function) temp;
							} else {
								throw new RuntimeException("Function became constant");
							}
							temp = rightFunc.replaceChildVariable(rightFindEc, false);
							if (temp != null) {
								if (temp instanceof Function) {
									rightFunc = (Function) temp;
								} else {
									throw new RuntimeException("Function became consant");
								}
							}
							rightVars.add(rightFunc);
						} else {
							throw new RuntimeException("Don't know what to do with function " + f);
						}
					} catch (VariableNotInMapping vnim) {
						throw new RuntimeException("Error while translating functions", vnim);
					} catch (VariableRemoved vr) {
						throw new RuntimeException("Shouldn't catch VariableRemoved without null mapping", vr);
					}

				}

				Set<Variable> beforePredsHead = new HashSet<Variable>(e.head);
				for (Predicate p : toEvalPreds) {
					if (! (p.var1 instanceof LiteralVariable)) {
						beforePredsHead.add(p.var1);
					}
					if (! (p.var2 instanceof LiteralVariable)) {
						beforePredsHead.add(p.var2);
					}
				}


				// Split the head between the left and the right sides
				Set<Variable> lHead = new HashSet<Variable>(), rHead = new HashSet<Variable>();
				for (Variable v : beforePredsHead) {
					try {
						Variable vv = v.applyMorphism(lhsMorphism, rt);
						if (vv == null) {
							vv = v;
						}
						if (vv instanceof LiteralVariable) {
							// Ignore equiv classes that should go entirely to the
							// other side, but have a literal in them.
							continue;
						}
						Variable vvv = v.replaceVariable(leftFindEc, false);
						if (vvv == null) {
							vvv = vv;
						}
						lHead.add(vvv);
					} catch (VariableNotInMapping vnim) {
					} catch (VariableRemoved vr) {
						throw new RuntimeException(vr);
					}
				}
				for (Variable v : beforePredsHead) {
					try {
						Variable vv = v.applyMorphism(rhsMorphism, rt);
						if (vv == null) {
							vv = v;
						}
						if (vv instanceof LiteralVariable) {
							// Ignore equiv classes that should go entirely to the
							// other side, but have a literal in them.
							continue;
						}
						Variable vvv = v.replaceVariable(rightFindEc, false);
						if (vvv == null) {
							vvv = vv;
						}
						rHead.add(vvv);
					} catch (VariableNotInMapping vnim) {
					} catch (VariableRemoved vr) {
						throw new RuntimeException(vr);
					}
				}

				// These are already in the child expressions' schemas
				lHead.addAll(lhsJoinVars);
				rHead.addAll(rhsJoinVars);

				Morphism lhsInverse = Morphism.createInverse(lhsMorphism), rhsInverse = Morphism.createInverse(rhsMorphism);

				Set<Variable> leftGroupBy = null, rightGroupBy = null;
				Set<Aggregate> leftAggs = Collections.emptySet(), rightAggs = Collections.emptySet();
				if (e.groupBy != null) {
					// Only allow foreign key joins
					boolean both[] = { true, false };

					boolean matched = false;
					AGGSIDE: for (boolean aggLeft : both) {
						Set<Variable> aggGroupBy = new HashSet<Variable>(e.groupBy.size()); 
						Morphism aggMorphism;
						Map<Variable,EquivClass> aggFindEc;
						if (aggLeft) {
							aggMorphism = lhsMorphism;
							aggFindEc = leftFindEc;
						} else {
							aggMorphism = rhsMorphism;
							aggFindEc = rightFindEc;
						}
						for (Variable v : e.groupBy) {
							try {
								Variable vv = v.applyMorphism(aggMorphism, rt);
								if (vv == null) {
									vv = v;
								}
								if (vv instanceof LiteralVariable) {
									// Ignore equiv classes that should go entirely to the
									// other side, but have a literal in them.
									continue;
								}
								Variable vvv = v.replaceVariable(aggFindEc, false);
								if (vvv == null) {
									vvv = vv;
								}
								aggGroupBy.add(vvv);
							} catch (VariableNotInMapping vnim) {
							} catch (VariableRemoved vr) {
								throw new RuntimeException(vr);
							}
						}

						if (! aggGroupBy.containsAll(aggLeft ? lhsJoinVars : rhsJoinVars)) {
							continue AGGSIDE;
						}

						Set<Relation> joinRel = new HashSet<Relation>(aggLeft ? right.size() : left.size());
						for (Relation r : relations) {
							if ((aggLeft ? right : left).contains(r)) {
								joinRel.add(r);
							}
						}

						Set<Relation> aggRel = new HashSet<Relation>();
						for (Variable v : e.groupBy) {
							if (v instanceof AtomVariable) {
								AtomVariable av = (AtomVariable) v; 
								Relation r = new Relation(av.relation, av.occurrence);
								if ((aggLeft ? left : right).contains(r)) {
									aggRel.add(r);
								}
							} else if (v instanceof EquivClass) {
								for (Variable vv : ((EquivClass) v)) {
									if (vv instanceof AtomVariable) {
										AtomVariable av = (AtomVariable) vv;
										Relation r = new Relation(av.relation, av.occurrence);
										if ((aggLeft ? left : right).contains(r)) {
											aggRel.add(r);
										}
									}
								}
							}
						}

						Set<Relation> matchedRelations = new HashSet<Relation>(aggRel);
						Set<Relation> addedLastRound = new HashSet<Relation>(aggRel);
						while (! addedLastRound.isEmpty()) {
							Set<Relation> addedThisRound = new HashSet<Relation>();
							for (Relation r : addedLastRound) {
								AbstractRelation relSchema = rt.getBaseRelationSchema(r.relName);
								FK: for (ForeignKey fk : relSchema.getForeignKeys()) {
									List<RelationField> thisRelFields = fk.getFields();
									List<RelationField> refFields = fk.getRefFields();
									int numFields = refFields.size();
									String refRelation = fk.getRefRelation().getName();
									AbstractRelation refSchema = rt.getBaseRelationSchema(refRelation);

									EquivClass[] ecs = new EquivClass[numFields];
									for (int j = 0; j < numFields; ++j) {
										RelationField relField = thisRelFields.get(j);
										AtomVariable relVar = new AtomVariable(r.relName, r.occ, relSchema.getColNum(relField.getName()), rt);
										EquivClass ec = e.findEquivClass.get(relVar);
										if (ec == null || (! joinEquivClasses.contains(ec))) {
											continue FK;
										}
										ecs[j] = ec;
									}
									int refCount = e.relAtoms.get(refRelation);
									RELOCC: for (int i = 1; i <= refCount; ++i) {
										for (int j = 0; j < numFields; ++j) {
											RelationField refField = refFields.get(j);
											AtomVariable refVar = new AtomVariable(refRelation, i, refSchema.getColNum(refField.getName()), rt);
											if (! ecs[j].contains(refVar)) {
												continue RELOCC;
											}
										}
										// All fields match
										Relation newRel =  new Relation(refRelation, i);
										if (! matchedRelations.contains(newRel)) {
											matchedRelations.add(newRel);
											addedThisRound.add(newRel);
										}
									}
								}
							}
							addedLastRound = addedThisRound;
						}

						if (! matchedRelations.containsAll(joinRel)) {
							continue AGGSIDE;
						}

						HashSet<Aggregate> aggs = new HashSet<Aggregate>(e.aggregates.size());

						for (Aggregate a : e.aggregates) {
							try {
								Aggregate aa = a.applyMorphism(aggMorphism, rt);
								if (aa == null) {
									aa = a;
								}
								Aggregate aaa = (Aggregate) a.replaceVariable(aggFindEc, false);
								if (aaa == null) {
									aaa = aa;
								}
								aggs.add(aaa);
							} catch (VariableNotInMapping vnim) {
							} catch (VariableRemoved vr) {
								throw new RuntimeException(vr);
							}
						}


						matched = true;
						if (aggLeft) {
							leftGroupBy = aggGroupBy;
							leftAggs = aggs;
						} else {
							rightGroupBy = aggGroupBy;
							rightAggs = aggs;
						}
						break AGGSIDE;
					}

					if (! matched) {
						continue SUBSET;
					}
				}


				Expression lExp = new Expression(lHead, leftRelCount, lhsClasses, leftGroupBy, leftPreds, leftVars, leftAggs);
				Expression rExp = new Expression(rHead, rightRelCount, rhsClasses, rightGroupBy, rightPreds, rightVars, rightAggs);
				if (toEvalPreds.isEmpty()) {
					andNodes.add(new JoinNode(addToGraph(lExp), addToGraph(rExp), lhsJoinVars, rhsJoinVars, lhsInverse, rhsInverse));
				} else {
					Expression beforePreds = new Expression(beforePredsHead, e.relAtoms, e.equivClasses, e.groupBy, pushedPreds, e.functions, e.aggregates);
					andNodes.add(new JoinNode(toEvalPreds, addToGraph(beforePreds), addToGraph(lExp), addToGraph(rExp), lhsJoinVars, rhsJoinVars, lhsInverse, rhsInverse));							
				}
			} else {
				try {
					// Make sure that we're not evaluating any functions that
					// make use of non-aggregated fields
					if (e.groupBy != null) {
						for (RelationOrFunction rof : allFunc) {
							Function f = ((FunctionInstance) rof).f;
							if (! f.isAggregatedComputable(e.groupBy)) {
								continue SUBSET;
							}
						}
					}

					// Make sure that any predicates or equiv classes that refer to a
					// function we're about to evaluate are evaluated immediately
					// thereafter
					Set<Predicate> toEvalPreds = new HashSet<Predicate>();
					Set<EquivClass> remainingEcs = new HashSet<EquivClass>();
					Map<EquivClass,Variable> replaceEC = new HashMap<EquivClass,Variable>();
					for (EquivClass ec : e.equivClasses) {
						List<Function> funcsToPred = new ArrayList<Function>();
						boolean changed = false;
						EquivClass newEc = new EquivClass();
						for (Variable v : ec) {
							if (v instanceof Function) {
								Function f = (Function) v;
								FunctionInstance fi = new FunctionInstance(f);
								if (allFunc.contains(fi)) {
									funcsToPred.add(f);
									changed = true;
								} else {
									newEc.add(v);
								}
							} else {
								newEc.add(v);
							}
						}
						if (changed) {
							if (newEc.size() == 0) {
								Function rep = funcsToPred.get(0);
								int numFuncs = funcsToPred.size();
								for (int i = 1; i < numFuncs; ++i) {
									toEvalPreds.add(new Predicate(rep, Predicate.Op.EQ, funcsToPred.get(i)));
								}
							} else {
								newEc.setFinished();
								remainingEcs.add(newEc);
								for (Function f : funcsToPred) {
									toEvalPreds.add(new Predicate(f, Predicate.Op.EQ, newEc));
								}
								replaceEC.put(ec, newEc);
							}
						} else {
							remainingEcs.add(ec);
						}
					}

					Set<Predicate> toPushPreds = new HashSet<Predicate>();
					for (Predicate p : e.predicates) {
						FunctionInstance fi1 = null, fi2 = null;
						if (p.var1 instanceof Function) {
							fi1 = new FunctionInstance((Function) p.var1);
						}
						if (p.var2 instanceof Function) {
							fi2 = new FunctionInstance((Function) p.var2);
						}
						Predicate pp = p.replaceVariable(replaceEC, false);
						if (pp == null) {
							pp = p;
						}
						if ((fi1 != null && allFunc.contains(fi1)) || (fi2 != null && allFunc.contains(fi2))) {
							toEvalPreds.add(pp);
						} else {
							toPushPreds.add(pp);
						}
					}



					Set<Function> applyFvs = new HashSet<Function>(allFunc.size());
					for (RelationOrFunction rof : allFunc) {
						Function f = ((FunctionInstance) rof).f;
						Variable v = f.replaceVariable(replaceEC, false);
						if (v == null) {
							applyFvs.add(f);
						} else if (v instanceof Function) {
							applyFvs.add((Function) v);
						} else {
							throw new RuntimeException("Function became variable");
						}
					}

					Set<Function> otherFvs = new HashSet<Function>();
					for (Function f : e.functions) {
						Variable vv = f.replaceChildVariable(replaceEC, false);
						Function ff;
						if (vv == null) {
							ff = f;
						} else if (vv instanceof Function) {
							ff = (Function) vv;
						} else {
							throw new RuntimeException("Function became variable");
						}
						if (! applyFvs.contains(ff)) {
							otherFvs.add(ff);
						}
					}

					Set<Variable> newHead = new HashSet<Variable>();
					for (Variable v : e.head) {
						Variable vv = v.replaceVariable(replaceEC, false);
						newHead.add(vv == null ? v : vv);
					}
					newHead.removeAll(applyFvs);

					for (Function f : applyFvs) {
						for (Variable v : f.getInputVariables()) {
							if (! applyFvs.contains(v) && (! (v instanceof LiteralVariable))) {
								newHead.add(v);
							}
						}
					}

					Set<Variable> newGroupBy = null;
					if (e.groupBy != null) {
						newGroupBy = new HashSet<Variable>();
						for (Variable v : e.groupBy) {
							Variable vv = v.replaceChildVariable(replaceEC, false);
							newGroupBy.add(vv == null ? v : vv);
						}
					}

					Set<Aggregate> newAggregates = new HashSet<Aggregate>(); 
					for (Aggregate a : e.aggregates) {
						Variable v = a.replaceChildVariable(replaceEC, false);
						if (v == null) {
							newAggregates.add(a);
						} else if (v instanceof Aggregate) {
							newAggregates.add((Aggregate) a);
						} else {
							throw new RuntimeException("Aggregate became variable");
						}
					}

					Expression inputExp = new Expression(newHead, e.relAtoms, remainingEcs, newGroupBy, toPushPreds, otherFvs, newAggregates);
					if (toEvalPreds.isEmpty()) {
						andNodes.add(new FunctionNode(addToGraph(inputExp), applyFvs));
					} else {
						Set<Variable> head = new HashSet<Variable>();
						for (Variable v : e.head) {
							Variable vv = v.replaceVariable(replaceEC, false);
							head.add(vv == null ? v : vv);
						}
						for (Predicate p : toEvalPreds) {
							if (! (p.var1 instanceof LiteralVariable)) {
								head.add(p.var1);
							}
							if (! (p.var2 instanceof LiteralVariable)) {
								head.add(p.var2);
							}
						}
						Set<Function> newFuncs = new HashSet<Function>(applyFvs);
						newFuncs.addAll(otherFvs);
						Expression beforePreds = new Expression(head, e.relAtoms, remainingEcs, newGroupBy, toPushPreds, newFuncs, newAggregates);
						andNodes.add(new FunctionNode(addToGraph(inputExp), toEvalPreds, addToGraph(beforePreds), applyFvs));
					}
				} catch (Type.TypeError te) {
					throw new RuntimeException(te);
				} catch (VariableRemoved vr) {
					throw new RuntimeException(vr);
				}
			}
		}
	}

	private void createAggregateNodes(Expression e, List<? super AggregateNode> andNodes) {
		try {
			if (e.groupBy != null) {
				Set<Predicate> predsToEval = new HashSet<Predicate>();
				Set<Predicate> predsToPush = new HashSet<Predicate>();
				// We can evaluate the aggregation only if there are no functions, predicates,
				// or equalities that refer to the aggregate variables, and there are
				// no functions, predicates, or equalities that refer to non-aggregates non-grouping
				// variables
				for (Function f : e.functions) {
					if (! f.isNonAggregatedComputable(e.groupBy)) {
						return;
					}
				}

				Set<EquivClass> pushedECs = new HashSet<EquivClass>();
				Map<EquivClass,Variable> replaceEC = new HashMap<EquivClass,Variable>();
				for (EquivClass ec : e.equivClasses) {
					boolean changed = false;
					List<Variable> toEval = new ArrayList<Variable>();
					EquivClass newEc = new EquivClass();
					Variable rep = null;
					for (Variable v : ec) {
						if (! v.isNonAggregatedComputable(e.groupBy)) {
							toEval.add(v);
							changed = true;
						} else {
							newEc.add(v);
							rep = v;
						}
					}
					if (changed) {
						if (newEc.size() > 1) {
							newEc.setFinished();
							pushedECs.add(ec);
							rep = ec;
						}
						for (Variable v : toEval) {
							predsToEval.add(new Predicate(v, Predicate.Op.EQ, rep));
						}
						replaceEC.put(ec, rep);
					} else {
						pushedECs.add(ec);
					}
				}

				for (Predicate p : e.predicates) {
					Predicate pp = p.replaceVariable(replaceEC, false);
					if (pp == null) {
						pp = p;
					}
					if (pp.isNonAggregatedComputable(e.groupBy)) {
						predsToPush.add(pp);
					} else {
						predsToEval.add(pp);
					}
				}

				Set<Aggregate> noAggregates = Collections.emptySet();

				HashSet<Function> newFuncs = new HashSet<Function>();
				for (Function f : e.functions) {
					Variable v = f.replaceVariable(replaceEC, false);
					Function ff;
					if (v == null) {
						ff = f;
					} else if (v instanceof Function) {
						ff = (Function) v;
					} else {
						throw new RuntimeException("Function became literal");
					}
					newFuncs.add(ff);
				}

				HashSet<Variable> newHead = new HashSet<Variable>();
				for (Variable v : e.groupBy) {
					Variable vv = v.replaceVariable(replaceEC, false);
					newHead.add(vv == null ? v : vv);
				}

				for (Aggregate agg : e.aggregates) {
					if (agg.hasInputVariable()) {
						Variable v = agg.getInputVariable();
						Variable vv = v.replaceVariable(replaceEC, false);
						newHead.add(vv == null ? v : vv);
					}
				}

				Set<Variable> groupBy = new HashSet<Variable>();
				for (Variable v : e.groupBy) {
					Variable vv = v.replaceVariable(replaceEC, false);
					groupBy.add(vv == null ? v : vv);
				}

				Set<Aggregate> aggs = new HashSet<Aggregate>();
				for (Aggregate a : e.aggregates) {
					Variable v = a.replaceVariable(replaceEC, false);
					Aggregate aa;
					if (v == null) {
						aa = a;
					} else if (v instanceof Aggregate) {
						aa = (Aggregate) v;
					} else {
						throw new RuntimeException("Aggregate became variable");
					}
					aggs.add(aa);
				}


				Expression subExp = new Expression(newHead, e.relAtoms, pushedECs, null, predsToPush,
						newFuncs, noAggregates);
				if (predsToEval.isEmpty()) {
					andNodes.add(new AggregateNode(addToGraph(subExp), aggs, groupBy));
				} else {
					Set<Variable> head = new HashSet<Variable>();
					for (Variable v : e.head) {
						Variable vv = v.replaceVariable(replaceEC, false);
						head.add(vv == null ? v : vv);
					}
					for (Predicate p : predsToEval) {
						if (! (p.var1 instanceof LiteralVariable)) {
							head.add(p.var1);
						}
						if (! (p.var2 instanceof LiteralVariable)) {
							head.add(p.var2);
						}
					}
					Expression beforePreds = new Expression(head, e.relAtoms, pushedECs, groupBy, predsToPush, newFuncs, aggs);
					andNodes.add(new AggregateNode(addToGraph(subExp), predsToEval, addToGraph(beforePreds), aggs, groupBy));
				}
			}
		} catch (VariableRemoved vr) {
			throw new RuntimeException(vr);
		} catch (TypeError te) {
			throw new RuntimeException(te);
		}

	}

	private List<AndNode> createAndStoreOrNode(Pair<Integer,AndNodeType> idAndAnt) {
		Expression e = getExpressionForId(idAndAnt.getFirst());
		AndNodeType ant = idAndAnt.getSecond();
		List<AndNode> andNodes = new ArrayList<AndNode>();

		if (ant == AndNodeType.FUNC || ant == AndNodeType.JOIN) {
			createJoinAndFuncNodes(e, andNodes, bushy);
			List<AndNode> func = new ArrayList<AndNode>();
			List<AndNode> join = new ArrayList<AndNode>();
			for (AndNode an : andNodes) {
				if (an.getNodeType() == AndNodeType.FUNC) {
					func.add(an);
				} else if (an.getNodeType() == AndNodeType.JOIN) {
					join.add(an);
				} else {
					throw new IllegalStateException("createJoinAndFuncNodes returned a node of type " + an.getNodeType());
				}
			}
			if (ant == AndNodeType.FUNC) {
				andNodes = func;
			} else {
				andNodes = join;
			}
			synchronized (andOrGraph) {
				andOrGraph.put(new Pair<Integer,AndNodeType>(idAndAnt.getFirst(), ant == AndNodeType.FUNC ? AndNodeType.JOIN: AndNodeType.FUNC),
						Collections.unmodifiableList(ant == AndNodeType.FUNC ? join : func));
			}
		} else if (ant == AndNodeType.AGG) {
			createAggregateNodes(e, andNodes);
		} else if (ant == AndNodeType.VIEW) {
			createViewNodes(e, andNodes);
		} else {
			throw new IllegalArgumentException("Don't know how to rewrite " + ant);
		}

		andNodes = Collections.unmodifiableList(andNodes);

		synchronized(andOrGraph) {
			andOrGraph.put(idAndAnt, andNodes);
		}


		return andNodes;
	}

	/**
	 * Get the or node for the canonical expression corresponding to the
	 * supplied expression, and a mapping from the supplied expression
	 * to the canonical expression
	 * 
	 * @param e			The expression to get the or node for
	 * @param lazy		If <code>false</code>, precompute the or nodes
	 * 					for all inputs to this or node (otherwise they will
	 * 					be computed as requested)
	 * @return			The canonical expression's or node and the morphism from
	 * 					the supplied expression to the canonical expression
	 */
	synchronized OrNodeAndMorphism getOrNode(Expression e, AndNodeType ant, boolean lazy) {
		if (ant == AndNodeType.SCAN) {
			throw new IllegalArgumentException();
		}
		ExpIdAndMorphism eid = getCanonicalExp.get(e);
		if (eid == null) {
			eid = addToGraph(e);
		}

		List<AndNode> orNode = new ArrayList<AndNode>();
		Collection<AndNodeType> ants;
		if (ant == null) {
			ants = AndNodeType.all; 
		} else {
			ants = Collections.singleton(ant);
		}
		for (AndNodeType inputAnt : ants) {
			if (inputAnt == AndNodeType.SCAN) {
				continue;
			}
			orNode.addAll(getOrNode(eid.expId, inputAnt, lazy));
		}

		return new OrNodeAndMorphism(orNode, eid.morphism);
	}

	/**
	 * Get the or node for the supplied (canonical) expression id. If
	 * the expression is a scan, then the OrNode may be empty
	 * 
	 * @param expId		The id of the canonical expression of interest
	 * @param lazy		If <code>false</code>, precompute the or nodes
	 * 					for all inputs to this or node (otherwise they will
	 * 					be computed as requested)
	 * @return			The or node
	 */
	synchronized private List<AndNode> getOrNode(int expId, AndNodeType ant, boolean lazy) {
		if (ant == null) {
			throw new NullPointerException();
		}
		Pair<Integer,AndNodeType> pair = new Pair<Integer,AndNodeType>(expId, ant);
		List<AndNode> orNode = andOrGraph.get(pair);
		if (orNode == null) {
			// We need to create the node for the canonical exp
			orNode = createAndStoreOrNode(pair);
		}

		if (! lazy) {
			for (AndNode an : orNode) {
				for (AndNodeType ant2 : ant.getFollowing()) {
					for (ExpIdAndMorphism input : an.getInputs()) {
						getOrNode(input.expId, ant2, false);
					}
				}
			}
		}
		return orNode;
	}

	public double getExpectedCardinality(int id) {
		return getMetadata(id).getExpectedCardinality();
	}

	public RelationMetadata getMetadata(int id) {
		synchronized (expressionMetadata) {
			RelationMetadata md = expressionMetadata.get(id);
			if (md != null) {
				return md;
			}
		}

		Expression e = getExpressionForId(id);

		// Result gets cleaned up to fix equiv classes, deal with projected variables etc.
		// right before it is stored and returned
		RelationMetadata result;
		AndNode an = null;
		RelationMetadata md = null;
		if (e.isScan()) {
			String relName = null;
			for (String rel : e.relAtoms.keySet()) {
				relName = rel;
			}
			Set<Predicate> predsToApply = new HashSet<Predicate>(e.predicates);
			for (EquivClass ec : e.equivClasses) {
				List<Variable> vs = new ArrayList<Variable>(ec.size());
				for (Variable v : ec) {
					vs.add(v);
				}
				int size = vs.size();
				for (int i = 1; i < size; ++i) {
					try {
						predsToApply.add(new Predicate(vs.get(0), Predicate.Op.EQ, vs.get(i)));
					} catch (TypeError te) {
						throw new RuntimeException(te);
					}
				}
			}
			result = rt.getRelationMetadata(relName).applyPredicates(predsToApply);
		} else {
			List<AggregateNode> aggNodes = new ArrayList<AggregateNode>(1);
			createAggregateNodes(e, aggNodes);
			if (! aggNodes.isEmpty()) {
				an = aggNodes.get(0);
			} else {
				List<AndNode> andNodes = new ArrayList<AndNode>();
				createJoinAndFuncNodes(e, andNodes, false);
				FunctionNode fn = null;
				JoinNode jn = null;
				for (AndNode n : andNodes) {
					if (n instanceof JoinNode) {
						jn = (JoinNode) n;
					} else if (n instanceof FunctionNode) {
						FunctionNode fn2 = (FunctionNode) n;
						if (fn == null || fn2.functions.size() > fn.functions.size()) {
							fn = fn2;
						}
					} else {
						throw new IllegalStateException("Unexpected node " + n);
					}
				}
				if (fn != null) {
					an = fn;
				} else if (jn != null) {
					an = jn;
				} else {
					throw new IllegalStateException("Couldn't figure out how to create histogram for expression " + id + ": " + e);
				}
			}
			if (an instanceof OneInputNode) {
				OneInputNode oin = (OneInputNode) an;
				md = getMetadata(oin.input.expId);
				if (oin.input.morphism != null) {
					Morphism m = Morphism.createInverse(oin.input.morphism);
					md = md.applyMorphim(m, rt);
				}

				if (an instanceof AggregateNode) {
					AggregateNode aggNode = (AggregateNode) an;
					double numBuckets = 1.0;
					Set<Variable> groupingVars = aggNode.groupingVariables;
					double pkSelectivity;
					if (md.primaryKey != null && aggNode.groupingVariables.containsAll(md.primaryKey)) {
						// If extra grouping attributes are determined by the
						// other grouping attributed, don't count them
						for (Variable v : md.primaryKey) {
							numBuckets *= md.histograms.get(v).getNumInRange(null, null).distinctValues;
						}
						pkSelectivity = md.pkSelectivity;
					} else {
						Set<Variable> determining = md.FDs.findDeterminingVariables(groupingVars);
						for (Variable v : determining) {
							numBuckets *= md.histograms.get(v).getNumInRange(null, null).distinctValues;
						}
						pkSelectivity = Double.NaN;
					}
					if (numBuckets < 1.0) {
						numBuckets = 1.0;
					}
										
					Map<Variable,Histogram<?>> newHists = new HashMap<Variable,Histogram<?>>(groupingVars.size() + aggNode.aggregates.size());
					for (Variable v : groupingVars) {
						Histogram<?> h = md.histograms.get(v);
						double numVals = h.getNumInRange(null, null).cardinality;
						double scaleFactor = numBuckets / numVals;
						newHists.put(v, h.scaleHistogram(scaleFactor, 1.0));
					}

					for (Aggregate agg : aggNode.aggregates) {
						Histogram<?> hist;
						Aggregate.AggFunc aggFunc = agg.aggFunc;
						if (agg.hasInputVariable()) {
							Histogram<?> inputHist = md.histograms.get(agg.getInputVariable());
							if (aggFunc == Aggregate.AggFunc.AVG || aggFunc == Aggregate.AggFunc.MAX
									|| aggFunc == Aggregate.AggFunc.MIN || aggFunc == Aggregate.AggFunc.SUM) {
								hist = inputHist.createAggregateHistogram(aggFunc, numBuckets);
							} else if (aggFunc == Aggregate.AggFunc.COUNT) {
								hist = Histogram.createCountHistogram(inputHist.getNumInRange(null, null).cardinality, numBuckets); 
							} else {
								throw new UnsupportedOperationException("Don't know how to process aggregate " + aggFunc);
							}
						} else {
							if (aggFunc == Aggregate.AggFunc.COUNT) {
								hist = Histogram.createCountHistogram(md.getExpectedCardinality(), numBuckets);
							} else {
								throw new UnsupportedOperationException("Don't know how to process aggregate " + aggFunc + " as a nullary function");
							}
						}
						newHists.put(agg, hist);
					}

					List<RelationMetadata.ForeignKeyDef> fks = new ArrayList<RelationMetadata.ForeignKeyDef>();
					for (ForeignKeyDef fk : md.foreignKeys) {
						if (groupingVars.containsAll(fk.localVars)) {
							fks.add(fk);
						}
					}

					Set<Variable> newPk = groupingVars;
					
					result = new RelationMetadata(newPk, fks, newHists, md.FDs.restrictToVariables(groupingVars), pkSelectivity);
					if (aggNode.predicates != null) {
						result = result.applyPredicates(aggNode.predicates);
					}
				} else if (an instanceof FunctionNode) {
					FunctionNode fn = (FunctionNode) an;
					Map<Variable,Histogram<?>> hists = new HashMap<Variable,Histogram<?>>(md.histograms);
					List<Function> needed = new ArrayList<Function>();
					Set<Function> already = new HashSet<Function>();
					needed.addAll(fn.functions);
					for (Variable v : md.histograms.keySet()) {
						if (v instanceof Function) {
							already.add((Function) v);
						}
					}
					for (int i = 0; i < needed.size(); ++i) {
						for (Variable v : needed.get(i).getInputVariables()) {
							if (v instanceof Function) {
								if (already.contains(v)) {
									continue;
								} else {
									already.add((Function) v);
									needed.add((Function) v);
								}
							}
						}
					}

					FunctionalDependencies fds = new FunctionalDependencies(md.FDs);

					Collections.reverse(needed);
					for (Function f : needed) {
						List<Variable> inputs = f.getInputVariables();
						List<Histogram<?>> inputHists = new ArrayList<Histogram<?>>(inputs.size());
						for (Variable v : inputs) {
							Histogram<?> h = hists.get(v);
							if (h == null) {
								throw new IllegalStateException("Missing histogram for " + v);
							}
							inputHists.add(h);
						}

						hists.put(f, f.getOutputHistogram(inputHists));
						fds.addDependency(inputs, Collections.singleton(f));
					}

					result = new RelationMetadata(md.primaryKey, md.foreignKeys, hists, md.FDs, md.pkSelectivity);
					if (fn.predicates != null) {
						result = result.applyPredicates(fn.predicates);
					}
				} else {
					throw new RuntimeException("Don't know how to process node " + an);
				}

			} else if (an instanceof JoinNode) {
				JoinNode jn = (JoinNode) an;

				Expression leftExp = getExpressionForId(jn.lhs.expId);
				Expression rightExp = getExpressionForId(jn.rhs.expId);
				RelationMetadata left = getMetadata(jn.lhs.expId);
				RelationMetadata right = getMetadata(jn.rhs.expId);
				if (jn.lhs.morphism != null) {
					Morphism m = Morphism.createInverse(jn.lhs.morphism);
					left = left.applyMorphim(m, rt);
					leftExp = leftExp.applyMorphism(m, rt);
				}
				if (jn.rhs.morphism != null) {
					Morphism m = Morphism.createInverse(jn.rhs.morphism);
					right = right.applyMorphim(m, rt);
					rightExp = rightExp.applyMorphism(m, rt);
				}

				final double leftSize = getExpectedCardinality(jn.lhs.expId);
				final double rightSize = getExpectedCardinality(jn.rhs.expId);
				final double crossProductSize = leftSize * rightSize;

				Map<Variable,Histogram<?>> newHists = new HashMap<Variable,Histogram<?>>();

				// 1.0 = cross product, 0.0 = nothing
				double selectivity = 1.0;

				// leftForiegnKey is true if doing a foreign key join
				// against a key of the left relation
				boolean leftForeignKey = false, rightForeignKey = false;

				// Determine if this is a foreign key join
				if (right.primaryKey != null) {
					FK: for (ForeignKeyDef fk : left.foreignKeys) {
						Integer numOcc = rightExp.relAtoms.get(fk.relation);
						if (numOcc == null) {
							continue;
						}

						for (int i = 1; i <= numOcc; ++i) {
							Set<Integer> needed = new HashSet<Integer>(fk.columns);
							for (Variable v : jn.rhsJoinVars) {
								if (right.primaryKey.contains(v) && v instanceof AtomVariable) {
									AtomVariable av = (AtomVariable) v;
									if (av.relation.equals(fk.relation) && av.occurrence == i) {
										needed.remove(av.position);
										if (needed.isEmpty()) {
											rightForeignKey = true;
											break FK;
										}
									}
								}
							}
						}
					}
				}

				if (left.primaryKey != null && (! rightForeignKey)) {
					FK: for (ForeignKeyDef fk : right.foreignKeys) {
						Integer numOcc = leftExp.relAtoms.get(fk.relation);
						if (numOcc == null) {
							continue;
						}

						for (int i = 1; i <= numOcc; ++i) {
							Set<Integer> needed = new HashSet<Integer>(fk.columns);
							for (Variable v : jn.lhsJoinVars) {
								if (left.primaryKey.contains(v) && v instanceof AtomVariable) {
									AtomVariable av = (AtomVariable) v;
									if (av.relation.equals(fk.relation) && av.occurrence == i) {
										needed.remove(av.position);
										if (needed.isEmpty()) {
											leftForeignKey = true;
											break FK;
										}
									}
								}
							}
						}
					}
				}

				if (leftForeignKey) {
					if (Double.isNaN(left.pkSelectivity)) {
						throw new IllegalStateException("Should have a pk selectivity");
					}
					selectivity = rightSize * left.pkSelectivity / crossProductSize;
				} else if (rightForeignKey) {
					if (Double.isNaN(right.pkSelectivity)) {
						throw new IllegalStateException("Should have a pk selectivity");
					}
					selectivity = leftSize * right.pkSelectivity / crossProductSize;
				} else {
					// TODO: Use functional dependencies here
					selectivity = 1.0;
					Iterator<Variable> lhsIt = jn.lhsJoinVars.iterator(),
					rhsIt = jn.rhsJoinVars.iterator();

					while (lhsIt.hasNext()) {
						Variable l = lhsIt.next(), r = rhsIt.next();
						Histogram<?> leftHist = left.histograms.get(l);
						Histogram<?> rightHist = right.histograms.get(r);
						Histogram<?> joined = getJoinHistogram(l, leftHist, r, rightHist);
						Variable lMapped, rMapped;
						lMapped = l.applyMorphism(jn.lhsMap, rt);
						rMapped = r.applyMorphism(jn.rhsMap, rt);
						if (lMapped == null) {
							lMapped = l;
						}
						if (rMapped == null) {
							rMapped = r;
						}
						newHists.put(lMapped, joined);
						newHists.put(rMapped, joined);
						selectivity *= joined.getNumInRange(null, null).cardinality / crossProductSize;
					}
				}

				double outputSize = selectivity * crossProductSize;

				for (Map.Entry<Variable, Histogram<?>> me : left.histograms.entrySet()) {
					Variable v = me.getKey().applyMorphism(jn.lhsMap, rt);
					if (v == null) {
						v = me.getKey();
					}
					Histogram<?> h;
					if (newHists.containsKey(v)) {
						h = newHists.get(v);
					} else {
						h = me.getValue();
					}
					double currCard = h.getNumInRange(null, null).cardinality;
					double mult = outputSize / currCard;
					double dvMult = mult < 1.0 ? mult : 1.0;
					Histogram<?> scaled = h.scaleHistogram(mult, dvMult);
					newHists.put(v, scaled);
				}

				for (Map.Entry<Variable, Histogram<?>> me : right.histograms.entrySet()) {
					Variable v = me.getKey().applyMorphism(jn.rhsMap, rt);
					if (v == null) {
						v = me.getKey();
					}
					Histogram<?> h;
					if (newHists.containsKey(v)) {
						h = newHists.get(v);
					} else {
						h = me.getValue();
					}
					double currCard = h.getNumInRange(null, null).cardinality;
					double mult = outputSize / currCard;
					double dvMult = mult < 1.0 ? mult : 1.0;
					Histogram<?> scaled = h.scaleHistogram(mult, dvMult);
					newHists.put(v, scaled);
				}

				FunctionalDependencies fds = new FunctionalDependencies();
				Set<Variable> exposedVariables = e.getExposedVariables();
				Set<Variable> pk;
				List<ForeignKeyDef> fks = new ArrayList<ForeignKeyDef>();


				// TODO: use mdLeft and mdRight above
				RelationMetadata mdLeft = left.applyMorphim(jn.lhsMap, rt);
				mdLeft = left.updateWithECs(e.findEquivClass);
				RelationMetadata mdRight = right.applyMorphim(jn.rhsMap, rt);
				mdRight = right.updateWithECs(e.findEquivClass);

				// Copy all relevant functional dependencies from left and right inputs
				for (RelationMetadata inputMD : Arrays.asList(mdLeft, mdRight)) {
					FunctionalDependencies inputFDs = inputMD.FDs.restrictToVariables(exposedVariables);
					fds.addDependenciesFrom(inputFDs);

					for (ForeignKeyDef fk : inputMD.foreignKeys) {
						if (e.getExposedVariables().containsAll(fk.localVars)) {
							fks.add(fk);
						}
					}
				}

				// If possible, turn the primary key of possibly repeated rows
				// into a functional dependency. If a FK join, only do this
				// for the many side since the one side will supply the
				// derived primary key
				if ((! leftForeignKey) && mdRight.primaryKey != null) {
					if (exposedVariables.containsAll(mdRight.primaryKey)) {
						Set<Variable> body = new HashSet<Variable>();
						for (Variable v : mdRight.histograms.keySet()) {
							if (! mdRight.primaryKey.contains(v)) {
								body.add(v);
							}
						}
						if (! body.isEmpty()) {
							fds.addDependency(mdRight.primaryKey, body);
						}
					}
				}
				if (! rightForeignKey && mdRight.primaryKey != null) {
					if (exposedVariables.containsAll(mdLeft.primaryKey)) {
						Set<Variable> body = new HashSet<Variable>();
						for (Variable v : mdLeft.histograms.keySet()) {
							if (! mdLeft.primaryKey.contains(v)) {
								body.add(v);
							}
						}
						if (! body.isEmpty()) {
							fds.addDependency(mdLeft.primaryKey, body);
						}
					}
				}


				double pkSelectivity;
				if (leftForeignKey) {
					pk = mdRight.primaryKey;
					pkSelectivity = mdLeft.pkSelectivity * mdRight.pkSelectivity;
				} else if (rightForeignKey) {
					pk = mdLeft.primaryKey;
					pkSelectivity = mdLeft.pkSelectivity * mdRight.pkSelectivity;
				} else {
					pk = null;
					pkSelectivity = Double.NaN;
				}
				fds.setFinished();
				result = new RelationMetadata(pk, fks, newHists, fds, pkSelectivity);
				if (jn.predicates != null) {
					result = result.applyPredicates(jn.predicates);
				}
			} else {
				throw new RuntimeException("Don't know how to process node " + an);
			}
		}

		result = result.updateWithECs(e.findEquivClass);
		result = result.restrictForExpression(e);
		
		synchronized (expressionMetadata) {
			expressionMetadata.put(id, result);
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	static Histogram<?> getJoinHistogram(Variable v1, Histogram h1, Variable v2, Histogram h2) {
		final Type t1 = v1.getType(), t2 = v2.getType();
		if (t1 instanceof IntType && t2 instanceof IntType) {
			return ((Histogram<Integer>) h1).joinWith((Histogram<Integer>) h2);
		} else if (t1 instanceof DoubleType && t2 instanceof DoubleType) {
			return ((Histogram<Double>) h1).joinWith((Histogram<Double>) h2);
		} else if (t1 instanceof DateType && t2 instanceof DateType) {
			return ((Histogram<Date>) h1).joinWith((Histogram<Date>) h2);
		} else if ((t1 instanceof CharType || t1 instanceof VarCharType) &&
				(t2 instanceof CharType || t2 instanceof VarCharType)) {
			return ((Histogram<String>) h1).joinWith((Histogram<String>) h2);
		} else {
			throw new IllegalArgumentException("Don't know how to join histograms of types " + t1 + " and " + t2);
		}
	}

	@SuppressWarnings("unchecked")
	static Histogram<?> getSemiJoinHistogram(Variable v1, Histogram h1, Variable v2, Histogram h2) {
		final Type t1 = v1.getType(), t2 = v2.getType();
		if (t1 instanceof IntType && t2 instanceof IntType) {
			return ((Histogram<Integer>) h1).semiJoinWith((Histogram<Integer>) h2);
		} else if (t1 instanceof DoubleType && t2 instanceof DoubleType) {
			return ((Histogram<Double>) h1).semiJoinWith((Histogram<Double>) h2);
		} else if (t1 instanceof DateType && t2 instanceof DateType) {
			return ((Histogram<Date>) h1).semiJoinWith((Histogram<Date>) h2);
		} else if ((t1 instanceof CharType || t1 instanceof VarCharType) &&
				(t2 instanceof CharType || t2 instanceof VarCharType)) {
			return ((Histogram<String>) h1).semiJoinWith((Histogram<String>) h2);
		} else {
			throw new IllegalArgumentException("Don't know how to join histograms of types " + t1 + " and " + t2);
		}
	}

	private CreatedQP<S,QP,C> getPlan(Expression e, P props, SchemaFactory<? extends S> schemaFactory) throws InterruptedException {
		ExpIdAndMorphism already = getCanonicalExp(e);
		if (already == null) {
			already = this.addToGraph(e);
		}

		TableEntry te = null;
		C upperBound = null;
		for (AndNodeType ant : AndNodeType.all) {
			TableEntry antTe = getTableEntry(already.expId, props, ant, upperBound);
			if (antTe.cost == null) {
				continue;
			}
			if (te == null || queryPlanGenerator.compare(te.cost, antTe.cost) > 0) {
				te = antTe;
				upperBound = antTe.cost;
			}
		}
		if (te == null || te.cost == null) {
			throw new NullPointerException();
		}
		CreatedQP<S,QP,C> qp = te.getCreatedQP(schemaFactory);

		Morphism m = already.morphism;
		if (m == null) {
			return qp;
		} else {
			final int numVars = qp.schema.getNumCols();
			VariablePosition newVarPos = new VariablePosition(numVars);
			Morphism inverse = Morphism.createInverse(m);

			try {
				for (int i = 0; i < numVars; ++i) {
					Variable v = qp.varPos.getVariable(i);
					Variable vv = v.applyMorphism(inverse, rt);
					if (vv == null) {
						vv = v;
					}
					newVarPos.addVariable(vv);
				}
			} catch (VariableNotInMapping vnim) {
				throw new RuntimeException(vnim);
			}
			newVarPos.finish();
			return new CreatedQP<S,QP,C>(qp.qp, qp.schema, qp.cost, newVarPos);
		}
	}

	private TableEntry getTableEntry(int expId, P props, AndNodeType ant, C upperBound) throws InterruptedException {
		if (ant == null) {
			throw new NullPointerException();
		}
		final Triple<Integer,P,AndNodeType> triple = new Triple<Integer,P,AndNodeType>(expId,props,ant);
		TableEntry result;
		boolean foundLock;
		Object lock = null;
		synchronized (table) {
			result = table.get(triple);
			if (result != null) {
				return result;
			}
		}
		if (numProcessingThreads > 1) {
			while (lock == null) {
				synchronized (currentComputing) {
					lock = currentComputing.get(triple);
					if (lock == null) {
						foundLock = false;
						lock = new Object();
						currentComputing.put(triple, lock);
					} else {
						foundLock = true;
					}
				}
				if (foundLock) {
					synchronized (lock) {
						lock.wait();
					}
					lock = null;
					synchronized (table) {
						result = table.get(triple);
						if (result != null) {
							return result;
						}
					}
					// Otherwise, someone was going to compute a plan
					// but it was going to be too expensive, so we'll
					// try again
				}
			}
		}

		Collection<? extends LocalCost<C,P,S,QP>> localCosts = this.localCosts.get(triple);
		if (localCosts == null) {
			if (ant == AndNodeType.SCAN) {
				LocalCost<C,P,S,QP> lc = queryPlanGenerator.getScanCost(expId, props, this);
				if (lc == null) {
					localCosts = Collections.emptySet();
				} else {
					localCosts = Collections.singleton(lc);
				}
			} else {
				List<LocalCost<C,P,S,QP>> ourLcs = new ArrayList<LocalCost<C,P,S,QP>>();
				List<AndNode> orNode = getOrNode(expId,ant,true);
				for (AndNode an : orNode) {
					ourLcs.addAll(queryPlanGenerator.getLocalCost(an, props, expId, this, propFactory));
				}
				localCosts = ourLcs;
			}
			this.localCosts.put(triple, Collections.unmodifiableCollection(localCosts));
		}

		LocalCost<C,P,S,QP> best = null;
		C bestCost = null;
		List<TableEntry> bestInputs = null;

		PLAN: for (LocalCost<C,P,S,QP> localCost : localCosts) {
			List<TableEntry> inputs = new ArrayList<TableEntry>(localCost.inputs.size());
			// Compute a maximum cost for which we want to explore plans
			// based on an argument and the lowest cost we've seen thus
			// far for this expression
			final C bound;
			if (upperBound != null) {
				if (bestCost != null && queryPlanGenerator.compare(bestCost, upperBound) < 0) {
					bound = bestCost;
				} else {
					bound = upperBound;					
				}
			} else if (bestCost != null) {
				bound = bestCost;
			} else {
				bound = null;
			}
			C currCost = localCost.localCost;
			C inputsCost = zero;
			for (Pair<Integer,P> input : localCost.inputs) {
				if (bound != null && queryPlanGenerator.compare(currCost, bound) > 0) {
					continue PLAN;
				}
				C remainingCost = null;
				if (bound != null) {
					if (takeMax) {
						remainingCost = queryPlanGenerator.subtractFrom(bound, localCost.localCost);
					} else {
						remainingCost = queryPlanGenerator.subtractFrom(bound, currCost);
					}
				}
				TableEntry inputPlan = null;
				for (AndNodeType inputType : ant.getFollowing()) {
					TableEntry te = getTableEntry(input.getFirst(), input.getSecond(), inputType, remainingCost);
					if (te != null) {
						if (te.cost == null) {
							continue;
						} else if (inputPlan == null || queryPlanGenerator.compare(te.cost, inputPlan.cost) < 0) {
							inputPlan = te;
						}
					}
				}
				if (inputPlan == null) {
					// Plan for input was too expensive
					continue PLAN;
				}
				inputs.add(inputPlan);
				if (takeMax) {
					if (queryPlanGenerator.compare(inputPlan.cost, inputsCost) > 0) {
						inputsCost = inputPlan.cost;
					}
				} else {
					inputsCost = queryPlanGenerator.addTogether(inputsCost, inputPlan.cost);
				}
				currCost = queryPlanGenerator.addTogether(inputsCost, localCost.localCost);
			}
			if (best == null || queryPlanGenerator.compare(currCost, bestCost) < 0) {
				best = localCost;
				bestCost = currCost;
				bestInputs = inputs;
			}
		}

		if (best != null) {
			if (best.inputs.size() != bestInputs.size()) {
				throw new IllegalStateException("Arrays should have same size");
			}
			result = new TableEntry(expId, bestCost, best, bestInputs);

			// Store the result and tell anyone waiting for it that it is ready
			synchronized (table) {
				table.put(triple, result);
			}
		} else if (localCosts.size() == 0) {
			result = new TableEntry(expId);
			synchronized (table) {
				table.put(triple, result);
			}
		}
		// Otherwise, result == null since this node was too expensive
		// to explore fully

		synchronized (currentComputing) {
			currentComputing.remove(triple);
		}

		if (lock != null) {
			synchronized (lock) {
				lock.notifyAll();
			}
		}

		return result;
	}

	private int nextOperatorId = 0;

	public synchronized int getOperatorId() {
		return nextOperatorId++;
	}
}
