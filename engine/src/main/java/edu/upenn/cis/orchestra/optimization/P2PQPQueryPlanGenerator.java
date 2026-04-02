package edu.upenn.cis.orchestra.optimization;

import static edu.upenn.cis.orchestra.p2pqp.plan.ScanNode.Type.DistributedProbe;
import static edu.upenn.cis.orchestra.p2pqp.plan.ScanNode.Type.DistributedScan;
import static edu.upenn.cis.orchestra.p2pqp.plan.ScanNode.Type.IndexScan;
import static edu.upenn.cis.orchestra.p2pqp.plan.ScanNode.Type.LocalScan;

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

import edu.upenn.cis.orchestra.datamodel.AbstractRelation.BadColumnName;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.optimization.Aggregate.AggFunc;
import edu.upenn.cis.orchestra.optimization.Optimizer.ExpIdAndMorphism;
import edu.upenn.cis.orchestra.optimization.Predicate.Op;
import edu.upenn.cis.orchestra.optimization.RelationTypes.MaterializedView;
import edu.upenn.cis.orchestra.optimization.Type.TypeError;
import edu.upenn.cis.orchestra.p2pqp.Bandwidth;
import edu.upenn.cis.orchestra.p2pqp.CombineCalibrations;
import edu.upenn.cis.orchestra.p2pqp.DHTService;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.SystemCalibration;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator.ColumnInput;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator.ColumnOrFunction;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator.EvalFunc;
import edu.upenn.cis.orchestra.p2pqp.plan.CentralizedLoc;
import edu.upenn.cis.orchestra.p2pqp.plan.FilterNode;
import edu.upenn.cis.orchestra.p2pqp.plan.PipelinedJoinNode;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlan;
import edu.upenn.cis.orchestra.p2pqp.plan.ScanNode;
import edu.upenn.cis.orchestra.p2pqp.plan.ShipNode;
import edu.upenn.cis.orchestra.p2pqp.plan.SpoolNode;
import edu.upenn.cis.orchestra.p2pqp.plan.AggregateNode.OutputColumn;
import edu.upenn.cis.orchestra.predicate.AndPred;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.predicate.EqualityPredicate;
import edu.upenn.cis.orchestra.predicate.PredicateLitMismatch;
import edu.upenn.cis.orchestra.predicate.PredicateMismatch;
import edu.upenn.cis.orchestra.util.SubsetIterator;
import edu.upenn.cis.orchestra.util.Triple;

public class P2PQPQueryPlanGenerator<S extends QpSchema,M> extends DoubleCostQueryPlanGenerator<Location,S,QueryPlan<M>> {
	final static double PER_OPERATOR_COST = 0.02;

	final static int tuplesPerBucket = 100;
	
	public final int numNodes;


	private final double indexPageSize =  DHTService.numTuplesPerPage;
	// Message size estimates, in bytes
	private final double msgOverheadBytes = 200;

	private final double tuplesPerInterval = 1000;
	private final int keyTupleOverheadBytes = 4;
	private final int fullTupleOverheadBytes = keyTupleOverheadBytes + 32;
	private final int tupleOverheadPerFieldBytes = 5;

	private final SystemCalibration localCalibration, remoteCalibration;
	private final Map<String,SystemCalibration> namedCalibration;
	private final Bandwidth localBandwidth, remoteBandwidth;
	private final Map<String,Bandwidth> namedBandwidth;

	public P2PQPQueryPlanGenerator(int numNodes, CombineCalibrations cc) {
		this(numNodes, cc.localCal, cc.remoteCal, cc.namedCals,
				cc.localBand, cc.remoteBand, cc.namedBands);
	}

	public P2PQPQueryPlanGenerator(int numNodes,
			SystemCalibration localCalibration, SystemCalibration remoteCalibration,
			Map<String,SystemCalibration> namedCalibration, Bandwidth localBandwidth,
			Bandwidth remoteBandwidth, Map<String,Bandwidth> namedBandwidth) {
		this(numNodes, 2, localCalibration, remoteCalibration,
				namedCalibration, localBandwidth, remoteBandwidth, namedBandwidth);
	}

	public P2PQPQueryPlanGenerator(int numNodes, int rehashLogBase,
			SystemCalibration localCalibration, SystemCalibration remoteCalibration,
			Map<String,SystemCalibration> namedCalibration, Bandwidth localBandwidth,
			Bandwidth remoteBandwidth, Map<String,Bandwidth> namedBandwidth) {

		this.numNodes = numNodes;

		this.localCalibration = localCalibration;
		this.remoteCalibration = remoteCalibration;
		this.namedCalibration = new HashMap<String,SystemCalibration>(namedCalibration);
		this.localBandwidth = localBandwidth;
		this.remoteBandwidth = remoteBandwidth;
		this.namedBandwidth = new HashMap<String,Bandwidth>(namedBandwidth);
	}

	public Collection<LocalCost<Double, Location, S, QueryPlan<M>>> getLocalCost(
			AndNode an, final Location l, int expId,
			Optimizer<? extends Location,QueryPlan<M>,Double,? extends S> o,
			PhysicalPropertiesFactory<Location> propFactory) {

		if (an.getNodeType() != AndNode.AndNodeType.AGG && l.willBeRegrouped()) {
			return Collections.emptySet();
		}

		List<LocalCost<Double,Location,S,QueryPlan<M>>> retval = new ArrayList<LocalCost<Double,Location,S,QueryPlan<M>>>();
		final RelationTypes<? extends Location,? extends S> rt = o.rt;
		final Expression e;
		if (an.beforePreds == null) {
			e = o.getExpressionForId(expId);
			if (! l.isValidFor(e)) {
				throw new IllegalArgumentException("Location " + l + " is not valid for expression " + e);
			}
		} else {
			Expression temp = o.getExpressionForId(an.beforePreds.expId);
			Morphism m = Morphism.createInverse(an.beforePreds.morphism);
			e = temp.applyMorphism(m, rt);

			Expression actual = o.getExpressionForId(expId);
			if (! l.isValidFor(e)) {
				throw new IllegalArgumentException("Location " + l + " is not valid for expression " + actual);
			}

		}

		double outputCard = o.getExpectedCardinality(expId);
		final RelationMetadata md = o.getMetadata(expId);

		Set<Predicate> predsToEval = an.predicates;

		Map<Location,Collection<LocalCost<Double,Location,S,QueryPlan<M>>>> localCosts =
			new HashMap<Location,Collection<LocalCost<Double,Location,S,QueryPlan<M>>>>();

		if (an instanceof ViewNode) {
			if (l.isFullyReplicated()) {
				return Collections.emptyList();
			}

			ViewNode vn = (ViewNode) an;
			// They get pushed into the scan
			predsToEval = null;
			final MaterializedView<? extends Location,? extends S> mv = rt.getMaterializedView(vn.viewName);

			final SplitPredicate sp;
			if (vn.predicates != null) {
				Set<EquivClass> empty = Collections.emptySet();
				sp = convertToPredicatesForScan(vn.predicates, empty, mv.schema, mv.varPos);
			} else {
				Set<Predicate> empty = Collections.emptySet();
				sp = new SplitPredicate(null,null,empty,empty);
			}

			Location loc = mv.props;
			if (! loc.isNamedLocation()) {
				throw new RuntimeException("Only support views at named locations for now");
			}

			SystemCalibration sc = namedCalibration.get(loc.getLocationName());
			if (sc == null) {
				throw new IllegalStateException("Missing calibration for location " + loc.getLocationName());
			}

			RelationMetadata viewMd = o.getMetadata(o.addToGraph(mv.exp).expId);

			RelationMetadata postKeyMd;
			if (sp.keyPreds.isEmpty()) {
				postKeyMd = viewMd;
			} else {
				postKeyMd = viewMd.applyPredicates(sp.keyPreds);
			}

			double keyPredSelectivity = postKeyMd.getExpectedCardinality() / viewMd.getExpectedCardinality();
			double fullPredSelectivity = md.getExpectedCardinality() / viewMd.getExpectedCardinality();

			double scanLatency = sc.getVersionedScanCost(viewMd.getExpectedCardinality(), keyPredSelectivity, fullPredSelectivity) + PER_OPERATOR_COST;

			Collection<LocalCost<Double,Location,S,QueryPlan<M>>> lcs = localCosts.get(mv.props);
			if (lcs == null) {
				lcs = new ArrayList<LocalCost<Double,Location,S,QueryPlan<M>>>();
				localCosts.put(mv.props, lcs);
			}

			List<Location> noLocs = Collections.emptyList();
			List<Integer> noInputs = Collections.emptyList();


			lcs.add(new LocalCost<Double,Location,S,QueryPlan<M>>(scanLatency, noInputs, noLocs) {

				CreatedQP<S, QueryPlan<M>, Double> createQP(
						OperatorIdSource ois,
						SchemaFactory<? extends S> schemaFactory,
						List<CreatedQP<S, QueryPlan<M>, Double>> inputs) {
					if (! inputs.isEmpty()) {
						throw new IllegalArgumentException("View scan takes no inputs");
					}

					QueryPlan<M> qp = new ScanNode<M>(mv.schema,
							sp.keyPred, sp.remainingPred,
							mv.props.getPlanLocation(), ois.getOperatorId(), LocalScan);
					qp.setCost(localCost);
					return new CreatedQP<S,QueryPlan<M>,Double>(qp,mv.schema,this.localCost,mv.varPos);
				}

				public String toString() {
					return "ViewNode";
				}

				@Override
				public Location getOutputProperties() {
					return mv.props;
				}
			});
		} else if (an instanceof FunctionNode) {
			FunctionNode fn = (FunctionNode) an;
			final Set<Function> functions = fn.functions;
			final int inputExpId = fn.input.expId;
			final Morphism m = fn.input.morphism;
			final Morphism inverse = Morphism.createInverse(m);

			Expression inputExp = o.getExpressionForId(inputExpId);

			Iterator<Location> locs;
			if (l.isFullyReplicated()) {
				locs = Collections.singleton(Location.FULLY_REPLICATED).iterator();
			} else {
				locs = propFactory.enumerateAllProperties(inputExp, rt);
			}
			while (locs.hasNext()) {
				Location loc = locs.next(); 

				if (loc.isFullyReplicated() && (! l.isFullyReplicated())) {
					continue;
				}

				final Location morphedLoc = loc.applyMorphism(inverse, rt);



				SystemCalibration sc;
				final double nodeOutputCard;
				if (loc.isCentralized()) {
					sc = localCalibration;
					nodeOutputCard = outputCard;
				} else if (loc.isNamedLocation()) {
					sc = namedCalibration.get(loc.getLocationName());
					nodeOutputCard = outputCard;
				} else {
					sc = remoteCalibration;
					if (loc.isFullyReplicated()) {
						nodeOutputCard = outputCard;
					} else {
						nodeOutputCard = md.getPerNodeCardinality(numNodes, loc.getHashVars());
					}
				}

				double cost = PER_OPERATOR_COST + sc.getFunctionLatency(nodeOutputCard, fn.functions.size());

				LocalCost<Double,Location,S,QueryPlan<M>> lc =
					new LocalCost<Double,Location,S,QueryPlan<M>>(cost, Collections.singletonList(inputExpId), Collections.singletonList(loc)) {


					CreatedQP<S,QueryPlan<M>,Double> createQP(
							OperatorIdSource ois,
							SchemaFactory<? extends S> schemaFactory,
							List<CreatedQP<S,QueryPlan<M>,Double>> inputs) {
						if (inputs.size() != 1) {
							throw new IllegalArgumentException("Must be given exactly one input query plan");
						}

						CreatedQP<S,QueryPlan<M>,Double> input = inputs.get(0);

						Set<Variable> vars = e.getExposedVariables();
						final int numVars = vars.size();

						S newSchema = schemaFactory.createNewSchema();
						VariablePosition varPos = fillInSchema(vars, md.primaryKey, newSchema);

						List<ColumnOrFunction> output = new ArrayList<ColumnOrFunction>(numVars);

						for (int i = 0; i < numVars; ++i) {
							Variable v = varPos.getVariable(i);
							if (functions.contains(v)) {
								Function f = (Function) v;
								Function inInput;
								Variable temp = f.applyMorphism(m, rt);
								if (temp == null) {
									inInput = f;
								} else if (temp instanceof Function) {
									inInput = (Function) temp;
								} else {
									throw new RuntimeException("Function became constant");
								}
								output.add(createEvalFunc(inInput, input.varPos, schemaFactory));
							} else {
								Variable inInput;
								inInput = v.applyMorphism(m, rt);
								if (inInput == null) {
									inInput = v;
								}
								output.add(new ColumnInput(input.varPos.getPos(inInput)));
							}
						}

						QueryPlan<M> qp = new edu.upenn.cis.orchestra.p2pqp.plan.FunctionNode<M>(input.schema,
								newSchema, output, morphedLoc.getPlanLocation(), ois.getOperatorId(), input.qp);
						qp.setCost(localCost);
						return new CreatedQP<S,QueryPlan<M>,Double>(qp,
								newSchema, input.cost + this.localCost, varPos);
					}

					private EvalFunc createEvalFunc(Function f, VariablePosition varPos, SchemaFactory<? extends S> schemaFactory) {
						List<ColumnOrFunction> inputs = new ArrayList<ColumnOrFunction>();
						edu.upenn.cis.orchestra.p2pqp.Function qpFunc = f.getP2PQPFunction(schemaFactory);
						for (Variable v : f.getInputVariables()) {
							Type t = v.getType();
							if (t.valueKnown()) {
								throw new IllegalStateException("Should not have input variables with value known");
							} else {
								Integer posInInput = varPos.getPos(v);
								if (posInInput == null) {
									if (v instanceof Function) {
										inputs.add(createEvalFunc((Function) v, varPos, schemaFactory));
									} else {
										throw new IllegalArgumentException("Input variable " + v + " not found");
									}
								} else {
									inputs.add(new ColumnInput(posInInput));
								}
							}
						}

						return new EvalFunc(qpFunc, inputs);
					}

					public String toString() {
						return "FunctionNode";
					}

					@Override
					public Location getOutputProperties() {
						return morphedLoc;
					}
				};

				Collection<LocalCost<Double,Location,S,QueryPlan<M>>> lcs = localCosts.get(morphedLoc);
				if (lcs == null) {
					lcs = new ArrayList<LocalCost<Double,Location,S,QueryPlan<M>>>();
					localCosts.put(morphedLoc, lcs);
				}
				lcs.add(lc);
			}
		} else if (an instanceof AggregateNode) {
			AggregateNode agg = (AggregateNode) an;
			int inputExp = agg.input.expId;
			final Morphism m = agg.input.morphism;
			final Set<Aggregate> aggs = agg.aggregates;
			final Set<Variable> groupingVars = agg.groupingVariables;

			Iterator<Location> expLocs;
			if (l.isFullyReplicated()) {
				expLocs = Collections.singleton(Location.FULLY_REPLICATED).iterator();
			} else {
				expLocs = propFactory.enumerateAllProperties(e, rt);
			}
			LOC: while(expLocs.hasNext()) {
				Location computeLoc = expLocs.next();
				if (l.willBeRegrouped()) {
					if (! computeLoc.isDistributed()) {
						continue LOC;
					}
					computeLoc = computeLoc.getWillBeRegrouped();
				} else if (computeLoc.isDistributed()) {
					if (! e.groupBy.containsAll(computeLoc.getHashVars())) {
						continue LOC;
					}
				} else if (computeLoc.isFullyReplicated() && (! l.isFullyReplicated())) {
					continue LOC;
				}

				Location inputLoc = computeLoc.applyMorphism(m, rt);
				if (inputLoc.willBeRegrouped()) {
					inputLoc = inputLoc.getWillNotBeRegrouped();
				}

				SystemCalibration sc;
				final double nodeInputCard;
				if (inputLoc.isCentralized()) {
					sc = localCalibration;
					nodeInputCard = md.cardinality;
				} else if (inputLoc.isNamedLocation()) {
					sc = namedCalibration.get(inputLoc.getLocationName());
					nodeInputCard = md.cardinality;
				} else {
					sc = remoteCalibration;
					if (inputLoc.isFullyReplicated()) {
						nodeInputCard = md.cardinality;
					} else {
						nodeInputCard = md.getPerNodeCardinality(numNodes, inputLoc.getHashVars());
					}
				}

				double cost = PER_OPERATOR_COST + sc.getAggregateLatency(nodeInputCard, aggs.size());

				AggregateCost lc = new AggregateCost(cost, e, md.primaryKey, m, aggs, groupingVars, rt, inputExp, inputLoc.willBeRegrouped() ? inputLoc.getWillNotBeRegrouped() : inputLoc, computeLoc);
				Collection<LocalCost<Double,Location,S,QueryPlan<M>>> lcs = localCosts.get(computeLoc);
				if (lcs == null) {
					lcs = new ArrayList<LocalCost<Double,Location,S,QueryPlan<M>>>();
					localCosts.put(computeLoc, lcs);
				}
				lcs.add(lc);
			}

			if (! (l.willBeRegrouped() || l.isFullyReplicated())) {
				// Try to aggregate striped across the network and then reaggregate locally)

				Map<Aggregate,Aggregate> rewrittenAggregates = new HashMap<Aggregate,Aggregate>(e.aggregates.size());
				Map<Aggregate,RewrittenAverage> rewrittenAverages = new HashMap<Aggregate,RewrittenAverage>();
				Set<Aggregate> newAggregates = new HashSet<Aggregate>(e.aggregates.size());

				try {
					for (Aggregate a : e.aggregates) {
						if (a.aggFunc.composable) {
							rewrittenAggregates.put(a, new Aggregate(a.aggFunc, a, true));
						} else if (a.aggFunc == AggFunc.COUNT) {
							rewrittenAggregates.put(a, new Aggregate(AggFunc.SUM, a, true));
						} else if (a.aggFunc == AggFunc.AVG) {
							RewrittenAverage ra = new RewrittenAverage(
									new Aggregate(AggFunc.SUM, a.getInputVariable(), false),
									new Aggregate(AggFunc.COUNT, a.getInputVariable(), false));
							rewrittenAverages.put(a, ra);
						} else {
							throw new IllegalArgumentException("Don't know how to rewrite aggregate " + a.aggFunc);
						}
					}
				} catch (Type.TypeError te) {
					throw new RuntimeException("Shouldn't get type error while rewriting aggregates", te);
				}

				for (Aggregate a : rewrittenAggregates.values()) {
					newAggregates.add((Aggregate) a.getInputVariable());
				}

				for (RewrittenAverage ra : rewrittenAverages.values()) {
					newAggregates.add(ra.countCol);
					newAggregates.add(ra.sumCol);
				}

				Set<Variable> rewrittenHead = new HashSet<Variable>(groupingVars.size() + newAggregates.size());
				rewrittenHead.addAll(groupingVars);
				rewrittenHead.addAll(newAggregates);
				Expression rewritten = new Expression(rewrittenHead, e.relAtoms, e.equivClasses,
						groupingVars, e.predicates, e.functions, newAggregates);
				ExpIdAndMorphism rewrittenId = o.addToGraph(rewritten);
				double inputCard = o.getExpectedCardinality(rewrittenId.expId);
				if (! (l.isDistributed() && groupingVars.containsAll(l.getHashVars()))) {
					inputCard *= numNodes;
				}

				Collection<LocalCost<Double,Location,S,QueryPlan<M>>> costsAtCurrLoc = localCosts.get(l);
				if (costsAtCurrLoc == null) {
					costsAtCurrLoc = new ArrayList<LocalCost<Double,Location,S,QueryPlan<M>>>();
					localCosts.put(l, costsAtCurrLoc);
				}

				SystemCalibration sc;
				if (l.isCentralized()) {
					sc = localCalibration;
				} else if (l.isNamedLocation()) {
					sc = namedCalibration.get(l.getLocationName());
				} else {
					sc = remoteCalibration;
				}
				Location inputLoc = l.applyMorphism(m, rt).getWillBeRegrouped();

				double cost = PER_OPERATOR_COST + sc.getAggregateLatency(inputCard, e.aggregates.size());
				AggregateCost lc = new RewrittenAggregateCost(cost, e, md.primaryKey, rewrittenId.morphism,
						aggs, groupingVars, rt, rewrittenId.expId,
						inputLoc, rewrittenAggregates, rewrittenAverages, l);
				costsAtCurrLoc.add(lc);
			}
		} else if (an instanceof JoinNode) {
			JoinNode jn = (JoinNode) an;

			final ExpIdAndMorphism lhs = jn.lhs, rhs = jn.rhs;

			final Morphism inputToLhs = Morphism.createInverse(lhs.morphism), inputToRhs = Morphism.createInverse(rhs.morphism);
			final Morphism lhsJoinMorphism = Morphism.compose(inputToLhs, jn.lhsMap), rhsJoinMorphism = Morphism.compose(inputToRhs, jn.rhsMap);

			final List<Variable> lhsJoinVars, rhsJoinVars;
			if (inputToLhs == null) {
				lhsJoinVars = jn.lhsJoinVars;
			} else {
				lhsJoinVars = new ArrayList<Variable>(jn.lhsJoinVars.size());
				for (Variable v : jn.lhsJoinVars) {
					Variable vv = v.applyMorphism(inputToLhs, rt);
					lhsJoinVars.add(vv == null ? v : vv);
				}
			}
			if (inputToRhs == null) {
				rhsJoinVars = jn.rhsJoinVars;
			} else {
				rhsJoinVars = new ArrayList<Variable>(jn.rhsJoinVars.size());
				for (Variable v : jn.rhsJoinVars) {
					Variable vv = v.applyMorphism(inputToRhs, rt);
					rhsJoinVars.add(vv == null ? v : vv);
				}
			}
			double leftCard = o.getExpectedCardinality(lhs.expId);
			double rightCard = o.getExpectedCardinality(rhs.expId);
			double leftJoinDVs = 1.0, rightJoinDVs = 1.0;
			RelationMetadata leftMetadata = o.getMetadata(lhs.expId);
			RelationMetadata rightMetadata = o.getMetadata(rhs.expId);
			for (Variable v : jn.lhsJoinVars) {
				leftJoinDVs *= leftMetadata.histograms.get(v).getNumDistinctValues();
			}
			for (Variable v : jn.rhsJoinVars) {
				rightJoinDVs *= rightMetadata.histograms.get(v).getNumDistinctValues();
			}
			final double distinctJoinVals = leftJoinDVs > rightJoinDVs ? leftJoinDVs : rightJoinDVs;
			

			Expression rhsExp = o.getExpressionForId(rhs.expId);
			Expression lhsExp = o.getExpressionForId(lhs.expId);

			final int numJoinVars = lhsJoinVars.size();
			Set<Integer> joinPos = new HashSet<Integer>(numJoinVars);
			for (int i = 0; i < numJoinVars; ++i) {
				joinPos.add(i);
			}

			List<Integer> inputExps = Arrays.asList(lhs.expId, rhs.expId);

			// You know what they say about real estate... :-P
			// Each represents three different ways of referring to the same location:
			// in the left input's way, in the right input's way, and in the output's way
			Set<Triple<Location,Location,Location>> locs = new HashSet<Triple<Location,Location,Location>>();

			if (l.isFullyReplicated()) {
				locs.add(new Triple<Location,Location,Location>(Location.FULLY_REPLICATED, Location.FULLY_REPLICATED, Location.FULLY_REPLICATED));
			} else {
				locs.add(new Triple<Location,Location,Location>(Location.CENTRALIZED, Location.CENTRALIZED, Location.CENTRALIZED));

				Iterator<Set<Integer>> joinVarSubsets = new SubsetIterator<Integer>(joinPos, true);

				while (joinVarSubsets.hasNext()) {
					Set<Integer> joinVars = joinVarSubsets.next();
					if (joinVars.isEmpty()) {
						continue;
					}

					HashSet<Variable> leftPos = new HashSet<Variable>(joinVars.size());
					HashSet<Variable> rightPos = new HashSet<Variable>(joinVars.size());
					HashSet<Variable> outputPos = new HashSet<Variable>(joinVars.size());
					for (int pos : joinVars) {
						Variable v = lhsJoinVars.get(pos);
						Variable inOutput = v.applyMorphism(jn.lhsMap, rt);
						if (inOutput == null) {
							inOutput = v;
						}
						EquivClass ec;
						if (inOutput instanceof EquivClass) {
							ec = null;
							for (Variable vv : ((EquivClass) inOutput)) {
								ec = e.findEquivClass.get(vv);
								if (ec != null) {
									break;
								}
							}
						} else {
							ec = e.findEquivClass.get(inOutput);
						}
						if (ec == null) {
							throw new IllegalStateException("Could not find equiv class for joined variable " + inOutput);
						}
						outputPos.add(ec);
						Variable inInput = v.applyMorphism(lhs.morphism, rt);
						leftPos.add(inInput == null ? v : inInput);
						v = rhsJoinVars.get(pos);
						inInput = v.applyMorphism(rhs.morphism, rt);
						rightPos.add(inInput == null ? v : inInput);					
					}


					Location leftLoc = new Location(leftPos);
					Location rightLoc = new Location(rightPos);
					Location outputLoc = new Location(outputPos);

					locs.add(new Triple<Location,Location,Location>(leftLoc,rightLoc,outputLoc));
				}

				Iterator<Location> viewLocs = propFactory.getRelevantViewProperties(e);
				while (viewLocs.hasNext()) {
					Location viewLoc = viewLocs.next();
					locs.add(new Triple<Location,Location,Location>(viewLoc,viewLoc,viewLoc));
				}

				boolean[] rightReplicatedOptions = new boolean[] { true, false };

				for (boolean rightReplicated : rightReplicatedOptions) {
					Expression exp = rightReplicated ? rhsExp : lhsExp;
					Expression other = rightReplicated ? lhsExp : rhsExp;

					boolean foundReplicated = false;
					for (String rel : exp.relAtoms.keySet()) {
						if (rt.getRelationProps(rel).isFullyReplicated()) {
							foundReplicated = true;
							break;
						}
					}

					if (! foundReplicated) {
						continue;
					}

					boolean doingMvLocs = false;
					Iterator<Location> otherLocs = propFactory.enumerateAllProperties(other, rt);
					Morphism otherMorphism = rightReplicated ? lhsJoinMorphism : rhsJoinMorphism;

					while (otherLocs.hasNext()) {
						Location otherLoc = otherLocs.next();
						if (! otherLoc.isDistributed()) {
							continue;
						}

						Set<Variable> outputHashVars = new HashSet<Variable>();
						for (Variable v : otherLoc.getHashVars()) {
							Variable vv = v.applyMorphism(otherMorphism, rt);
							if (vv == null) {
								vv = v;
							}
							if (e.findEquivClass.containsKey(vv)) {
								vv = e.findEquivClass.get(v);
							} else if (vv instanceof EquivClass) {
								for (Variable vvv : ((EquivClass) vv)) {
									if (e.findEquivClass.containsKey(vvv)) {
										vv = e.findEquivClass.get(vvv);
										break;
									}
								}
							}
							outputHashVars.add(vv);
						}

						Location outputLoc = new Location(outputHashVars);

						locs.add(new Triple<Location,Location,Location>(rightReplicated ? otherLoc : Location.FULLY_REPLICATED,
								rightReplicated ? Location.FULLY_REPLICATED : otherLoc,
										outputLoc));

						if ((! otherLocs.hasNext()) && (! doingMvLocs)) {
							otherLocs = propFactory.getRelevantViewProperties(other);
						}
					}
				}
			}


			for (Triple<Location,Location,Location> t : locs) {
				Location leftLoc = t.getFirst();
				Location rightLoc = t.getSecond();
				Location outputLoc = t.getThird();

				List<Location> inputLocs = new ArrayList<Location>(2);
				inputLocs.add(leftLoc);
				inputLocs.add(rightLoc);

				SystemCalibration sc;
				if (outputLoc.isCentralized()) {
					sc = localCalibration;
				} else if (outputLoc.isDistributed() || outputLoc.isFullyReplicated()) {
					sc = remoteCalibration;
				} else {
					sc = namedCalibration.get(outputLoc.getLocationName());
				}

				double nodeLeftCard, nodeRightCard, nodeOutputCard;
				final double nodeDistinctJoinVals;
				if (outputLoc.isDistributed()) {
					nodeLeftCard = leftCard / numNodes;
					nodeRightCard = rightCard / numNodes;
					nodeOutputCard = outputCard / numNodes;
					nodeDistinctJoinVals = distinctJoinVals / numNodes;
				} else {
					nodeLeftCard = leftCard;
					nodeRightCard = rightCard;
					nodeOutputCard = outputCard;
					nodeDistinctJoinVals = distinctJoinVals;
				}

				double cost = PER_OPERATOR_COST + sc.getJoinLatency(nodeLeftCard + nodeRightCard, nodeOutputCard);


				JoinCost jc = new JoinCost(cost, inputExps, inputLocs, outputLoc, lhsJoinVars, rhsJoinVars, lhsJoinMorphism, rhsJoinMorphism,
						md.primaryKey, e, rt) {


					CreatedQP<S,QueryPlan<M>,Double> createQP(
							OperatorIdSource ois,
							SchemaFactory<? extends S> schemaFactory,
							List<CreatedQP<S,QueryPlan<M>,Double>> inputs) {

						if (inputs.size() != 2) {
							throw new IllegalArgumentException("Need exactly two created query plans");
						}

						CreatedQP<S,QueryPlan<M>,Double> left = inputs.get(0), right = inputs.get(1);

						createJoinData(schemaFactory, left.schema, left.varPos,
								right.schema, right.varPos);
						
						int numBuckets = (int) (nodeDistinctJoinVals / tuplesPerBucket);
						if (numBuckets < 100) {
							numBuckets = 100;
						}
						
						QueryPlan<M> qp = new PipelinedJoinNode<M>(newSchema,
								lhsJoinPos, rhsJoinPos, lhsOutputPos, rhsOutputPos,
								loc.getPlanLocation(), numBuckets, left.qp, right.qp, ois.getOperatorId());
						qp.setCost(localCost);
						return new CreatedQP<S,QueryPlan<M>,Double>(qp, newSchema, left.cost + right.cost + localCost, varPos);
					}

					public String toString() {
						return "PipelinedJoinCost";
					}

					@Override
					public Location getOutputProperties() {
						return loc;
					}
				};
				Collection<LocalCost<Double,Location,S,QueryPlan<M>>> lcs = localCosts.get(outputLoc);
				if (lcs == null) {
					lcs = new ArrayList<LocalCost<Double,Location,S,QueryPlan<M>>>();
					localCosts.put(outputLoc, lcs);
				}

				lcs.add(jc);
			}
		} else {
			throw new RuntimeException("Don't know how to process node " + an);
		}

		for (Map.Entry<Location, Collection<LocalCost<Double,Location,S,QueryPlan<M>>>> me :
			localCosts.entrySet()) {
			Location execLoc = me.getKey();
			Collection<LocalCost<Double,Location,S,QueryPlan<M>>> lcs = me.getValue();

			if (predsToEval != null) {
				Expression outputExp = o.getExpressionForId(expId);
				// Apply predicates
				Collection<LocalCost<Double,Location,S,QueryPlan<M>>> newLcs = new ArrayList<LocalCost<Double,Location,S,QueryPlan<M>>>(lcs.size());
				double inputCard = o.getExpectedCardinality(an.beforePreds.expId);
				double selectivity = outputCard / inputCard;

				execLoc = execLoc.updateWithEquivClasses(outputExp.findEquivClass);

				SystemCalibration sc;
				if (execLoc.isCentralized()) {
					sc = localCalibration;
				} else if (execLoc.isDistributed()) {
					sc = remoteCalibration;
				} else {
					sc = namedCalibration.get(execLoc.getLocationName());
				}

				double nodeInputCard;

				if (execLoc.isDistributed()) {
					nodeInputCard = inputCard / numNodes;
				} else {
					nodeInputCard = inputCard;
				}
				double cost = PER_OPERATOR_COST + sc.getPredicateLatency(nodeInputCard, an.predicates.size(), selectivity);

				for (LocalCost<Double,Location,S,QueryPlan<M>> lc : lcs) {
					LocalCost<Double,Location,S,QueryPlan<M>> newLc =
						new PredicateCost(cost, execLoc, an.predicates,
								lc, outputExp);
					newLcs.add(newLc);
				}
				lcs = newLcs;
			}

			if (execLoc.noShippingNeeded(l)) {
				// Compute locally
				retval.addAll(lcs);
			} else if (! l.canShipTo(e)) {
				continue;
			} else {
				// Compute remotely and ship/rehash
				double shipCost = getShipCost(execLoc, l, expId, o);
				for (LocalCost<Double,Location,S,QueryPlan<M>> lc : lcs) {
					retval.add(new ShipLocalCost(shipCost, e.head, execLoc, l, lc));
				}
			}
		}


		for (LocalCost<Double,Location,S,QueryPlan<M>> lc : retval) {
			if (! lc.getOutputProperties().equals(l)) {
				throw new IllegalStateException("LocalCost " + lc + " has location " + lc.getOutputProperties() + " but should have location " + l + " for expression " + expId + ": "+ e);
			}
		}

		return retval;
	}

	private class AggregateCost extends LocalCost<Double,Location,S,QueryPlan<M>> {
		final Expression e;
		final Morphism m;
		final RelationTypes<? extends Location, ? extends S> rt;
		final Set<? extends Variable> primaryKey, groupingVars;
		final Set<? extends Aggregate> aggs;
		final Location computeLoc;
		AggregateCost(double cost, Expression e, Set<? extends Variable> primaryKey, Morphism m,
				Set<? extends Aggregate> aggs, Set<? extends Variable> groupBy,
				RelationTypes<? extends Location, ? extends S> rt,
				int inputExp, Location inputLoc, Location computeLoc) {
			super(cost, Collections.singletonList(inputExp), Collections.singletonList(inputLoc));
			this.e = e;
			this.m = m;
			this.rt = rt;
			this.primaryKey = primaryKey;
			this.groupingVars = groupBy;
			this.aggs = aggs;
			this.computeLoc = computeLoc;
		}

		CreatedQP<S,QueryPlan<M>,Double> createQP(OperatorIdSource ois,
				SchemaFactory<? extends S> schemaFactory,
				List<CreatedQP<S,QueryPlan<M>,Double>> inputs) {

			if (inputs.size() != 1) {
				throw new IllegalArgumentException("Must supply exactly one created query plan");
			}

			CreatedQP<S,QueryPlan<M>,Double> input = inputs.get(0);

			Set<Variable> vars = e.getExposedVariables();
			S newSchema = schemaFactory.createNewSchema();
			VariablePosition varPos = fillInSchema(vars, primaryKey, newSchema);
			final int numCols = vars.size();

			List<OutputColumn> output = new ArrayList<OutputColumn>(numCols);

			for (int i = 0; i < numCols; ++i) {
				Variable v = varPos.getVariable(i);
				output.add(createOutputColumn(v, input.varPos));
			}

			List<Integer> groupingCols = new ArrayList<Integer>(groupingVars.size());
			try {
				for (Variable v : groupingVars) {
					Variable mapped = v.applyMorphism(m, rt);
					if (mapped == null) {
						mapped = v;
					}
					int pos = input.varPos.getPos(mapped);
					groupingCols.add(pos);
				}
			} catch (VariableNotInMapping vnim) {
				throw new RuntimeException(vnim);
			}

			QueryPlan<M> qp = new edu.upenn.cis.orchestra.p2pqp.plan.AggregateNode<M>(input.schema, newSchema,
					groupingCols, output, this.inputs.get(0).getSecond().getPlanLocation(), ois.getOperatorId(),
					input.qp);
			qp.setCost(localCost);
			return new CreatedQP<S,QueryPlan<M>,Double>(qp, newSchema, input.cost + this.localCost, varPos);
		}

		OutputColumn createOutputColumn(Variable v, VariablePosition canonicalVP) {
			Aggregate a = null;
			Variable inputVar = null;
			if (v instanceof Aggregate) {
				a = (Aggregate) v;
				if (a.hasInputVariable()) {
					inputVar = a.getInputVariable();
				}
			} else {
				inputVar = v;
			}
			if (inputVar == null) {
				return new OutputColumn(a.aggFunc);
			} else {
				int inputPos = getPosInInput(inputVar, canonicalVP);
				if (a == null) {
					return new OutputColumn(inputPos);
				} else {
					return new OutputColumn(inputPos,a.aggFunc);
				}
			}
		}

		int getPosInInput(Variable inputVar, VariablePosition canonicalVP) {
			Variable inputMapped;
			try {
				inputMapped = inputVar.applyMorphism(m, rt);
			} catch (VariableNotInMapping vnim) {
				throw new RuntimeException(vnim);
			}
			if (inputMapped == null) {
				inputMapped = inputVar;
			}
			Integer inputPos = canonicalVP.getPos(inputVar);
			if (inputPos == null) {
				throw new IllegalArgumentException("Could not find variable " + inputVar + " (mapped to " + inputMapped + ") in VariablePosition");
			}
			return inputPos;
		}

		@Override
		public String toString() {
			return "AggregateNode";
		}

		@Override
		public Location getOutputProperties() {
			return computeLoc;
		}
	}

	private static class RewrittenAverage {
		final Aggregate sumCol;
		final Aggregate countCol;

		RewrittenAverage(Aggregate sumCol, Aggregate countCol) {
			this.sumCol = sumCol;
			this.countCol = countCol;
		}
	}

	private class RewrittenAggregateCost extends AggregateCost {
		final Map<Aggregate,Aggregate> rewrittenAggregates;
		final Map<Aggregate,RewrittenAverage> rewrittenAverages;

		RewrittenAggregateCost(double cost, Expression e, Set<? extends Variable> primaryKey, Morphism m,
				Set<? extends Aggregate> aggs, Set<? extends Variable> groupBy,
				RelationTypes<? extends Location, ? extends S> rt,
				int inputExp, Location inputLoc, Map<Aggregate,Aggregate> rewrittenAggregates,
				Map<Aggregate,RewrittenAverage> rewrittenAverages, Location computeLoc) {
			super(cost, e, primaryKey, m, aggs, groupBy, rt, inputExp, inputLoc, computeLoc);
			this.rewrittenAggregates = rewrittenAggregates;
			this.rewrittenAverages = rewrittenAverages;
		}


		OutputColumn createOutputColumn(Variable v, VariablePosition canonicalVP) {
			if (v instanceof Aggregate) {
				Aggregate rewritten = rewrittenAggregates.get(v);
				if (rewritten != null) {
					return super.createOutputColumn(rewritten, canonicalVP);
				}
				RewrittenAverage ra = rewrittenAverages.get(v);
				if (ra == null) {
					throw new IllegalArgumentException("Don't have rewriting for " + v);
				}
				int sumCol = this.getPosInInput(ra.sumCol, canonicalVP);
				int countCol = this.getPosInInput(ra.countCol, canonicalVP);
				return new OutputColumn(sumCol, countCol);
			}  else {
				return super.createOutputColumn(v, canonicalVP);
			}
		}

		public String toString() {
			return "RewrittenAggregateCost";
		}
	}


	// Abstract out the computation of the join parameters
	// since it is the same for regular joins and probe joins
	private abstract class JoinCost extends LocalCost<Double,Location,S,QueryPlan<M>> {
		// Names in children
		final List<Variable> lhsJoinVars, rhsJoinVars;
		// Mapping from names in children to names in current expression
		final Morphism lhsMap, rhsMap;
		List<Integer> lhsJoinPos, rhsJoinPos, lhsOutputPos, rhsOutputPos;
		VariablePosition varPos;
		S newSchema;
		final Location loc;
		final Set<Variable> primaryKey;
		final Expression e;
		final RelationTypes<? extends Location,? extends S> rt;

		JoinCost(double cost, List<Integer> inputExps, List<Location> inputLocs, Location loc,
				List<Variable> lhsJoinVars, List<Variable> rhsJoinVars, Morphism lhsMap,
				Morphism rhsMap, Set<Variable> primaryKey, Expression e, RelationTypes<? extends Location,? extends S> rt) {
			super(cost, inputExps, inputLocs);
			this.loc = loc;
			this.lhsJoinVars = lhsJoinVars;
			this.rhsJoinVars = rhsJoinVars;
			this.lhsMap = lhsMap;
			this.rhsMap = rhsMap;
			this.primaryKey = primaryKey;
			this.e = e;
			this.rt = rt;
		}

		void createJoinData(SchemaFactory<? extends S> schemaFactory,
				S leftSchema, VariablePosition leftMap,
				S rightSchema, VariablePosition rightMap) {
			newSchema = schemaFactory.createNewSchema();
			Set<Variable> head = new HashSet<Variable>(e.getExposedVariables());
			varPos = fillInSchema(head, primaryKey, newSchema);

			final int numJoinVars = lhsJoinVars.size();
			final int leftSize = leftSchema.getNumCols();
			final int rightSize = rightSchema.getNumCols();
			lhsJoinPos = new ArrayList<Integer>(numJoinVars);
			rhsJoinPos = new ArrayList<Integer>(numJoinVars);
			lhsOutputPos = new ArrayList<Integer>(leftSize);
			rhsOutputPos = new ArrayList<Integer>(rightSize);

			for (int i = 0; i < numJoinVars; ++i) {
				int lhsPos = leftMap.getPos(lhsJoinVars.get(i));

				int rhsPos = rightMap.getPos(rhsJoinVars.get(i));

				if (lhsPos >= leftSchema.getNumCols()) {
					throw new IllegalStateException("Left join column is " + lhsPos + " but left schema only has " + leftSchema.getNumCols() + " columns");
				}

				if (rhsPos >= rightSchema.getNumCols()) {
					throw new IllegalStateException("Right join column is " + lhsPos + " but left schema only has " + rightSchema.getNumCols() + " columns");
				}

				lhsJoinPos.add(lhsPos);
				rhsJoinPos.add(rhsPos);
			}


			for (int i = 0; i < leftSize; ++i) {
				Variable vv;
				Variable v = leftMap.getVariable(i);
				try {
					vv = v.applyMorphism(lhsMap, rt); 
				} catch (VariableNotInMapping vnim) {
					lhsOutputPos.add(null);
					continue;
				}
				if (vv == null) {
					vv = v;
				}
				try {
					v = vv.replaceVariable(e.findEquivClass, true);
				} catch (VariableRemoved e1) {
					throw new RuntimeException(e1);
				}
				if (v == null) {
					v = vv;
				}
				if (head.contains(v)) {
					lhsOutputPos.add(varPos.getPos(v));
					head.remove(v);
				} else {
					lhsOutputPos.add(null);
				}
			}

			for (int i = 0; i < rightSize; ++i) {
				Variable vv;
				Variable v = rightMap.getVariable(i);
				try {
					vv = v.applyMorphism(rhsMap, rt); 
				} catch (VariableNotInMapping vnim) {
					rhsOutputPos.add(null);
					continue;
				}
				if (vv == null) {
					vv = v;
				}
				try {
					v = vv.replaceVariable(e.findEquivClass, true);
				} catch (VariableRemoved e1) {
					throw new RuntimeException(e1);
				}
				if (v == null) {
					v = vv;
				}
				if (head.contains(v)) {
					rhsOutputPos.add(varPos.getPos(v));
					head.remove(v);
				} else {
					rhsOutputPos.add(null);
				}
			}

			if (! head.isEmpty()) {
				throw new RuntimeException("Didn't find variables " + head + " in left or right side");
			}
		}
	}

	private static final List<Integer> noIds = Collections.emptyList();
	private static final List<Location> noLocs = Collections.emptyList();

	private class ReplicatedScan extends LocalCost<Double,Location,S,QueryPlan<M>> {
		private final S baseSchema;
		private final Location l;
		private final SplitPredicate sp;
		private final VariablePosition baseVarPos;

		public ReplicatedScan(double scanCost, Location l, SplitPredicate sp, S baseSchema, VariablePosition baseVarPos) {
			super(scanCost, noIds, noLocs);
			this.l = l;
			this.sp = sp;
			this.baseSchema = baseSchema;
			this.baseVarPos = baseVarPos;
		}

		@Override
		CreatedQP<S, QueryPlan<M>, Double> createQP(
				OperatorIdSource ois,
				SchemaFactory<? extends S> schemaFactory,
				List<CreatedQP<S, QueryPlan<M>, Double>> inputs) {
			if (! inputs.isEmpty()) {
				throw new IllegalArgumentException("Should have no inputs to a replicated scan");
			}

			QueryPlan<M> qp = new ScanNode<M>(baseSchema,
					sp.keyPred, sp.remainingPred,
					l.getPlanLocation(), ois.getOperatorId(), LocalScan);

			qp.setCost(this.localCost);

			return new CreatedQP<S, QueryPlan<M>, Double>(qp, baseSchema, (double) localCost, baseVarPos);
		}

		@Override
		public String toString() {
			return "ReplicatedScan(" + baseSchema.getName() + ")";
		}

		@Override
		public Location getOutputProperties() {
			return l;
		}
	}

	public LocalCost<Double, Location, S, QueryPlan<M>> getScanCost(
			int expId, final Location l, Optimizer<? extends Location,QueryPlan<M>,Double,? extends S> o) {

		final Expression e = o.getExpressionForId(expId);
		if (! e.isScan()) {
			return null;
		}

		if (! l.isValidFor(e)) {
			throw new IllegalArgumentException("Location " + l + " is not valid for expression " + e);
		}

		double card = o.getMetadata(expId).getExpectedCardinality();

		LocalCost<Double, Location, S, QueryPlan<M>> retval = null;
		double retvalCost = Double.POSITIVE_INFINITY;


		String temp = null;
		for (String rel : e.relAtoms.keySet()) {
			temp = rel;
		}
		final String relation = temp;
		final RelationTypes<? extends Location,? extends S> rt = o.rt;
		Location scanLocInput = rt.getRelationProps(relation);
		final Location scanLoc = scanLocInput.updateWithEquivClasses(e.findEquivClass);



		final VariablePosition baseVarPos = rt.getBaseRelationVarPos(relation);
		final S baseSchema = rt.getBaseRelationSchema(relation);

		Set<Integer> keyCols = baseSchema.getKeyColsSet();
		final Set<Variable> keyVars = new HashSet<Variable>(keyCols.size());
		for (int col : keyCols) {
			keyVars.add(new AtomVariable(relation, 1, col, rt));
		}


		final SplitPredicate sp = convertToPredicatesForScan(e.predicates, e.equivClasses, baseSchema, baseVarPos);


		Set<EquivClass> noECs = Collections.emptySet();
		Set<Function> noFuncs = Collections.emptySet();
		Set<Aggregate> noAggs = Collections.emptySet();
		Expression inputExp = new Expression(keyVars, Collections.singletonMap(relation, 1),
				noECs, null, sp.keyPreds, noFuncs, noAggs);

		ExpIdAndMorphism inputExpId = o.addToGraph(inputExp);
		double inputCard = o.getMetadata(inputExpId.expId).getExpectedCardinality();

		double relationCard = rt.getRelationMetadata(relation).getExpectedCardinality();


		if (scanLoc.isFullyReplicated()) {
			SystemCalibration sc;
			if (l.isDistributed()) {
				sc = localCalibration;
			} else {
				sc = getCalibration(l);				
			}
			LocalCost<Double,Location,S,QueryPlan<M>> lc;
			double cost = PER_OPERATOR_COST + sc.getVersionedScanCost(relationCard, inputCard / relationCard, inputCard);
			if (sp.remainingPred != null) {
				cost += sc.getPredEvalTime(inputCard);
			}


			if (l.isDistributed()) {
				// Must compute at central node and rehash
				// TODO: compare with broadcast?
				if (! e.head.containsAll(l.getHashVars())) {
					// Can't rehash
					return null;
				}
				lc = new ReplicatedScan(cost, Location.CENTRALIZED, sp, baseSchema, baseVarPos);
				double shipCost = getShipCost(Location.CENTRALIZED, l, expId, o);
				lc = new ShipLocalCost(shipCost, e.head, Location.CENTRALIZED, l, lc);
			} else {
				// Otherwise is named, centralized or replicated and we can just scan
				lc = new ReplicatedScan(cost, l, sp, baseSchema, baseVarPos);
			}
			return lc;
		} else if (l.isFullyReplicated()) {
			// Cannot get a fully replicated table from a striped one
			return null;
		}


		int keySize = keyTupleOverheadBytes;
		for (int col : baseSchema.getKeyColsSet()) {
			keySize += baseSchema.getColType(col).getOptimizerType().getExpectedSize() + tupleOverheadPerFieldBytes;
		}


		// Cost of owner node requesting and receiving list of page IDs
		// + the cost of requesting a page of keys be sent
		// + the cost of retrieving and filtering that page
		// + the cost of sending a tuple request
		double startCost = PER_OPERATOR_COST + 3 * localBandwidth.getTransmissionTime(msgOverheadBytes) +
		remoteCalibration.getIndexLookupCost(indexPageSize) + remoteCalibration.getKeyTupleDeserializationTime(indexPageSize) +
		remoteBandwidth.getTransmissionTime(keySize + msgOverheadBytes);

		double numIndexPages = relationCard / indexPageSize;
		double indexPagesPerNode = Math.ceil(relationCard / (indexPageSize * numNodes));
		// Cost of a index node sending all of its matching tuples for this relation
		double remoteCost = remoteCalibration.getIndexLookupCost(indexPageSize) * indexPagesPerNode +
		remoteBandwidth.getTransmissionTime((keySize + msgOverheadBytes) * relationCard / numNodes) +
		remoteCalibration.getIdComputationTime(indexPagesPerNode * indexPageSize) +
		remoteCalibration.getMsgSendTime(indexPagesPerNode * indexPageSize);

		if (sp.keyPred != null) {
			startCost += remoteCalibration.getPredEvalTime(indexPageSize);
			remoteCost += remoteCalibration.getPredEvalTime(relationCard / numNodes);
		}


		boolean canShip = (! (l.isDistributed() && (! scanLoc.equals(l)) && (! e.head.containsAll(l.getHashVars()))));

		boolean canIndexScan = (sp.remainingPred == null);

		if (canIndexScan) {
			HEAD: for (Variable v : e.head) {
				List<Variable> vars;
				if (v instanceof AtomVariable) {
					vars = Collections.singletonList(v);
				} else if (v instanceof EquivClass) {
					EquivClass ec = (EquivClass) v;
					vars = new ArrayList<Variable>();
					for (Variable vv : ec) {
						vars.add(vv);
					}
				} else {
					throw new IllegalStateException("Should not find variable of type " + v.getClass().getName() + " in scan");
				}
				for (Variable vv : vars) {
					AtomVariable av;
					if (vv instanceof AtomVariable) {
						av = (AtomVariable) vv;
					} else {
						throw new IllegalStateException("Should not find variable of type " + v.getClass().getName() + " in scan");
					}
					if (keyCols.contains(av.position)) {
						continue HEAD;
					}
				}
				// Variable in head is not a key column and therefore not in the index
				canIndexScan = false;
				break HEAD;
			}
		}

		if (canIndexScan) {
			if (l.isDistributed() && (! e.head.containsAll(l.getHashVars()))) {
				// Can't ship there
				canIndexScan = false;
			}
		}

		if (canIndexScan) {
			SystemCalibration evalCalibration;
			final Location evalLoc;
			if (l.isNamedLocation()) {
				evalCalibration = namedCalibration.get(l.getLocationName());
				evalLoc = l;
			} else {
				// We'll compute at the central node and ship
				evalCalibration = localCalibration;
				evalLoc = Location.CENTRALIZED;
			}

			remoteCost = remoteCalibration.getIndexLookupCost(indexPageSize) * indexPagesPerNode +
			remoteCalibration.getMsgSendTime(indexPagesPerNode);
			double localCost = evalCalibration.getMsgDeliverTime(numIndexPages) + evalCalibration.getPredEvalTime(relationCard) + evalCalibration.getMsgSendTime(numIndexPages + 1) +
			evalCalibration.getKeyTupleDeserializationTime(relationCard);
			if (evalLoc.isNamedLocation()) {
				remoteCost += remoteBandwidth.getTransmissionTime(indexPagesPerNode * (indexPageSize * keySize + msgOverheadBytes), evalLoc.getLocationName());
			} else {
				remoteCost += remoteBandwidth.getTransmissionTime(indexPagesPerNode * (indexPageSize * keySize + msgOverheadBytes));
			}


			double cost = (remoteCost > localCost ? remoteCost : localCost) + PER_OPERATOR_COST;

			double totalCost = cost;
			double shipCost = 0.0;
			if (l.isDistributed()) {
				shipCost = getShipCost(evalLoc, l, expId, o);
				totalCost += shipCost;
			}

			if (totalCost < retvalCost) {
				LocalCost<Double,Location,S,QueryPlan<M>> indexScan = new LocalCost<Double,Location,S,QueryPlan<M>>(
						cost, noIds, noLocs) {

					@Override
					CreatedQP<S, QueryPlan<M>, Double> createQP(
							OperatorIdSource ois,
							SchemaFactory<? extends S> schemaFactory,
							List<CreatedQP<S, QueryPlan<M>, Double>> inputs) {
						if (! inputs.isEmpty()) {
							throw new IllegalArgumentException("IndexScan should not be given any input query plans");
						}

						S newSchema = schemaFactory.createNewSchema();
						VariablePosition varPos = fillInSchema(e.head, keyVars, newSchema);
						VariablePosition baseWithECs = baseVarPos.updateWithEcs(e.findEquivClass);
						VariablePosition outputMapping = varPos.updateWithEcs(e.findEquivClass);
						Map<Integer,Integer> newSchemaMapping = new HashMap<Integer,Integer>(varPos.size());
						for (int i = 0; i < varPos.size(); ++i) {
							newSchemaMapping.put(i, baseWithECs.getPos(varPos.getVariable(i)));
						}
						
						QueryPlan<M> qp = new ScanNode<M>(baseSchema, newSchema, newSchemaMapping, sp.keyPred, null, evalLoc.getPlanLocation(), ois.getOperatorId(), IndexScan);
						qp.setCost(localCost);
						return new CreatedQP<S,QueryPlan<M>,Double>(qp, newSchema, localCost, outputMapping);
					}

					@Override
					public String toString() {
						return "IndexScanNode";
					}

					@Override
					public Location getOutputProperties() {
						return Location.CENTRALIZED;
					}
				};

				if (! evalLoc.equals(l)) {
					retval = new ShipLocalCost(shipCost, e.head, evalLoc, l, indexScan);
				} else {
					retval = indexScan;
				}
				retvalCost = totalCost;
			}
		}

		if (! canShip) {
			return null;
		}


		double keyPredSelectivity = inputCard / relationCard;
		double filteredIndexPageSize = indexPageSize * keyPredSelectivity / numNodes;
		
		double indexCost = remoteCalibration.getMsgSendTime(indexPagesPerNode * numNodes) + remoteCalibration.getMsgDeliverTime(indexPagesPerNode * numNodes) +
			indexPagesPerNode * (remoteCalibration.getIndexLookupCost(indexPageSize) + remoteCalibration.getIdComputationTime(indexPageSize) + remoteCalibration.getKeyTupleDeserializationTime(indexPageSize)) +
			remoteBandwidth.getTransmissionTime(indexPagesPerNode * filteredIndexPageSize * keySize / numNodes);
		
		double startSendingKeysCost = localCalibration.getMsgSendTime(numIndexPages) + remoteBandwidth.getTransmissionTime(msgOverheadBytes);
		
		// Cost of doing the distributed part of a distributed scan
		double dsDistributedCost = remoteCalibration.getMsgDeliverTime(numIndexPages) + remoteCalibration.getIndexedScanCost(inputCard / numNodes, card / numNodes);
		// Cost of doing the distributed part of a probe scan
		double psDistributedCost = remoteCalibration.getMsgDeliverTime(numIndexPages) + remoteCalibration.getProbeCost(inputCard / numNodes) + remoteCalibration.getFullTupleDeserializationTime(inputCard / numNodes);
		
		if (sp.keyPred != null) {
			indexCost += remoteCalibration.getPredEvalTime(indexPageSize * indexPagesPerNode);
		}

		if (sp.remainingPred != null) {
			double evalPredCost = remoteCalibration.getPredEvalTime(inputCard / numNodes); 
			dsDistributedCost += evalPredCost;
			psDistributedCost += evalPredCost;
		}

		double dsCost = PER_OPERATOR_COST + startSendingKeysCost + indexCost + dsDistributedCost;
		double psCost = PER_OPERATOR_COST + startSendingKeysCost + indexCost + psDistributedCost;
		double shipCost = 0.0;
		double dsTotalCost = dsCost;
		double psTotalCost = psCost;
		if (! scanLoc.equals(l)) {
			shipCost = getShipCost(scanLoc, l, expId, o);
			dsTotalCost += shipCost;
			psTotalCost += shipCost;
		}

		if (dsCost < psCost && dsCost < retvalCost) {
			LocalCost<Double,Location,S,QueryPlan<M>> distributedScan = new LocalCost<Double,Location,S,QueryPlan<M>>(dsCost, noIds, noLocs) {

				@Override
				CreatedQP<S, QueryPlan<M>, Double> createQP(
						OperatorIdSource ois,
						SchemaFactory<? extends S> schemaFactory,
						List<CreatedQP<S, QueryPlan<M>, Double>> inputs) {
					if (! inputs.isEmpty()) {
						throw new IllegalArgumentException("Should never give inputs to a scan");
					}

					VariablePosition outputMapping = baseVarPos.updateWithEcs(e.findEquivClass);
					QueryPlan<M> qp = new ScanNode<M>(baseSchema, sp.keyPred, sp.remainingPred, scanLoc.getPlanLocation(), ois.getOperatorId(), DistributedScan);
					qp.setCost(localCost);
					return new CreatedQP<S,QueryPlan<M>,Double>(qp, baseSchema, localCost, outputMapping);
				}

				@Override
				public String toString() {
					return "DistributedScanNode";
				}

				@Override
				public Location getOutputProperties() {
					if (! scanLoc.isDistributed()) {
						return scanLoc;
					}
					Set<Variable> locVars = new HashSet<Variable>();
					for (Variable v : scanLoc.getHashVars()) {
						if (e.findEquivClass.containsKey(v)) {
							locVars.add(e.findEquivClass.get(v));
						} else {
							locVars.add(v);
						}
					}

					return new Location(locVars);
				}
			};

			if (scanLoc.equals(l)) {
				retval = distributedScan;
			} else {
				retval = new ShipLocalCost(shipCost, e.head, scanLoc, l, distributedScan);
			}

			retvalCost = dsTotalCost;
		} else if (psCost < retvalCost) {
			LocalCost<Double,Location,S,QueryPlan<M>> probeScan = new LocalCost<Double,Location,S,QueryPlan<M>>(
					psCost, noIds, noLocs) {


				CreatedQP<S,QueryPlan<M>,Double> createQP(OperatorIdSource ois,
						SchemaFactory<? extends S> schemaFactory,
						List<CreatedQP<S,QueryPlan<M>,Double>> inputs) {


					QueryPlan<M> qp = new ScanNode<M>(baseSchema, sp.keyPred, sp.remainingPred, rt.getRelationProps(relation).getPlanLocation(), ois.getOperatorId(), DistributedProbe);
					qp.setCost(localCost);

					VariablePosition outputMapping = baseVarPos.updateWithEcs(e.findEquivClass);
					return new CreatedQP<S,QueryPlan<M>,Double>(qp, baseSchema, localCost, outputMapping);
				}

				@Override
				public String toString() {
					return "ProbeScanNode";
				}

				@Override
				public Location getOutputProperties() {
					return l;
				}
			};
			if (scanLoc.equals(l)) {
				retval = probeScan;
			} else {
				retval = new ShipLocalCost(shipCost, e.head, scanLoc, l, probeScan);
			}
			retvalCost = psTotalCost;
		}

		return retval;
	}

	private static double getSendCost(SystemCalibration from, SystemCalibration to, double numSendMsgs, double numReceiveMsgs) {
		double firstCost = from.getMsgSendTime(1.0) + to.getMsgDeliverTime(1.0); 
		double sendCost = 0.0, receiveCost = 0.0;
		if (numReceiveMsgs > 1.0) {
			receiveCost = to.getMsgDeliverTime(numReceiveMsgs - 1.0);
		}
		if (numSendMsgs > 1.0) {
			sendCost = from.getMsgSendTime(numSendMsgs - 1.0);
		}

		if (sendCost > receiveCost) {
			return firstCost + sendCost;
		} else {
			return firstCost + receiveCost;
		}
	}

	private double getShipCost(Location from, Location to, int expId, Optimizer<? extends Location,?,?,?> o) {
		if (from.isFullyReplicated() || to.isFullyReplicated()) {
			throw new IllegalArgumentException("Should not be shipping to or from a fully replicated location");
		}

		int tupleSize = fullTupleOverheadBytes;
		Expression e = o.getExpressionForId(expId);
		if (! to.isValidFor(e)) {
			throw new IllegalArgumentException("Destination " + to + " is not valid for expression (id " + expId + "): " + e);
		}
		for (Variable v : e.getExposedVariables()) {
			tupleSize += v.getType().getExpectedSize() + tupleOverheadPerFieldBytes;
		}
		final RelationMetadata md = o.getMetadata(expId);

		// TODO: deal properly with reaggregation here

		if (to.isCentralized()) {
			if (from.isNamedLocation()) {
				final double card = md.cardinality;
				final double numMsgs = card / tuplesPerInterval;
				SystemCalibration fromCalibration = namedCalibration.get(from.getLocationName());
				return localBandwidth.getTransmissionTime(tupleSize * card + numMsgs * msgOverheadBytes, from.getLocationName()) +
				getSendCost(fromCalibration, localCalibration, numMsgs, numMsgs);
			} else {
				final double card = md.getPerNodeCardinality(numNodes, from.getHashVars());
				final double numMsgs = card / tuplesPerInterval;
				final double numDistributedMsgs = numMsgs / numNodes;
				return localBandwidth.getTransmissionTime(tupleSize * card / numNodes + numDistributedMsgs * msgOverheadBytes) +
				getSendCost(remoteCalibration, localCalibration, numDistributedMsgs, numMsgs);
			}
		}
		if (from.isDistributed()) {
			final double card = md.getPerNodeCardinality(numNodes, from.getHashVars());
			final double numMsgs = card / tuplesPerInterval;
			final double numDistributedMsgs = numMsgs / numNodes;
			if (to.isDistributed()) {				
				return remoteBandwidth.getTransmissionTime(tupleSize * card + numMsgs * msgOverheadBytes) +
				remoteCalibration.getIdComputationTime(card / numNodes) + 
				getSendCost(remoteCalibration, remoteCalibration, numMsgs, numMsgs);
			} else {
				// to.isNamedLocation() is true
				SystemCalibration destCalibration = namedCalibration.get(to.getLocationName());
				return namedBandwidth.get(to.getLocationName()).getTransmissionTime(card * tupleSize / numNodes + numDistributedMsgs * msgOverheadBytes) +
				getSendCost(remoteCalibration, destCalibration, numDistributedMsgs, numMsgs);
			}
		} else {
			final double card = md.cardinality;
			final double numMsgs = card / tuplesPerInterval;
			Bandwidth b;
			SystemCalibration fromSc, toSc;
			if (from.isCentralized()) {
				b = localBandwidth;
				fromSc = localCalibration;
			} else {
				// from.isNamedLocation() is true
				b = namedBandwidth.get(from.getLocationName());
				fromSc = namedCalibration.get(from.getLocationName());
			}
			double cost = 0.0;
			if (to.isDistributed()) {
				toSc = remoteCalibration;
				cost = fromSc.getIdComputationTime(card) + getSendCost(fromSc, toSc, numMsgs, numMsgs);

			} else {
				// to.isNamedLocation() is true
				toSc = namedCalibration.get(to.getLocationName());
				cost = getSendCost(fromSc, toSc, numMsgs, numMsgs);
			}
			return cost + b.getTransmissionTime(card * tupleSize + numMsgs * msgOverheadBytes);

		}
	}

	private static VariablePosition fillInSchema(Set<? extends Variable> vars, Set<? extends Variable> primaryKey, QpSchema empty) {
		int count = 0;
		VariablePosition vp = new VariablePosition(vars.size());
		Set<Variable> pkColsLeft;
		Set<String> keyColNames;
		if (primaryKey == null) {
			pkColsLeft = Collections.emptySet();
			keyColNames = Collections.emptySet();
		} else {
			pkColsLeft = new HashSet<Variable>(primaryKey);
			keyColNames = new HashSet<String>(primaryKey.size());
		}
		try {
			for (Variable v : vars) {
				String colName = "C" + count; 
				empty.addCol(colName, v.toString(), v.getType().getExecutionType());
				vp.addVariable(v);
				if (pkColsLeft.contains(v)) {
					keyColNames.add(colName);
					pkColsLeft.remove(v);
				}
				count++;
			}
		} catch (BadColumnName e) {
			throw new RuntimeException(e);
		}
		vp.finish();
		if (! pkColsLeft.isEmpty()) {
			// We've lost some of the columns from the primary key, so there
			// no longer is one
			try {
				empty.setPrimaryKey(keyColNames);
			} catch (UnknownRefFieldException e) {
				// Since we're generating the column names this should not happen
				throw new RuntimeException(e);
			}
		}
		empty.markFinishedExceptLocation();

		return vp;
	}

	private class PredicateCost extends LocalCost<Double,Location,S,QueryPlan<M>> {
		private final LocalCost<Double,Location,S,QueryPlan<M>> input;
		private final Location outputLoc;
		private final Set<Predicate> predicates;
		private final Expression e;

		PredicateCost(double localCost,	Location outputLoc, Set<Predicate> predicates,
				LocalCost<Double,Location,S,QueryPlan<M>> input,
				Expression e) {
			super(localCost + input.localCost, input.inputs);
			this.input = input;
			this.outputLoc = outputLoc;
			this.predicates = predicates;
			this.e = e;
		}

		CreatedQP<S,QueryPlan<M>,Double> createQP(
				OperatorIdSource ois,
				SchemaFactory<? extends S> schemaFactory,
				List<CreatedQP<S,QueryPlan<M>,Double>> inputs) {

			if (inputs.size() != 1) {
				throw new IllegalArgumentException("Should be given 1 table entry");
			}


			CreatedQP<S,QueryPlan<M>,Double> inputQP = input.createQP(ois, schemaFactory, inputs);
			VariablePosition varPos = inputQP.varPos;
			S schema = inputQP.schema;


			List<edu.upenn.cis.orchestra.predicate.Predicate> preds = new ArrayList<edu.upenn.cis.orchestra.predicate.Predicate>(predicates.size());
			for (Predicate p : predicates) {
				edu.upenn.cis.orchestra.predicate.Predicate cp = null;
				try {
					if (p.var1 instanceof LiteralVariable) {
						LiteralVariable lv = (LiteralVariable) p.var1;
						if (p.op == Op.EQ || p.op == Op.NE) {
							cp = new EqualityPredicate(varPos.getPos(p.var2), lv.lit, schema, p.op == Op.NE);
						} else {
							cp = ComparePredicate.createColLit(schema, varPos.getPos(p.var2), p.op.getReverseComparePredicateOp(), lv.lit);
						}
					} else if (p.var2 instanceof LiteralVariable) {
						LiteralVariable lv = (LiteralVariable) p.var2;
						if (p.op == Op.EQ || p.op == Op.NE) {
							cp = new EqualityPredicate(varPos.getPos(p.var1), lv.lit, schema, p.op == Op.NE);
						} else {
							cp = ComparePredicate.createColLit(schema, varPos.getPos(p.var1), p.op.getComparePredicateOp(), lv.lit);
						}
					} else {
						if (p.op == Op.EQ || p.op == Op.NE) {
							cp = new EqualityPredicate(schema, varPos.getPos(p.var1), varPos.getPos(p.var2), p.op == Op.NE);
						} else {
							cp = ComparePredicate.createTwoCols(schema, varPos.getPos(p.var1), p.op.getComparePredicateOp(), varPos.getPos(p.var2));
						}
					}
				} catch (PredicateMismatch pm) {
					throw new RuntimeException(pm);
				} catch (PredicateLitMismatch plm) {
					throw new RuntimeException(plm);
				}
				preds.add(cp);
			}

			QueryPlan<M> outputQP = new FilterNode<M>(schema, preds, outputLoc.getPlanLocation(), ois.getOperatorId(), inputQP.qp);
			outputQP.setCost(localCost);
			VariablePosition outputVarPos = varPos.updateWithEcs(e.findEquivClass);
			return new CreatedQP<S,QueryPlan<M>,Double>(outputQP, schema, inputQP.cost + this.localCost, outputVarPos);
		}

		@Override
		public String toString() {
			return "PredicateNode";
		}

		@Override
		public Location getOutputProperties() {
			return outputLoc;
		}

	}


	private class ShipLocalCost extends LocalCost<Double,Location,S,QueryPlan<M>> {
		private final Location destLoc;
		private final Location currLoc;
		// Query plan node that feeds directly into the ship node
		private final LocalCost<Double,Location,S,QueryPlan<M>> inputFrom;
		private final Set<Variable> head;

		ShipLocalCost(double shipCost, Set<Variable> head, Location currLoc, Location destLoc,
				LocalCost<Double,Location,S,QueryPlan<M>> inputFrom) {
			super(shipCost + inputFrom.localCost, inputFrom.inputs);
			this.currLoc = currLoc;
			this.destLoc = destLoc;

			if (destLoc.isDistributed() && (! head.containsAll(destLoc.getHashVars()))) {
				throw new IllegalArgumentException("Expression head must contain all hash variables");
			}

			this.head = head;


			this.inputFrom = inputFrom;
		}

		CreatedQP<S,QueryPlan<M>,Double> createQP(OperatorIdSource ois,
				SchemaFactory<? extends S> schemaFactory,
				List<CreatedQP<S,QueryPlan<M>,Double>> inputs) {

			CreatedQP<S,QueryPlan<M>,Double> inputQp;
			inputQp = inputFrom.createQP(ois, schemaFactory, inputs);

			Map<Integer,Integer> schemaMapping = null;
			final S outputSchema;
			final VariablePosition varPos;
			Set<Variable> inputExposedVars = new HashSet<Variable>();
			inputExposedVars.addAll(inputQp.varPos.getVariables());
			if (inputQp.schema.getLocation() == null && inputExposedVars.equals(head)) {
				// Can set location of input schema
				// and input schema doesn't contain any extra columns
				outputSchema = inputQp.schema;
				varPos = inputQp.varPos;
			} else {
				outputSchema = schemaFactory.createNewSchema();
				Set<Variable> pk = new HashSet<Variable>();
				for (int col : inputQp.schema.getKeyColsSet()) {
					pk.add(inputQp.varPos.getVariable(col));
				}
				varPos = fillInSchema(head, pk, outputSchema);
				schemaMapping = new HashMap<Integer,Integer>();
				for (Variable v : head) {
					int fromPos = inputQp.varPos.getPos(v);
					int toPos = varPos.getPos(v);
					schemaMapping.put(fromPos, toPos);
				}
			}
			if (destLoc.isCentralized()) {
				outputSchema.setCentralized();
			} else if (destLoc.isNamedLocation()) {
				outputSchema.setNamedLocation(destLoc.getLocationName());
			} else {
				Set<Variable> hashVars = destLoc.getHashVars();
				int numCols = hashVars.size();
				int[] cols = new int[numCols];
				int pos = 0;
				for (Variable v : hashVars) {	
					Integer col = varPos.getPos(v);
					if (col == null) {
						throw new IllegalArgumentException("Missing variable " + v + " from input schema " + inputQp.schema);
					}
					cols[pos++] = col;
				}
				outputSchema.setHashCols(cols);
			}
			outputSchema.markFinished();
			QueryPlan<M> outputQp = new ShipNode<M>(inputQp.schema, schemaMapping, outputSchema, currLoc.getPlanLocation(), ois.getOperatorId(), inputQp.qp);
			outputQp.setCost(localCost - inputFrom.localCost);
			CreatedQP<S,QueryPlan<M>,Double> shipQp = new CreatedQP<S,QueryPlan<M>,Double>(outputQp, outputSchema, localCost + inputQp.cost, varPos);
			return shipQp;
		}


		@Override
		public String toString() {
			return inputFrom.toString() + "->ShipNode";
		}

		@Override
		public Location getOutputProperties() {
			return destLoc;
		}
	}

	private SplitPredicate convertToPredicatesForScan(Collection<Predicate> preds,
			Collection<EquivClass> equivClasses,
			S schema, VariablePosition mapping) {

		List<Predicate> convertedPreds = new ArrayList<Predicate>(preds.size());

		Set<Integer> keyCols = schema.getKeyColsSet();

		try {
			for (Predicate p : preds) {
				Variable var1, var2;
				boolean changed = false;
				if (p.var1 instanceof EquivClass) {
					Variable rep = null;
					for (Variable v : ((EquivClass) p.var1)) {
						rep = v;
						if (keyCols.contains(mapping.getPos(v))) {
							break;
						}
					}
					changed = true;
					var1 = rep;
				} else {
					var1 = p.var1;
				}
				if (p.var2 instanceof EquivClass) {
					Variable rep = null;
					for (Variable v : ((EquivClass) p.var2)) {
						rep = v;
						if (keyCols.contains(mapping.getPos(v))) {
							break;
						}
					}
					changed = true;
					var2 = rep;
				} else {
					var2 = p.var2;
				}

				if (changed) {
					convertedPreds.add(new Predicate(var1, p.op, var2));
				} else {
					convertedPreds.add(p);
				}
			}
		} catch (Type.TypeError te) {
			throw new RuntimeException("Shouldn't get type error when converting predicates", te);
		}


		for (EquivClass ec : equivClasses) {
			Type t = ec.getType();
			Variable rep = null;
			if (t.valueKnown()) {
				rep = new LiteralVariable(t.getConstantValue());
			}
			for (Variable v : ec) {
				if (rep == null) {
					rep = v;
				} else if (! v.getType().valueKnown()) {
					try {
						convertedPreds.add(new Predicate(v, Predicate.Op.EQ, rep));
					} catch (TypeError e) {
						// Shouldn't happen since they're already in the same equiv class
						throw new RuntimeException(e);
					}
				}
			}
		}

		edu.upenn.cis.orchestra.predicate.Predicate keyPred = null, allPred = null;

		Set<Predicate> keyPreds = new HashSet<Predicate>(), otherPreds = new HashSet<Predicate>();

		try {
			for (Predicate p : convertedPreds) {
				boolean isKey = true;
				edu.upenn.cis.orchestra.predicate.Predicate newPred;
				if (p.var1 instanceof AtomVariable) {
					if (! keyCols.contains(mapping.getPos(p.var1))) {
						isKey = false;
					}
					if (p.var2 instanceof AtomVariable) {
						if (! keyCols.contains(mapping.getPos(p.var2))) {
							isKey = false;
						}
						if (p.op == Op.EQ || p.op == Op.NE) {
							newPred = new EqualityPredicate(schema, mapping.getPos(p.var1), mapping.getPos(p.var2), p.op == Op.NE);
						} else {
							newPred = ComparePredicate.createTwoCols(schema, mapping.getPos(p.var1), p.op.getComparePredicateOp(), mapping.getPos(p.var2));
						}
					} else if (p.var2 instanceof LiteralVariable) {
						LiteralVariable lv2 = (LiteralVariable) p.var2;
						if (p.op == Op.EQ || p.op == Op.NE) {
							newPred = new EqualityPredicate(mapping.getPos((AtomVariable) p.var1), lv2.lit, schema, p.op == Op.NE);
						} else {
							newPred = ComparePredicate.createColLit(schema, mapping.getPos((AtomVariable) p.var1), p.op.getComparePredicateOp(), lv2.lit);
						}
					} else {
						throw new RuntimeException("Should not find predicate " + p + " in a scan");
					}
				} else if (p.var1 instanceof LiteralVariable) {
					LiteralVariable lv1 = (LiteralVariable) p.var1;
					if (p.var2 instanceof AtomVariable) {
						if (! keyCols.contains(mapping.getPos(p.var2))) {
							isKey = false;
						}
						if (p.op == Op.EQ || p.op == Op.NE) {
							newPred = new EqualityPredicate(mapping.getPos((AtomVariable) p.var2), lv1.lit, schema, p.op == Op.NE);
						} else {
							newPred = ComparePredicate.createColLit(schema, mapping.getPos((AtomVariable) p.var2), p.op.getReverseComparePredicateOp(), lv1.lit);
						}
					} else {
						throw new RuntimeException("Should not find predicate " + p + " in a scan");
					}
				} else {
					throw new RuntimeException("Should not find predicate " + p + " in a scan");
				}
				if (isKey) {
					if (keyPred == null) {
						keyPred = newPred;
					} else {
						keyPred = new AndPred(newPred,keyPred);
					}
					keyPreds.add(p);
				} else {
					if (allPred == null) {
						allPred = newPred;
					} else {
						allPred = new AndPred(newPred,allPred);
					}
					otherPreds.add(p);
				}
			}
		} catch (PredicateMismatch pm) {
			throw new RuntimeException(pm);
		} catch (PredicateLitMismatch plm) {
			throw new RuntimeException(plm);
		}
		return new SplitPredicate(keyPred, allPred, keyPreds, otherPreds);
	}

	private static class SplitPredicate {
		final edu.upenn.cis.orchestra.predicate.Predicate keyPred, remainingPred;
		final Set<Predicate> keyPreds, otherPreds;

		SplitPredicate(edu.upenn.cis.orchestra.predicate.Predicate keyPred,
				edu.upenn.cis.orchestra.predicate.Predicate remainingPred,
				Set<Predicate> keyPreds, Set<Predicate> otherPreds) {
			this.keyPred = keyPred;
			this.remainingPred = remainingPred;
			this.keyPreds = keyPreds;
			this.otherPreds = otherPreds;
		}
	}

	public boolean takeMaxOfMultipleInputs() {
		return true;
	}

	private SystemCalibration getCalibration(Location l) {
		if (l.isCentralized()) {
			return localCalibration;
		} else if (l.isDistributed() || l.isFullyReplicated()) {
			return remoteCalibration;
		} else if (l.isNamedLocation()) {
			SystemCalibration sc = namedCalibration.get(l.getLocationName());
			if (sc == null) {
				throw new IllegalArgumentException("Missing calibration for location with name " + l.getLocationName());
			}
			return sc;
		} else {
			throw new IllegalArgumentException("Don't know how to find calibration for location " + l);
		}
	}

	public CreatedQP<S, QueryPlan<M>, Double> createQueryRoot(List<Variable> head,
			S headSchema,
			SchemaFactory<? extends S> schemaFactory,
			OperatorIdSource ois,
			Location dest, CreatedQP<S, QueryPlan<M>, Double> input) {

		int[] inputPos = new int[head.size()];

		S schema = input.schema;
		boolean mustReorder = headSchema != null;
		if (head.size() != schema.getNumCols()) {
			mustReorder = true;
		}
		int pos = 0;
		for (Variable v : head) {
			inputPos[pos] = input.varPos.getPos(v);
			if (inputPos[pos] != pos) {
				mustReorder = true;
			}
			pos++;
		}

		QueryPlan<M> qp = input.qp;
		VariablePosition vp = input.varPos;
		if (mustReorder) {
			vp = new VariablePosition(inputPos.length);
			if (headSchema == null) {
				schema = schemaFactory.createNewSchema();
			} else {
				schema = headSchema;
			}
			try {
				for (int i = 0; i < inputPos.length; ++i) {
					if (headSchema == null) {
						schema.addCol("C" + i, input.schema.getColType(inputPos[i]));
					}
					vp.addVariable(head.get(i));
				}
			} catch (BadColumnName e) {
				throw new RuntimeException(e);
			}
			if (headSchema == null) {
				schema.markFinished();
			}
			vp.finish();
			qp = new edu.upenn.cis.orchestra.p2pqp.plan.ProjectNode<M>(input.schema, inputPos, schema, CentralizedLoc.getInstance(), ois.getOperatorId(), qp);
		}

		QueryPlan<M> spool = new SpoolNode<M>(schema, ois.getOperatorId(), qp);
		return new CreatedQP<S,QueryPlan<M>,Double>(spool, schema, input.cost, vp);
	}
	
	public void setExpectedCard(QueryPlan<M> plan, double card) {
		plan.setCard(card);
		if (plan instanceof ShipNode) {
			((ShipNode<M>) plan).input.setCard(card);
		}
	}
}
