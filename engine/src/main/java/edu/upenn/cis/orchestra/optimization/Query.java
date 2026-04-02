package edu.upenn.cis.orchestra.optimization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import com.experlog.zql.ZConstant;
import com.experlog.zql.ZExp;
import com.experlog.zql.ZExpression;
import com.experlog.zql.ZFromItem;
import com.experlog.zql.ZGroupBy;
import com.experlog.zql.ZQuery;
import com.experlog.zql.ZSelectItem;

import edu.upenn.cis.orchestra.optimization.Type.TypeError;
import edu.upenn.cis.orchestra.util.ReadOnlyList;

public class Query {
	public static class SyntaxError extends Exception {
		private static final long serialVersionUID = 1L;

		SyntaxError(String error) {
			super("Syntax error: " + error);
		}
	}

	public final Expression exp;
	public final List<Variable> head;

	private Query(Expression exp, List<Variable> head) {
		this.exp = exp;
		this.head = head;
	}

	@SuppressWarnings("deprecation")
	public Query(ZQuery zq, RelationTypes<?,?> rt)
	throws TypeError, SyntaxError {

		Map<String,Integer> relAtoms = new HashMap<String,Integer>();
		Set<EquivClass> equivClasses = new HashSet<EquivClass>();

		Vector<ZSelectItem> select = zq.getSelect();
		Vector<ZFromItem> from = zq.getFrom();
		ZExp where = zq.getWhere();
		ZGroupBy zGroupBy = zq.getGroupBy();

		// Column names that appear only in element of the from clause
		Set<String> uniqueColNames = new HashSet<String>();
		// Column names that appear in several elements of the from clause
		Set<String> duplicateColNames = new HashSet<String>();

		// Mapping from variable names (including name/alias) to variable objects,
		// i.e. 'R.a' -> [R,1,0] or 'a' -> [R,1,0]
		Map<String,AtomVariable> findAtom = new HashMap<String,AtomVariable>();

		for (ZFromItem zfe : from) {
			String table = zfe.getTable();
			table = table.toUpperCase();
			Integer count = relAtoms.get(table);
			if (count == null) {
				count = 1;
			} else {
				count = count + 1;
			}
			relAtoms.put(table, count);
			String[] colNames = rt.getColumnNames(table);
			for (String col : colNames) {
				col = col.toLowerCase();
				if (duplicateColNames.contains(col)) {
					continue;
				} else if (uniqueColNames.contains(col)) {
					uniqueColNames.remove(col);
					duplicateColNames.add(col);
				} else {
					uniqueColNames.add(col);
				}
			}
		}

		Map<String,Integer> relCount = new HashMap<String,Integer>();

		for (ZFromItem zfe : from) {
			String table = zfe.getTable();
			table = table.toUpperCase();
			String alias = zfe.getAlias();
			if ((relAtoms.get(table) > 1) && (alias == null)) {
				throw new SyntaxError("multiple occurrences of the same table require unique names");
			}
			Integer count = relCount.get(table);
			if (count == null) {
				count = 1;
			} else {
				count = count + 1;
			}
			relCount.put(table, count);

			String[] cols = rt.getColumnNames(table);
			for (int i = 0; i < cols.length; ++i) {
				String colName = cols[i].toLowerCase();
				AtomVariable av = new AtomVariable(table, count, i, rt);
				if (uniqueColNames.contains(colName)) {
					findAtom.put(colName, av);
				}
				String tableName = (alias == null) ? table : alias.toUpperCase();
				findAtom.put(tableName + "." + colName, av);
			}
		}


		// Index into equivClasses by variable name
		Map<Variable,EquivClass> findEquivClass = new HashMap<Variable,EquivClass>();

		Set<Predicate> predicates;

		if (where == null) {
			predicates = new HashSet<Predicate>();
		} else {
			predicates = decodeExp(where, findEquivClass, findAtom);
		}

		// Process the group by and having clauses
		Set<Variable> groupBy = new HashSet<Variable>();
		if (zGroupBy != null) {
			for (ZExp ze : zGroupBy.getGroupBy()) {
				if (!(ze instanceof ZConstant)) {
					throw new SyntaxError("Cannot group by " + ze);
				}
				
				String suppliedVar = ((ZConstant) ze).getValue(); 
				int dotIndex = suppliedVar.indexOf('.');
				String varName;
				if (dotIndex == -1) {
					varName = suppliedVar.toLowerCase();
				} else {
					String tableName = suppliedVar.substring(0, dotIndex).toUpperCase();
					String fieldName = suppliedVar.substring(dotIndex + 1).toLowerCase();
					varName = tableName + "." + fieldName;
				}
				
				
				AtomVariable groupByVar = findAtom.get(varName);
				if (groupByVar == null) {
					throw new SyntaxError("Cannot find variable " + varName + " to group by");
				} else {
					groupBy.add(groupByVar);
				}
			}

			ZExp having = zGroupBy.getHaving();
			if (having != null) {
				Set<Predicate> havingPreds = decodeExp(zGroupBy.getHaving(), findEquivClass,
						findAtom);
				for (Predicate p : havingPreds) {
					if (! p.var1.isAggregatedComputable(groupBy)) {
						throw new SyntaxError("Predicate " + p + " in having clause refers to non-grouping variable " + p.var1);
					}
					if (! p.var2.isAggregatedComputable(groupBy)) {
						throw new SyntaxError("Predicate " + p + " in having clause refers to non-grouping variable " + p.var2);
					}
				}
				predicates.addAll(decodeExp(zGroupBy.getHaving(), findEquivClass,
						findAtom));
			}
		}

		// Tidy up equivalence classes into canonical representations
		Set<Variable> ecVarCreated = new HashSet<Variable>();
		ECLOOP: for (EquivClass ec : findEquivClass.values()) {
			for (Variable v : ec) {
				if (ecVarCreated.contains(v)) {
					continue ECLOOP;
				}
			}
			ec.setFinished();
			equivClasses.add(ec);
			for (Variable v : ec) {
				for (Variable vv : ec) {
					if (v.equals(vv)) {
						continue;
					}
					if (v.usesVariable(vv)) {
						throw new IllegalArgumentException("Cannot deal with self-referencing equiv class " + ec);
					}
				}
				ecVarCreated.add(v);
			}
		}

		EquivClass.NormalizedEquivClasses nec = EquivClass.normalizeEquivClasses(equivClasses);
		equivClasses = nec.equivClasses;
		Map<Variable,EquivClass> findEC = nec.findECVar;

		Set<Predicate> fixedPreds = new HashSet<Predicate>(predicates.size());
		try {
			for (Predicate p : predicates) {
				Predicate pp = p.replaceVariable(findEC, false);
				fixedPreds.add(pp == null ? p : pp);
			}
		} catch (VariableRemoved vr) {
			throw new RuntimeException("Shouldn't catch VariableRemoved without null mapping", vr);
		}

		try {
			Set<Variable> newGroupBy = new HashSet<Variable>(groupBy.size());
			for (Variable v : groupBy) {
				Variable vv = v.replaceVariable(findEC, false);
				newGroupBy.add(vv == null ? v : vv);
			}
			groupBy = newGroupBy;
		} catch (VariableRemoved vr) {
			throw new RuntimeException("Shouldn't catch VariableRemoved without null mapping", vr);
		}

		
		ArrayList<Variable> head = new ArrayList<Variable>(select.size());

		for (ZSelectItem si : select) {
			ZExp ze = si.getExpression();
			if (ze == null) {
				// TODO: Add support for *, table.*
				throw new SyntaxError("Don't support 'SELECT *'");
			}
			Variable v = makeVariable(ze, findAtom);
			Variable vv;
			try {
				vv = v.replaceVariable(findEC, true);
			} catch (VariableRemoved vr) {
				throw new RuntimeException("Shouldn't catch VariableRemoved without null mapping", vr);
			}
			if (vv != null) {
				v = vv;
			}
			if (zGroupBy != null && (! v.isAggregatedComputable(groupBy))) {
				// Need to make sure variable doesn't refer to non-aggregate non-group by
				// variables
				throw new SyntaxError("Variable " + v + " in head refers to non-aggregated or non-grouping variable");
			}
			head.add(v);
		}

		this.head = ReadOnlyList.create(head);

		Set<Variable> used = new HashSet<Variable>();
		used.addAll(head);
		for (Variable v : head) {
			v.getInputVariables(used);
		}
		for (Variable v : equivClasses) {
			v.getInputVariables(used);
		}
		used.addAll(groupBy);
		for (Variable v : groupBy) {
			v.getInputVariables(used);
		}
		for (Predicate p : predicates) {
			used.add(p.var1);
			used.add(p.var2);
			p.var1.getInputVariables(used);
			p.var2.getInputVariables(used);
		}

		Set<Function> functions = new HashSet<Function>();
		Set<Aggregate> aggregates = new HashSet<Aggregate>();
		for (Variable v : used) {
			if (v instanceof Function) {
				functions.add((Function) v);
			}
			if (v instanceof Aggregate) {
				aggregates.add((Aggregate) v);
			}
		}


		if (groupBy.isEmpty() && aggregates.isEmpty()) {
				groupBy = null;
		}

		exp = new Expression(new HashSet<Variable>(head), relAtoms, equivClasses, groupBy, fixedPreds, functions, aggregates);
	}

	Set<Query> computePermutations(RelationTypes<?,?> rt) throws TypeError {
		List<Morphism> mappings = exp.computePermutationMappings();
		HashSet<Query> retval = new HashSet<Query>(mappings.size());
		for (Morphism mapping : mappings) {
			try {
				List<Variable> newHead = new ArrayList<Variable>(head.size());
				Expression newExp = exp.applyMorphism(mapping, rt);
				for (Variable v : head) {
					Variable newV = v.applyMorphism(mapping, rt);
					if (newV == null) {
						newHead.add(v);
					} else {
						newHead.add(newV);
					}
				}
				Query q = new Query(newExp, newHead);
				retval.add(q);
			} catch (VariableNotInMapping vnim) {
				throw new RuntimeException("Permutation did not map variable", vnim);
			}
		}

		return retval;
	}


	/**
	 * Get the type of a variable in the head of the query
	 * 
	 * @param pos		The position of the variable in the head of the query
	 * @return			The type, or <code>null</code> if the query has not
	 * 					been typechecked
	 * @throws IndexOutOfBoundsException
	 * 					if <code>pos</code> is out of range
	 */
	Type getType(int pos) throws IndexOutOfBoundsException {
		return head.get(pos).getType();
	}



	/**
	 * Decode an expression into a set of predicates
	 * 
	 * @param inputExp			The expression to decode
	 * @param findEquivClass	The mapping from variables to equivalence classes
	 * @return					The set of predicates
	 * @throws SyntaxError		if a syntax error or unsupported SQL fragment is
	 * 							encountered
	 * @throws TypeError		if a type error is encountered
	 */
	private static Set<Predicate> decodeExp(ZExp inputExp,
			Map<Variable,EquivClass> findEquivClass,
			Map<String,AtomVariable> findAtom)
			throws SyntaxError, TypeError {
		Set<Predicate> retval = new HashSet<Predicate>();
		if (! (inputExp instanceof ZExpression)) {
			throw new SyntaxError("Don't know how to process " + inputExp);
		}

		ZExpression inputExpression = (ZExpression) inputExp;

		List<ZExpression> inputExpressions = new ArrayList<ZExpression>();
		// We can only operate over a top-level conjuction with no lower-level
		// conjuctions or disjunctions (ZQL will create this the query is
		// conjunctive)
		if (inputExpression.getOperator().equalsIgnoreCase("AND")) {
			for (ZExp ze : inputExpression.getOperands()) {
				if (ze instanceof ZExpression) {
					inputExpressions.add((ZExpression) ze);
				} else {
					throw new SyntaxError("Don't know how to process condition " + ze);
				}
			}
		} else {
			inputExpressions.add(inputExpression);
		}

		// Decode the conjunction of predicates
		for (ZExpression ze : inputExpressions) {
			String operator = ze.getOperator();

			if (operator.equalsIgnoreCase("AND")) {
				throw new SyntaxError("Only support one level of conjunctions in conditions");
			} else if (operator.equalsIgnoreCase("OR")) {
				throw new SyntaxError("Do not support disjunction within queries");
			} else if (operator.equals("=") || operator.equals("<") || operator.equals("<=")
					|| operator.equals(">=") || operator.equals(">") || operator.equals("<>")) {
				ZExp l = ze.getOperand(0);
				ZExp r = ze.getOperand(1);
				Variable lVar = makeVariable(l, findAtom);
				Variable rVar = makeVariable(r, findAtom);

				// Decode equality expressions into equivalence classes
				// and other predicates into Predicates
				if (operator.equals("=")) {
					EquivClass lClass = findEquivClass.get(lVar);
					EquivClass rClass = findEquivClass.get(rVar);
					if (lClass != null && rClass != null) {
						if (lClass != rClass) {
							// Need to unify equivalence classes
							lClass.addAll(rClass);
							for (Variable v : rClass) {
								findEquivClass.put(v, lClass);
							}
						}
					} else if (lClass != null) {
						lClass.add(rVar);
						findEquivClass.put(rVar, lClass);
					} else if (rClass != null) {
						rClass.add(lVar);
						findEquivClass.put(lVar, rClass);
					} else {
						lClass = new EquivClass();
						findEquivClass.put(lVar, lClass);
						findEquivClass.put(rVar, lClass);
						lClass.add(lVar);
						lClass.add(rVar);
					}
				} else if (operator.equals("<")) {
					retval.add(new Predicate(lVar, Predicate.Op.LT, rVar));
				} else if (operator.equals("<=")) {
					retval.add(new Predicate(lVar, Predicate.Op.LE, rVar));
				} else if (operator.equals(">=")) {
					// Don't have >= predicate to make it easier to find
					// equivalent sets of predicates
					retval.add(new Predicate(rVar, Predicate.Op.LE, lVar));
				} else if (operator.equals(">")) {
					// Don't have > predicate to make it easier to find
					// equivalent sets of predicates
					retval.add(new Predicate(rVar, Predicate.Op.LT, lVar));
				} else if (operator.equals("<>")) {
					retval.add(new Predicate(lVar, Predicate.Op.NE, rVar));
				} else {
					throw new RuntimeException("Need to write code to handle predicate " + operator);
				}
			} else {
				throw new SyntaxError("Do not support operator " + operator);
			}
		}
		return retval;
	}

	private static Pattern allDigits = Pattern.compile("^\\d+$");

	private static Variable decodeSQLLiteral(int type, String lit) throws SyntaxError {
		if (type == ZConstant.STRING) {
			return new LiteralVariable(lit);
		} else if (lit.contains(".") || lit.contains("E") || lit.contains("e")) {
			return new LiteralVariable(Double.parseDouble(lit));
		} else if (allDigits.matcher(lit).matches()) {
			return new LiteralVariable(Integer.parseInt(lit));
		} else if (type == ZConstant.DATE){
			try {
				return new LiteralVariable(edu.upenn.cis.orchestra.datamodel.Date.fromString(lit));
			} catch (IllegalArgumentException e) {
			}
				throw new SyntaxError("Could not decode date literal " + lit);
		}
		throw new SyntaxError("Could not determine type of literal " + lit);

	}


	private static Variable makeVariable(ZExp ze,
			Map<String,AtomVariable> atomVariables)
	throws SyntaxError, TypeError {
		if (ze instanceof ZConstant) {
			ZConstant zc = (ZConstant) ze;
			int type = zc.getType();
			String value = zc.getValue();
			if (type == ZConstant.COLUMNNAME) {
				String[] parts = value.split("\\.");
				String valueCorrectCase;
				if (parts.length == 1) {
					valueCorrectCase = value.toLowerCase();
				} else if (parts.length == 2) {
					valueCorrectCase = parts[0].toUpperCase() + "." + parts[1].toLowerCase();
				} else {
					throw new SyntaxError("Incorrectly formed atom " + value);
				}
				AtomVariable av = atomVariables.get(valueCorrectCase);
				if (av == null) {
					throw new SyntaxError("Could not find relation in FROM clause for atom " + valueCorrectCase);
				}
				return av;
			} else {
				return decodeSQLLiteral(type, value);
			}
		}
		if (ze instanceof ZExpression) {
			ZExpression zexp = (ZExpression) ze;
			String op = zexp.getOperator();
			Vector<ZExp> operands = zexp.getOperands();
			int numInputs = operands.size();
			if (numInputs != 1 && numInputs != 2) {
				throw new SyntaxError("Only expect unary or binary functions");
			}
			if (numInputs == 2) {
				List<Variable> inputs = new ArrayList<Variable>(numInputs);
				for (ZExp operand : operands) {
					Variable v = makeVariable(operand, atomVariables);
					inputs.add(v);
				}
				Variable v = null;
				if (op.equals("+")) {
					v = Sum.create(inputs);
				} else if (op.equals("||")) {
					v = Concatenate.create(inputs);
				} else if (op.equals("*")) {
					v = Product.create(inputs);
				} else if (op.equals("-")) {
					List<Integer> mults = Arrays.asList(1, -1);
					v = Sum.create(inputs, mults);
				} else if (op.equals("/")) {
					List<Integer> powers = Arrays.asList(1, -1);
					v = Product.create(inputs, powers);
				}
				if (v != null) {
					return v;
				}
			} else if (numInputs == 1) {
				ZExp operand = operands.get(0);
				Aggregate a = null;
				if (operand instanceof ZConstant) {
					ZConstant zc = (ZConstant) operand;
					if (zc.getType() == ZConstant.COLUMNNAME && zc.getValue().equals("*")) {
						a = Aggregate.makeAggregate(op);
					}
				}
				if (a == null) {
					Variable input = makeVariable(operand, atomVariables);
					a = Aggregate.makeAggregate(op, input);
				}
				if (a != null) {
					return a;
				}
			}
			throw new SyntaxError("Don't support operator " + op);
			// TODO: Add support for CASE function needed for trust conditions
		}
		throw new SyntaxError("Don't know what to do with expression " + ze);
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Query q = (Query) o;

		boolean headsEqual = head.equals(q.head);
		boolean bodiesEqual = exp.equals(q.exp);
		return (headsEqual && bodiesEqual);
	}

	public int hashCode() {
		return head.hashCode() + 37 * exp.hashCode();
	}

	Expression getExpression() {
		return exp;
	}

	public String toString() {
		return head.toString() + " " + exp.toString();
	}
}