package edu.upenn.cis.orchestra.p2pqp;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.FieldSource;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.p2pqp.Function.EvaluateException;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class FunctionEvaluator<M> extends Operator<M> {
	private final QpSchema inputSchema, outputSchema;
	private final ColumnOrFunction[] toOutput;
	private final EvalStep[] evalSteps;
	private final int numTempVars;
	private final AbstractRelation.RelationMapping rm;

	public FunctionEvaluator(QpSchema inputSchema, QpSchema outputSchema,
			List<ColumnOrFunction> toOutput, Operator<M> dest,
			WhichInput destInput, InetSocketAddress nodeId, int operatorId, MetadataFactory<M> mdf, QpSchema.Source schemas, RecordTuples rt, boolean enableRecovery) throws ValueMismatchException {
		super(dest, destInput, nodeId, operatorId, mdf, schemas, rt, enableRecovery);
		this.inputSchema = inputSchema;
		this.outputSchema = outputSchema;

		this.toOutput = toOutput.toArray(new ColumnOrFunction[toOutput.size()]);

		if (outputSchema.getNumCols() != toOutput.size()) {
			throw new IllegalArgumentException("toOutput contains the wrong number of elements (" + toOutput.size() + ") for output schema " + outputSchema.getRelationName() + " (" + outputSchema.getNumCols() + ")");
		}

		Set<EvalFunc> functionsToEval = new HashSet<EvalFunc>();
		Set<ColumnInput> inputColumns = new HashSet<ColumnInput>();

		for (ColumnOrFunction cof : toOutput) {
			cof.getColumnsUsed(inputColumns);
			cof.getUsedFunctions(functionsToEval);
		}

		final int numInputColumns = this.inputSchema.getNumCols();

		for (ColumnInput ci : inputColumns) {
			if (ci.column >= numInputColumns) {
				throw new IllegalArgumentException("Column " + ci.column + " is not present in input schema " + inputSchema.getRelationName());
			}
		}

		Map<EvalFunc,Integer> tempPos = new HashMap<EvalFunc,Integer>();

		EvalFunc[] evalOrder = functionsToEval.toArray(new EvalFunc[functionsToEval.size()]);
		Arrays.sort(evalOrder);

		for (int i = 0; i < evalOrder.length; ++i) {
			tempPos.put(evalOrder[i], i);
		}

		evalSteps = new EvalStep[evalOrder.length];
		for (int i = 0; i < evalOrder.length; ++i) {
			evalSteps[i] = new EvalStep(evalOrder[i], tempPos);
		}
		
		AbstractRelation.FieldSource fss[] = new FieldSource[this.toOutput.length];
		for (int i = 0; i < this.toOutput.length; ++i) {
			if (this.toOutput[i] instanceof EvalFunc) {
				fss[i] = new FieldSource(tempPos.get(this.toOutput[i]), false);
			} else {
				ColumnInput ci = (ColumnInput) this.toOutput[i];
				fss[i] = new FieldSource(ci.column, true);
			}
		}
		
		rm = new AbstractRelation.RelationMapping(this.inputSchema, this.outputSchema, null, fss);

		this.numTempVars = evalOrder.length;

		int pos = 0;
		for (ColumnOrFunction cof : toOutput) {
			Type inputType;
			if (cof instanceof ColumnInput) {
				ColumnInput ci = (ColumnInput) cof;
				inputType = inputSchema.getColType(ci.column);
			} else {
				EvalFunc ef = (EvalFunc) cof;
				inputType = ef.f.getOutputType();
			}
			Type outputType = outputSchema.getColType(pos);

			if (! inputType.canPutInto(outputType)) {
				throw new IllegalArgumentException("Cannot put value of type " + inputType + " into column " + outputSchema.getColName(pos) + " of type " + outputType);
			}
			++pos;
		}

		for (EvalFunc ef : functionsToEval) {
			final int numInputs = ef.inputs.size();
			if (ef.f.getNumInputs() != numInputs) {
				throw new IllegalArgumentException("Wrong number of arguments to function " + ef.f + ", expected " + ef.f.getNumInputs() + " but was " + numInputs);
			}
			Iterator<ColumnOrFunction> input = ef.inputs.iterator();
			for (int i = 0; i < numInputs; ++i) {
				Type t = input.next().getType(inputSchema);
				if (! ef.f.isValidForInput(i, t)) {
					throw new IllegalArgumentException("Incorrect input #" + i + " of type " + t + " to function " + ef.f);
				}
			}
		}
	}

	@Override
	protected void inputHasFinished(WhichInput whichChild, int phaseNo) {
		this.finishedSending(phaseNo);
	}

	@Override
	public void receiveTuples(WhichInput destInput, QpTupleBag<M> tuples) {
		QpTupleBag<M> results = new QpTupleBag<M>(outputSchema, schemas, tuples.mdf);
		byte[][] tempVars = new byte[numTempVars][];
		
		Iterator<QpTuple<M>> it = tuples.recyclingIterator();
		while (it.hasNext()) {
			QpTuple<M> t = it.next();
			for (EvalStep es : evalSteps) {
				try {
					es.eval(tempVars, t);
				} catch (ValueMismatchException e) {
					this.reportException(e);
					return;
				} catch (EvaluateException e) {
					this.reportException(e);
					return;
				}
			}
			results.addAndApplyMapping(rm, t, tempVars);
		}
		tuples.clear();
		sendTuples(results);
	}
	
	private static class EvalStep {
		final int[] functionInputCols;
		final Function f;
		final int outputPos;
		
		EvalStep(EvalFunc ef, Map<EvalFunc,Integer> tempPos) {
			f = ef.f;
			outputPos = tempPos.get(ef);
			int pos = 0;
			this.functionInputCols = new int[ef.inputs.size()];
			for (ColumnOrFunction cof : ef.inputs) {
				if (cof instanceof ColumnInput) {
					functionInputCols[pos] = ((ColumnInput) cof).column;
				} else {
					functionInputCols[pos] = (tempPos.get(cof) + 1) * -1;
				}
				++pos;
			}
		}
		
		void eval(byte[][] data, QpTuple<?> input) throws ValueMismatchException, EvaluateException {
			data[outputPos] = f.evaluateFunction(input, data, functionInputCols);
		}
	}

	public abstract static class ColumnOrFunction implements Comparable<ColumnOrFunction>, Serializable {
		private static final long serialVersionUID = 1L;

		public abstract boolean equals(Object o);
		public abstract int hashCode();
		abstract int getDepth();

		public int compareTo(ColumnOrFunction cof) {
			return getDepth() - cof.getDepth();
		}

		abstract void getUsedFunctions(Set<EvalFunc> efs);
		abstract void getColumnsUsed(Set<ColumnInput> cols);
		abstract Type getType(QpSchema inputSchema);

		public void loadSchemas(Source schemas) {
		}

		public abstract Element serialize(Document d, Map<String,QpSchema> schemas);

		public static ColumnOrFunction deserialize(Element el, Map<String,QpSchema> schemas) throws XMLParseException {
			String tag = el.getTagName(); 
			if (tag.equals("columnInput")) {
				return ColumnInput.deserialize(el);
			} else if (tag.equals("evalFunc")) {
				return EvalFunc.deserialize(el,schemas);
			} else {
				throw new XMLParseException("Unknown tag: " + tag, el);
			}
		}

	}

	public static class ColumnInput extends ColumnOrFunction {
		private static final long serialVersionUID = 1L;
		public final int column;
		public ColumnInput(int column) {
			this.column = column;
		}
		@Override
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}
			ColumnInput ci = (ColumnInput) o;
			return column == ci.column;
		}
		@Override
		public int hashCode() {
			return column;
		}
		@Override
		int getDepth() {
			return 0;
		}
		@Override
		void getUsedFunctions(Set<EvalFunc> efs) {
		}
		@Override
		void getColumnsUsed(Set<ColumnInput> cols) {
			cols.add(this);
		}
		@Override
		Type getType(QpSchema inputSchema) {
			return inputSchema.getColType(column);
		}
		@Override
		public Element serialize(Document d, Map<String,QpSchema> schemas) {
			Element el = d.createElement("columnInput");
			el.setAttribute("column", Integer.toString(column));
			return el;
		}
		public static ColumnInput deserialize(Element el) throws XMLParseException {
			String colAtt = el.getAttribute("column");
			if (colAtt.length() == 0) {
				throw new XMLParseException("Missing column attribute", el);
			}
			return new ColumnInput(Integer.parseInt(colAtt));
		}
	}

	public static class EvalFunc extends ColumnOrFunction {
		private static final long serialVersionUID = 1L;
		private final Function f;
		private final Collection<ColumnOrFunction> inputs;
		private final int hashCode;
		public EvalFunc(Function f, Collection<? extends ColumnOrFunction> inputs) {
			if (f == null) {
				throw new NullPointerException();
			}
			this.f = f;
			this.inputs = new ArrayList<ColumnOrFunction>(inputs.size());
			this.inputs.addAll(inputs);
			
			this.hashCode = f.hashCode() + 37 * inputs.hashCode();
		}
		@Override
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}
			EvalFunc ef = (EvalFunc) o;
			return (f.equals(ef.f) && inputs.equals(ef.inputs));
		}
		@Override
		public int hashCode() {
			return hashCode;
		}
		@Override
		int getDepth() {
			int maxDepth = 0;
			for (ColumnOrFunction cof : inputs) {
				int depth = cof.getDepth();
				if (depth > maxDepth) {
					maxDepth = depth;
				}
			}
			return maxDepth + 1;
		}
		@Override
		void getUsedFunctions(Set<EvalFunc> efs) {
			for (ColumnOrFunction cof : inputs) {
				cof.getUsedFunctions(efs);
			}
			efs.add(this);
		}
		@Override
		void getColumnsUsed(Set<ColumnInput> cols) {
			for (ColumnOrFunction cof: inputs) {
				cof.getColumnsUsed(cols);
			}
		}
		@Override
		Type getType(QpSchema inputSchema) {
			return f.getOutputType();
		}

		public void loadSchemas(Source schemas) {
			f.loadSchemas(schemas);
		}

		@Override
		public Element serialize(Document d, Map<String,QpSchema> schemas) {
			Element el = d.createElement("evalFunc");
			Element funcEl = DomUtils.addChild(d, el, "function");
			funcEl.appendChild(f.serialize(d, schemas));
			Element inputEl = DomUtils.addChild(d, el, "inputs");
			for (ColumnOrFunction cof : inputs) {
				inputEl.appendChild(cof.serialize(d, schemas));
			}
			return el;
		}

		public static EvalFunc deserialize(Element el, Map<String,QpSchema> schemas) throws XMLParseException {
			Element funcEl = DomUtils.getChildElementByName(el, "function");
			if (funcEl == null) {
				throw new XMLParseException("Missing function child element", el);
			}
			List<Element> func = DomUtils.getChildElements(funcEl);
			if (func.size() != 1) {
				throw new XMLParseException("Expecting exactly one function", funcEl);
			}
			Function f = Function.deserialize(func.get(0), schemas);

			Element inputEl = DomUtils.getChildElementByName(el, "inputs");
			if (inputEl == null) {
				throw new XMLParseException("");
			}
			List<ColumnOrFunction> inputs = new ArrayList<ColumnOrFunction>();
			List<Element> inputEls = DomUtils.getChildElements(inputEl);
			for (Element e : inputEls) {
				inputs.add(ColumnOrFunction.deserialize(e,schemas));
			}

			return new EvalFunc(f, inputs);
		}
	}
}
