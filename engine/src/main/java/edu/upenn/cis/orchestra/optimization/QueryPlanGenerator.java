package edu.upenn.cis.orchestra.optimization;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.util.Pair;

/**
 * @author netaylor
 *
 * @param <P>			The physical properties used by this type of query plan
 * @param <T>			The cost metric used by this type of query plan
 * @param <S>			The type of schema used by this type of query plan
 * @param <QP>			The type of the query plan
 */
public interface QueryPlanGenerator<P extends PhysicalProperties,T,S extends AbstractRelation,QP> extends Comparator<T> {
	public static abstract class LocalCost<T,P extends PhysicalProperties, S extends AbstractRelation,QP> {
		public final T localCost;
		public final List<Pair<Integer,P>> inputs;
		
		public LocalCost(T localCost, List<Pair<Integer,P>> inputs) {
			this.localCost = localCost;
			this.inputs = Collections.unmodifiableList(new ArrayList<Pair<Integer,P>>(inputs));
		}
		
		public LocalCost(T localCost, List<Integer> inputExps, List<P> inputProps) {
			this.localCost = localCost;
			if (inputExps.size() != inputProps.size()) {
				throw new IllegalArgumentException("inputExps and inputProps must have the same length");
			}
			List<Pair<Integer,P>> inputs = new ArrayList<Pair<Integer,P>>(inputExps.size());
			Iterator<Integer> expIt = inputExps.iterator();
			Iterator<P> propIt = inputProps.iterator();
			while (expIt.hasNext()) {
				inputs.add(new Pair<Integer,P>(expIt.next(), propIt.next()));
			}
			this.inputs = Collections.unmodifiableList(inputs);
		}

		abstract CreatedQP<S,QP,T> createQP(OperatorIdSource ois, SchemaFactory<? extends S> schemaFactory, List<CreatedQP<S,QP,T>> inputs);
		
		public abstract String toString();
		
		public abstract P getOutputProperties();
	}
	
	public static class CreatedQP<S extends AbstractRelation,QP,T> {
		public final QP qp;
		public final S schema;
		public final VariablePosition varPos;
		public final T cost;
		
		CreatedQP(QP qp, S schema, T cost, VariablePosition varPos) {
			this.qp = qp;
			this.schema = schema;
			this.varPos = varPos;
			this.cost = cost;
		}
	}
	
	Collection<? extends LocalCost<T,P,S,QP>> getLocalCost(AndNode an, P props, int expId, Optimizer<? extends P,QP,T,? extends S> o, PhysicalPropertiesFactory<P> propFactory);

	LocalCost<T, P, S, QP> getScanCost(int expId, P props, Optimizer<? extends P,QP,T,? extends S> o);
	
	CreatedQP<S,QP,T> createQueryRoot(List<Variable> head, S headSchema, SchemaFactory<? extends S> schemaFactory, OperatorIdSource ois, P dest, CreatedQP<S,QP,T> input);
	
	/**
	 * Add two costs together.
	 * 
	 * @param c1		The first cost
	 * @param c2		The second cost
	 * @return			Their sum
	 */
	T addTogether(T c1, T c2);
	
	
	/**
	 * Subtract one cost from another.
	 * 
	 * @param c1		The minuend
	 * @param c2		The subtrahend
	 * @return			Their difference
	 */
	T subtractFrom(T c1, T c2);
	
	/**
	 * Get the addition identity for the costs (usually zero).
	 * 
	 * @return			The identity
	 */
	T getIdentity();
	
	/**
	 * Determine how the costs of multiple inputs to an operator should be combined
	 * 
	 * @return	<code>true</code> if the max of the inputs' costs should be used,
	 * 			<code>false</code> if the sum of the inputs' costs should be used
	 */
	boolean takeMaxOfMultipleInputs();

	
	public abstract void setExpectedCard(QP plan, double card);
}
