package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import edu.upenn.cis.orchestra.p2pqp.Filter.FilterException;

public class FilterOperator<M> extends Operator<M> {
	private final List<Filter<? super QpTuple<?>>> fs;

	public FilterOperator(Filter<? super QpTuple<?>> f, Operator<M> dest, WhichInput destInput, InetSocketAddress nodeId, int operatorId, MetadataFactory<M> mdf, QpSchema.Source schemas, RecordTuples rt, boolean enableRecovery) {
		this(Collections.singleton(f), dest,destInput,nodeId,operatorId,mdf,schemas,rt,enableRecovery);
	}

	public FilterOperator(Collection<? extends Filter<? super QpTuple<?>>> fs, Operator<M> dest, WhichInput destInput, InetSocketAddress nodeId, int operatorId, MetadataFactory<M> mdf, QpSchema.Source schemas, RecordTuples rt, boolean enableRecovery) {
		super(dest,destInput,nodeId,operatorId,mdf,schemas,rt,enableRecovery);
		this.fs = new ArrayList<Filter<? super QpTuple<?>>>(fs);
	}

	@Override
	protected void receiveTuples(WhichInput destInput, QpTupleBag<M> tuples) {
		QpTupleBag<M> output = new QpTupleBag<M>(tuples.schema, schemas, tuples.mdf);
		Iterator<QpTuple<M>> it = tuples.recyclingIterator();
		TUPLE: while (it.hasNext()) {
			QpTuple<M> t = it.next();
			for (Filter<? super QpTuple<?>> f : fs) {
				try {
					if (! f.eval(t)) {
						continue TUPLE;
					}
				} catch (FilterException e) {
					reportException(e);
					return;
				}
			}
			output.add(t);
		}
		tuples.clear();
		if (! output.isEmpty()) {
			sendTuples(output);
		}
	}

	@Override
	protected void inputHasFinished(WhichInput whichChild, int phaseNo) {
		this.finishedSending(phaseNo);
	}

}
