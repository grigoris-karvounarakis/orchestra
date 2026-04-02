package edu.upenn.cis.orchestra.optimization;

import java.util.Iterator;

public interface PhysicalPropertiesFactory<P extends PhysicalProperties> {
	Iterator<P> enumerateAllProperties(Expression e, RelationTypes<? extends P,?> rt);
	Iterator<P> getRelevantViewProperties(Expression e);
}