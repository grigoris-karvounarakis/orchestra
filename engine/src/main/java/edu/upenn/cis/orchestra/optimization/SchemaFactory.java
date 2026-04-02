package edu.upenn.cis.orchestra.optimization;

import java.util.Collection;
import java.util.Map;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;

public interface SchemaFactory<T extends AbstractRelation> {
	T createNewSchema();
	
	Collection<T> getCreatedSchemas();
	
	Map<String,T> getCreatedSchemasByName();
	
	void clearCreatedSchemas();
}
