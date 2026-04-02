package edu.upenn.cis.orchestra.optimization;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import edu.upenn.cis.orchestra.p2pqp.QpSchema;

public class QpSchemaFactory implements SchemaFactory<QpSchema> {
	private Map<String,QpSchema> schemas = new HashMap<String,QpSchema>();
	
	public final int queryId;
	
	public QpSchemaFactory(int queryId) {
		this.queryId = queryId;
	}

	public void clearCreatedSchemas() {
		schemas.clear();
	}

	public QpSchema createNewSchema() {
		int schemaId = schemas.size() + 1;
		QpSchema q = new QpSchema("Q" + queryId + "S" + schemaId, -schemaId);
		schemas.put(q.getName(), q);
		return q;
	}

	public Collection<QpSchema> getCreatedSchemas() {
		return Collections.unmodifiableCollection(schemas.values());
	}

	public Map<String, QpSchema> getCreatedSchemasByName() {
		return Collections.unmodifiableMap(schemas);
	}
}
