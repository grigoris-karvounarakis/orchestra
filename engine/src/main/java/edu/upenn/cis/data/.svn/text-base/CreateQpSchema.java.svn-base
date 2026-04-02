/**
 * 
 */
package edu.upenn.cis.data;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import edu.upenn.cis.orchestra.p2pqp.QpSchema;

public class CreateQpSchema implements CreateSchema<QpSchema> {
	private final Map<String,Set<String>> hashCols;
	private final String hashCol;
	private int nextId;

	public CreateQpSchema(Map<String,Set<String>> hashCols) {
		this(0,hashCols);
	}

	public CreateQpSchema(int nextId, Map<String,Set<String>> hashCols) {
		this.hashCols = hashCols;
		this.nextId = nextId;
		this.hashCol = null;
	}

	public CreateQpSchema(int nextId, String hashCol) {
		this.hashCols = null;
		this.nextId = nextId;
		this.hashCol = hashCol;
	}

	public QpSchema createSchema(String name) {
		return new QpSchema(name, nextId++);
	}

	public void finalize(QpSchema schema) {
		if (hashCol != null) {
			schema.setHashCols(Collections.singleton(hashCol));
		} else {
			Set<String> cols = hashCols.get(schema.getName());
			if (cols == null) {
				schema.setReplicated();
			} else {
				schema.setHashCols(cols);
			}
		}
		schema.markFinished();
	}

}