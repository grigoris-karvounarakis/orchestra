package edu.upenn.cis.orchestra.p2pqp;

public class SimpleTableNameGenerator implements TableNameGenerator {
	private final String tableNamePrefix;
	private int count = 0;
	
	public SimpleTableNameGenerator(String tableNamePrefix) {
		this.tableNamePrefix = tableNamePrefix;
	}
	
	
	public synchronized String getFreshTableName() {
		return tableNamePrefix + (count++);
	}

}
