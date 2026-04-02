package edu.upenn.cis.orchestra.repository.utils.loader;

/**
 * Configuration for <code>SchemaLoader</code>s.
 * 
 * @author Sam Donnelly
 */
public interface ISchemaLoaderConfig {

	/**
	 * Return <code>true</code> if the table passes the filter,
	 * <code>false</code> otherwise.
	 * 
	 * @param databaseProductName
	 *            the database we're using.
	 * @param schemaName
	 *            the schema name.
	 * @return see description.
	 */
	public boolean filterOnSchemaName(String databaseProductName,
			String schemaName);

	/**
	 * Return <code>true</code> if the table passes the filter,
	 * <code>false</code> otherwise.
	 * 
	 * @param tableName
	 *            the table name.
	 * @return see description.
	 */
	public boolean filterOnTableName(String tableName);

}
