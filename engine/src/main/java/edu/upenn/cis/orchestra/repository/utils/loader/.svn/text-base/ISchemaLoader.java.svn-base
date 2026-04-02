//Source file: D:\\PENN\\SHARQ\\API\\SCHEMAAPI\\src\\main\\java\\edu\\upenn\\cis\\orchestra\\repository\\utils\\SchemaLoader.java

package edu.upenn.cis.orchestra.repository.utils.loader;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicatePeerIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.repository.utils.loader.exceptions.SchemaLoaderException;

/**
 * This interface was defined as a basis for utilities allowing the system to
 * load automatically relations from different sources (RDBMS, XML files...) and
 * add them to an Orchestra schema
 * 
 * @author Olivier Biton
 */
public interface ISchemaLoader {

	/**
	 * Build an <code>OrchestraSystem</code> from an existing schema.
	 * 
	 * @param orchestraSystemName
	 *            TODO
	 * @param catalogPattern
	 *            Filter on the catalog from which tables should be loaded, can
	 *            be null (no filter)
	 * @param schemaPattern
	 *            Filter on the schema from which tables should be loaded, can
	 *            be null (no filter)
	 * @param tableNamePattern
	 *            Filter on the table names to load, can be null (no filter)
	 * @roseuid 44AD2F63006D
	 * @return see description.
	 * @throws DuplicateSchemaIdException
	 *             if such an exception if thrown while building the
	 *             <code>OrchestraSystem</code>.
	 * @throws DuplicatePeerIdException
	 *             if such an exception if thrown while building the
	 *             <code>OrchestraSystem</code>.
	 */
	public OrchestraSystem buildSystem(String orchestraSystemName,
			String catalogPattern, String schemaPattern, String tableNamePattern)
			throws SchemaLoaderException, DuplicateSchemaIdException,
			DuplicatePeerIdException;

	/**
	 * For a given schema, refresh all constraints and statistics (tables row
	 * counts, indexes unique values counts, foreign keys...)
	 * 
	 * @param schema
	 *            Schema for which constraints and statistics must be updated
	 * @throws SchemaLoaderException
	 *             If any error occurs while extracting metadata
	 */
	public void refreshSchemaConstraintsAndStatistics(Schema schema)
			throws SchemaLoaderException;
}
