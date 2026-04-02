package edu.upenn.cis.orchestra.repository.utils.dbConverter;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.MetaDataAccessException;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.dbms.SqlDb;


public class SchemaConverterJDBC {

	// TODO: Queries could be returned as an SQL file too...
	// TODO: Would be better to keep indexes tablespaces but is it possible via JDBC ??
	// TODO: It also loses deferrability and this kind of qttributes for foreign keys
	
	private JdbcTemplate _jt;
	private SchemaConverterStatementsGen _statementsGen;
	private Log _log = LogFactory.getLog(getClass());
	
	public SchemaConverterJDBC (DataSource ds, String jdbcDriver, Schema sc)
	{
		_jt = new JdbcTemplate (ds);
		_statementsGen = new SchemaConverterStatementsGen (ds, jdbcDriver, sc);
	}
	
	
	/**
	 * Create the SQL statements necessary to convert the schema's relations so 
	 * that they can store labeled nulls.
	 * @param forceConversion If invalid labeled null column exist in the database, try to run the conversion anyway or cancel
	 * @return List of relations that already contain labeled null columns but without the correct characteristics
	 */	
	public Map<Relation,List<String>> runConversion (boolean forceConversion, boolean withLogging, String database, boolean stratified) throws SQLException
	{
		List<String> statements = new ArrayList<String> ();
		try {
//			HACK!!!
			Config.setJDBCDriver("db2");
			Map<Relation,List<String>> res = _statementsGen.createTableConversionStatements(statements, withLogging, database, stratified, 
					" NOT LOGGED INITIALLY", new SqlDb(null, null, null));
			if (res.size()==0 || forceConversion)
			{
				for (String statement : statements)
				{
					_log.debug("Running query " + statement);
					System.out.println(statement);
					_jt.execute(statement);
				}
			}
			return res;
		} catch (MetaDataAccessException e) {
			throw new SQLException("Unable to convert");
		}
	}
	
}
