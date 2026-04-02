package edu.upenn.cis.orchestra.dbms.sql.generation;

import java.util.Vector;

import com.experlog.zql.ZColumnDef;
import com.experlog.zql.ZCreate;

import edu.upenn.cis.orchestra.dbms.sql.vendors.DB2SqlStatementGen;

/**
 * 
 * @author gkarvoun
 *
 */
public class SqlCreateTempTable extends ZCreate{
	protected boolean _isDB2;
	protected String _typ;
	
	public SqlCreateTempTable(String name, String typ, Vector<ZColumnDef> columns, SqlStatementGen gen) {
		super(name, columns);
		_isDB2 = (gen instanceof DB2SqlStatementGen);
		if(typ != "")
			_typ = typ + " ";
		else
			_typ = typ; 
	}

	public String toString() {
			StringBuffer cols = new StringBuffer();
			for (int i = 0; i < columns_.size(); i++) {
				if (i > 0) {
					cols.append(", ");
				}
				
				cols.append(columns_.get(i).toString());
			}
			if(!_isDB2)
				return "CREATE " + _typ + "TABLE " + name_ + "(" + cols + ")";
			else
				return "DECLARE GLOBAL TEMPORARY TABLE " + name_ + " (" + cols + ") ON COMMIT PRESERVE ROWS NOT LOGGED";
		}

}
