package edu.upenn.cis.orchestra.dbms.sql.generation;

import java.util.Vector;

import com.experlog.zql.ZColumnDef;
import com.experlog.zql.ZCreate;

/**
 * 
 * @author gkarvoun
 *
 */
public class SqlCreate extends ZCreate{
	protected SqlQuery _asQuery; // in case this is a CREATE TABLE AS
	protected String _noLogging;
	protected String _typ;
	
	public SqlCreate(String name, String typ, Vector<ZColumnDef> columns, String noLogMsg) {
		super(name, columns);
		_noLogging = noLogMsg;
		if(typ != "")
			_typ = typ + " ";
		else
			_typ = typ; 
	}

	public SqlCreate(String name, SqlQuery asQuery) {
		super(name, null);
		_asQuery = asQuery;
		_typ = "";
	}

	public String toString() {
		if(_asQuery == null){
			StringBuffer cols = new StringBuffer();
			for (int i = 0; i < columns_.size(); i++) {
				if (i > 0) {
					cols.append(", ");
				}
				
				cols.append(columns_.get(i).toString());
			}
			if(_noLogging != null)
				return "CREATE " + _typ + "TABLE " + name_ + "(" + cols + ")" + _noLogging;
			else
				return "CREATE " + _typ + "TABLE " + name_ + "(" + cols + ")";
		}else{
//			Is this DB2 specific?
			if(_noLogging != null)
				return "CREATE " + _typ + "TABLE " + name_ + " as (" + _asQuery.toString() + ") DEFINITION ONLY" + _noLogging;
			else
				return "CREATE " + _typ + "TABLE " + name_ + " as (" + _asQuery.toString() + ") DEFINITION ONLY";
		}
	}

}
