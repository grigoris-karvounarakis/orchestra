package edu.upenn.cis.orchestra.dbms.sql.generation;

import java.util.Vector;

import com.experlog.zql.ZFromItem;
import com.experlog.zql.ZQuery;
import com.experlog.zql.ZSelectItem;
import edu.upenn.cis.orchestra.*;
/**
 * SqlQuery: an SQL SELECT statement
 * 
 * @author gkarvoun 
 * 
 */
public class SqlQuery extends ZQuery {

	public SqlQuery() {
		super();
	}
	
	public SqlQuery(SqlSelectItem si, SqlFromItem fi, SqlExpression wi) {
		super();
		
		Vector<SqlSelectItem> s = new Vector<SqlSelectItem>();
		s.add(si);
		
		Vector<SqlFromItem> f = new Vector<SqlFromItem>();
		f.add(fi);
		
		addSelectClause(s);
		addFromClause(f);
		addWhere(wi);
	}

	public void addSelectClause(Vector<SqlSelectItem> vs){
		Vector<ZSelectItem> v = new Vector<ZSelectItem>();
		v.addAll(vs);
		addSelect(v);
	}
	
	public void addFromClause(Vector<SqlFromItem> vs){
		Vector<ZFromItem> v = new Vector<ZFromItem>();
		v.addAll(vs);
		addFrom(v);
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer("select ");
		// TODO: make this -- distinct part when FROM is empty general!
		// Since we need to add a table to the FROM clause we need to
		// make sure we only output one result
		if(distinct_ || (from_.size() == 0)) buf.append("distinct ");

		//buf.append(select_.toString());
		int i;
		buf.append(select_.elementAt(0).toString());
		for(i=1; i<select_.size(); i++) {
			buf.append(", " + select_.elementAt(i).toString());
		}

		//buf.append(" from " + from_.toString());
		if (from_.size() > 0) {
			buf.append(" from ");
			buf.append(from_.elementAt(0).toString());
			for(i=1; i<from_.size(); i++) {
				buf.append(", " + from_.elementAt(i).toString());
			}
		} else
			// TODO: make this general!  If we have an empty FROM clause we
			// need at least one relation
			if(Config.isDB2() || Config.isHsql())
				buf.append(" from sysibm.systables");
			else if(Config.isMYSQL())
				buf.append(" from DUAL");


		if(where_ != null) {
			buf.append(" where " + where_.toString());
		}
		if(groupby_ != null) {
			buf.append(" " + groupby_.toString());
		}
		if(setclause_ != null) {
			buf.append(" " + setclause_.toString());
		}
		if(orderby_ != null) {
			buf.append(" order by ");
			//buf.append(orderby_.toString());
			buf.append(orderby_.elementAt(0).toString());
			for(i=1; i<orderby_.size(); i++) {
				buf.append(", " + orderby_.elementAt(i).toString());
			}
		}
		if(forupdate_) buf.append(" for update");

		return buf.toString();
	}
};

