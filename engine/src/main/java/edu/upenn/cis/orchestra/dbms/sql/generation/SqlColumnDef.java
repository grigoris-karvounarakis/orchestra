package edu.upenn.cis.orchestra.dbms.sql.generation;

import com.experlog.zql.ZColumnDef;

/**
 * 
 * @author gkarvoun
 *
 */
public class SqlColumnDef extends ZColumnDef{
    protected String name_;
    protected String type_;
    protected String default_;
    
    public SqlColumnDef(String name, String type, String def) {
        super(name, type);
        default_ = def;
    }
    
    public String getDefaultMsg(){
    	return "DEFAULT " + default_;
    }
    
    public String getDefault(){
    	return default_;
    }
    
    public String toString(boolean printDef) {
    	String ret = name_;
    	if(type_ != ""){
    		if(default_ != null && printDef)
    			ret = ret + " " + type_ + " " + getDefaultMsg();
    		else
    			ret = ret + " " + type_;
    	}
    	return ret;
    }
    
}
