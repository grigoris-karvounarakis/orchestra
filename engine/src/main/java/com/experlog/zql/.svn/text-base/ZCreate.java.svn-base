package com.experlog.zql;

import java.util.Vector;

public class ZCreate implements ZCreateStatement {
    protected String name_;
    protected Vector<ZColumnDef> columns_;  // a vector of ZColumnDef objects
    
    public ZCreate(String name, Vector<ZColumnDef> columns) {
        name_ = name;
        columns_ = columns;
    }
    
    public String getName() {
        return name_;
    }
    
    public int getColumnDefCount() {
        return columns_.size();
    }
    
    public ZColumnDef getColumnDef(int index) {
        return (ZColumnDef)columns_.get(index);
    }
    
    public String toString() {
        StringBuffer cols = new StringBuffer();
        for (int i = 0; i < columns_.size(); i++) {
            if (i > 0) {
                cols.append(", ");
            }
            cols.append(columns_.get(i).toString());
        }
        return "create table " + name_ + "(" + cols + ")";
    }
}
