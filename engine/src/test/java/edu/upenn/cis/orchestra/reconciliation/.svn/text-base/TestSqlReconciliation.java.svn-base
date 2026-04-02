package edu.upenn.cis.orchestra.reconciliation;

import java.sql.SQLException;

import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory;

public class TestSqlReconciliation extends TestReconciliation {
	final String jdbcUrl = "jdbc:db2://grw561-3.cis.upenn.edu:50000/orchestr";
	final String username = "orchestra";
	final String password = "apollo";
		
	SqlUpdateStore.Factory factory;
	
	public TestSqlReconciliation() throws ClassNotFoundException, SQLException, UpdateStore.USException {
		Class.forName("com.ibm.db2.jcc.DB2Driver");
		factory = new SqlUpdateStore.Factory(jdbcUrl, username, password); 
	}
	
	@Override
	protected Factory getStoreFactory() {
		return factory;
	}
	
	protected void clearState(Schema s) throws Exception {
		factory.resetStore(s);
	}

}
