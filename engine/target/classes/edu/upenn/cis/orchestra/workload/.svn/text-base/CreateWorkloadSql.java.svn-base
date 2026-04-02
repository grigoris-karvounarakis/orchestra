package edu.upenn.cis.orchestra.workload;

import com.sleepycat.je.Environment;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.BerkeleyDBStore;
import edu.upenn.cis.orchestra.reconciliation.ClientCentricDb;
import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.SqlUpdateStore;
import edu.upenn.cis.orchestra.reconciliation.StateStore;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore;

public class CreateWorkloadSql {
	final static String jdbcUrl = "jdbc:db2://grw561-3.cis.upenn.edu:50000/orchestr";
	final static String username = "orchestra";
	final static String password = "apollo";
	public static void main(String[] args) throws Exception {
		Class.forName(Config.getJDBCDriver());
		CreateWorkload cw = new CreateWorkload();
		cw.createWorkload(new SqlFactory(), args);
	}
	static class SqlFactory implements CreateWorkload.DatabaseFactory {
		UpdateStore.Factory usf;
		SqlFactory() {
			usf = new SqlUpdateStore.Factory(jdbcUrl, username, password);
		}
		public Db createDb(int id, Schema s, Environment env) throws Exception {
			StateStore.Factory ssf = new BerkeleyDBStore.Factory(env, "statestore_state" + id, "statestore_updates" + id);
			SchemaIDBinding scm = new SchemaIDBinding(env); 
			return new ClientCentricDb(new OrchestraSystem(scm), scm, s, new IntPeerID(id), usf, ssf);
		}

		public void initDb(Schema s) throws Exception {
			usf.resetStore(s);
		}
	}
}
