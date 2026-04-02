package edu.upenn.cis.orchestra.p2pqp;


import java.io.File;


import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class TestBDbTupleStore extends TestTupleStore {

	Environment e;
	
	public TupleStore<Null> getTupleStore() throws Exception {
		File f = new File("dbenv");
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
		} else {
			f.mkdir();
		}
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		e = new Environment(f, ec);
		
		BDbTupleStore<Null> ts = new BDbTupleStore<Null>(e, "tuplestore", null);
		ts.batchingCutoff = 2;
		return ts;
	}
	
	void close(TupleStore<Null> ts) throws Exception {
		if (ts != null) {
			ts.close();
			ts = null;
		}
		if (e != null) {
			e.close();
			e = null;
		}
	}

}
