package edu.upenn.cis.orchestra.reconciliation;

import com.sleepycat.je.Environment;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;

public class TestBerkeleyDBStore extends TestStore {
	Environment e;
	@Override
	StateStore getStore(AbstractPeerID pi, SchemaIDBinding scm, Schema s) throws Exception {
		return new BerkeleyDBStore(e, "state", "updates", pi, scm, -1);

	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
		((BerkeleyDBStore) ss).close();
		e.close();
	}

}
