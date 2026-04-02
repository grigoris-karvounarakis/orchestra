package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.TestReconciliation;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory;

public class TestBDBReconciliation extends TestReconciliation {
	BerkeleyDBStoreServer server;
	Environment e;
	File envDir = new File("bdbstoredir");
	File configFile = null;
	
	
	public void setUp() throws Exception {
		super.setUp();
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setTransactional(true);
		if (envDir.isDirectory()) {
			for (File f : envDir.listFiles()) {
				f.delete();
			}
		}
		envDir.delete();
		envDir.mkdir();
		e = new Environment(envDir,ec);
		server = new BerkeleyDBStoreServer(e);
	}
	
	public void tearDown() throws Exception {
		super.tearDown();
		server.quit();
		e.close();
		if (configFile != null) {
			configFile.delete();
		}
	}
	

	@Override
	protected void clearState(Schema s) throws Exception {
		// This should be taken care of by tearDown

	}

	@Override
	protected Factory getStoreFactory() {
		try {
			return new BerkeleyDBStoreClient.Factory(new InetSocketAddress(InetAddress.getLocalHost(), BerkeleyDBStoreServer.DEFAULT_PORT));
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
	}

	
}
