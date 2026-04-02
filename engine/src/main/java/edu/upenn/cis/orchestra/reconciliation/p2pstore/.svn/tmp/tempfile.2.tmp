package edu.upenn.cis.orchestra.reconciliation.p2pstore;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.USDump;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

public class PastryStoreFactory implements UpdateStore.Factory {
	PastryNodeFactory nf;
	rice.environment.Environment env;
	Environment dbEnv;
	String epochDbName;
	String transactionDbName;
	String reconciliationDbName;
	String decisionDbName;
	String pastryIdsDbName;
	int replicationFactor;
	
	int createdCount = 0;

	List<P2PStore> stores = new ArrayList<P2PStore>();
	
	// Is the factory creating a local update store?  Here we say yes --
	// there is a local object representing the factory
	public boolean isLocal() {
		return true;
	}
	
	public PastryStoreFactory(int port, Environment dbEnv, int replicationFactor,
			String epochDbName, String transactionDbName,
			String reconciliationDbName, String decisionDbName, String pastryIdsDbName) {
		env = new rice.environment.Environment();
		// Queue up a lot of messages, if necessary
		env.getParameters().setInt("pastry_socket_writer_max_queue_length", 50000);
		// Replication interval, in milliseconds
		env.getParameters().setInt("p2p_replication_maintenance_interval", 100000);
		// Buffer messages that can't be delivered
		env.getParameters().setBoolean("pastry_messageDispatch_bufferIfNotReady", true);
		// Buffer this many messages before we start dropping messages
		env.getParameters().setInt("pastry_messageDispatch_bufferSize", 128);
		this.dbEnv = dbEnv;
		this.epochDbName = epochDbName;
		this.transactionDbName = transactionDbName;
		this.reconciliationDbName = reconciliationDbName;
		this.decisionDbName = decisionDbName;
		this.pastryIdsDbName = pastryIdsDbName;
		this.replicationFactor = replicationFactor;
		nf = new PastryNodeFactory(env, port);
	}
	
	public void shutdownEnvironment() {
		env.destroy();
	}
	
	public P2PStore getUpdateStore(AbstractPeerID pid, SchemaIDBinding sch, Schema s, TrustConditions tc) throws USException {
		++createdCount;
		P2PStore retval = new P2PStore(nf, pid, tc, sch, s, dbEnv, replicationFactor,
				epochDbName + createdCount, transactionDbName + createdCount,
				reconciliationDbName + createdCount, decisionDbName + createdCount,
				pastryIdsDbName + createdCount);
		stores.add(retval);
		return retval;
	}

	public void serialize(Document doc, Element update) {
		update.setAttribute("type", "p2p");
		update.setAttribute("port", Integer.toString(nf.port));
		try {
			update.setAttribute("workdir", dbEnv.getHome().getPath());
		} catch (DatabaseException e) {
			assert(false);	// shouldn't happen
		}
		update.setAttribute("replicationFactor", Integer.toString(replicationFactor));
		update.setAttribute("epochDbName", epochDbName);
		update.setAttribute("transactionDbName", transactionDbName);
		update.setAttribute("reconciliationDbName", reconciliationDbName);
		update.setAttribute("decisionDbName", decisionDbName);
		update.setAttribute("pastryIdsDbName", pastryIdsDbName);
	}

	static public PastryStoreFactory deserialize(Element update) throws DatabaseException {
		int port = Integer.parseInt(update.getAttribute("port"));
		String workdir = update.getAttribute("workdir");
		Environment dbEnv = new Environment(new File(workdir), null);
		int replicationFactor = Integer.parseInt(update.getAttribute("replicationFactor"));
		String epochDbName = update.getAttribute("epochDbName");
		String transactionDbName = update.getAttribute("transactionDbName");
		String reconciliationDbName = update.getAttribute("reconciliationDbName");
		String decisionDbName = update.getAttribute("decisionDbName");
		String pastryIdsDbName = update.getAttribute("pastryIdsDbName");
		return new PastryStoreFactory(port, dbEnv, replicationFactor, epochDbName, transactionDbName, 
				reconciliationDbName, decisionDbName, pastryIdsDbName);
	}

	public void resetStore(Schema s) throws USException {
		throw new UnsupportedOperationException("reset is not supported for P2PStores (yet)");
	}

	public USDump dumpUpdateStore(SchemaIDBinding binding, Schema s) throws USException {
		throw new UnsupportedOperationException();
	}

	public void restoreUpdateStore(USDump d) throws USException {
		throw new UnsupportedOperationException();
	}
}
