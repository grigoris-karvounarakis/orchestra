package edu.upenn.cis.orchestra.datamodel;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicatePeerIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.repository.model.beans.OrchestraSystemBean;

public class TestOrchestraSystem extends junit.framework.TestCase {

	private Peer _peer1;
	private Peer _peer2;
	SchemaIDBinding scm;
	
	@Before
	public void setUp () throws UnsupportedTypeException, DatabaseException
	{
		TestPeer test = new TestPeer ();
		test.setUp();
		_peer1 = test.getPeerTwoSchemas("peer1");
		_peer2 = test.getPeerTwoSchemas("peer2");
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
		ec.setTransactional(true);
		Environment env = new Environment(f, ec);
		scm = new SchemaIDBinding(env);
		
		Map<AbstractPeerID,Integer> peerSchema = new HashMap<AbstractPeerID,Integer>();
		peerSchema.put(_peer1.getPeerId(), 0);
		List<Schema> schemas = new ArrayList<Schema>();
		schemas.addAll(_peer1.getSchemas());
		peerSchema.put(_peer2.getPeerId(), 1);
		
		scm.registerAllSchemas("test", schemas, peerSchema);
	}
	
	@Test
	public void testBasicProperties ()
	{
		OrchestraSystem system = new OrchestraSystem (scm);
		addPeer(system, _peer1, false);
		addPeer(system, _peer2, false);
		
		OrchestraSystemBean bean = system.deepCopy().toBean();
		assertTrue(bean.getPeers().size()==2);
		TestSchema.checkTwoNames("peer1", "peer2", bean.getPeers().get(0).getId(), bean.getPeers().get(1).getId());
		
		
	}
	
	@Test
	public void testPeerIdConflict ()
	{
		OrchestraSystem system = new OrchestraSystem (scm);
		addPeer(system, _peer1, false);
		addPeer(system, _peer1.deepCopy(), true);		
	}
	
	private void addPeer (OrchestraSystem system, Peer peer, boolean shouldFail)
	{
		
		try
		{
			system.addPeer(peer);
			assertTrue(!shouldFail);
		} catch (DuplicatePeerIdException ex)
		{
			assertTrue(shouldFail);
		}
	}
	
	
	
}
