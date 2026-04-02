package edu.upenn.cis.orchestra.datamodel;

import java.util.Iterator;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.orchestra.datamodel.exceptions.DuplicateSchemaIdException;
import edu.upenn.cis.orchestra.datamodel.exceptions.InvalidBeanException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.repository.model.beans.PeerBean;

public class TestPeer extends TestCase {

	// Use TestSchema setUp...
	private Schema _schema1;
	private Schema _schema2;
	
	@Before
	public void setUp () throws UnsupportedTypeException
	{
		TestSchema test = new TestSchema ();
		test.setUp();
		_schema1 = test.getTwoRelsSchema("schema1");
		_schema2 = test.getTwoRelsSchema("schema2");
	}
	
	
	public Peer getPeerTwoSchemas (String peerId)
	{
		Peer peer = new Peer (peerId, "127.0.0.1:4051", "test peer");
		addSchema(peer, _schema1.deepCopy(), false);
		addSchema(peer, _schema2.deepCopy(), false);
		return peer;
	}
	
	@Test
	public void testBasicProperties ()
	{
		Peer peer = getPeerTwoSchemas("peer");
		
		PeerBean bean = peer.toBean();
		assertTrue(bean.getId().equals("peer"));
		assertTrue(bean.getAddress().equals("127.0.0.1:4051"));
		assertTrue(bean.getDescription().equals("test peer"));
		assertTrue(bean.getSchemas().size()==2);
		TestSchema.checkTwoNames("schema1", "schema2", bean.getSchemas().get(0).getSchemaId(), bean.getSchemas().get(1).getSchemaId());
		
		Peer peer2 = getPeerFromBean (bean, false);
		assertTrue(peer2.getId().equals("peer"));
		assertTrue(peer2.getAddress().equals("127.0.0.1:4051"));
		assertTrue(peer2.getDescription().equals("test peer"));
		assertTrue(peer2.getSchemas().size()==2);
		Iterator<Schema> itSchema = peer2.getSchemas().iterator();
		TestSchema.checkTwoNames("schema1", "schema2", itSchema.next().getSchemaId(), itSchema.next().getSchemaId());
		assertTrue(peer2.getSchema("schema1")!=null);
		assertTrue(peer2.getSchema("schema2")!=null);
		
	}
	
	@Test
	public void testSchemaIdConflict ()
	{
		Peer peer = new Peer ("peer", "127.0.0.1:4051", "test peer");
		addSchema(peer, _schema1.deepCopy(), false);
		addSchema(peer, _schema1.deepCopy(), true);
		
	}
	
	private void addSchema (Peer peer, Schema schema, boolean shouldFail)
	{
		try
		{
			peer.addSchema(schema);
			assertTrue(!shouldFail);
		} catch (DuplicateSchemaIdException ex)
		{
			assertTrue(shouldFail);
		}
	}
	
	
	private Peer getPeerFromBean (PeerBean bean, boolean shouldFail)
	{

		Peer res = null;
		try
		{
			res= new Peer (bean);
			assertTrue(!shouldFail);
		} catch (InvalidBeanException ex)
		{
			assertTrue(shouldFail);
		}
		return res;
		
		
	}
	
}
