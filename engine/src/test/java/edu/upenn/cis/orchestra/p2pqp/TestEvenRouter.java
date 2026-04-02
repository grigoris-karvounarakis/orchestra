package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.orchestra.p2pqp.Router.NodeInfo;


public class TestEvenRouter {
	Id n1, n2, n3, n4, n5, n6, n7, n8;
	InetSocketAddress isa1, isa2, isa3, isa4, isa5, isa6, isa7, isa8;
	Router r8;

	@Before
	public void setUp() throws Exception {
		n1 = Id.fromMSBBytes(new byte[] {5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n2 = Id.fromMSBBytes(new byte[] {15,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n3 = Id.fromMSBBytes(new byte[] {25,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n4 = Id.fromMSBBytes(new byte[] {35,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n5 = Id.fromMSBBytes(new byte[] {45,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n6 = Id.fromMSBBytes(new byte[] {55,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n7 = Id.fromMSBBytes(new byte[] {65,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n8 = Id.fromMSBBytes(new byte[] {75,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
	
		
		isa1 = new InetSocketAddress(InetAddress.getLocalHost(), 1);
		isa2 = new InetSocketAddress(InetAddress.getLocalHost(), 2);
		isa3 = new InetSocketAddress(InetAddress.getLocalHost(), 3);
		isa4 = new InetSocketAddress(InetAddress.getLocalHost(), 4);
		isa5 = new InetSocketAddress(InetAddress.getLocalHost(), 5);
		isa6 = new InetSocketAddress(InetAddress.getLocalHost(), 6);
		isa7 = new InetSocketAddress(InetAddress.getLocalHost(), 7);
		isa8 = new InetSocketAddress(InetAddress.getLocalHost(), 8);
		
		List<NodeInfo> nis = new ArrayList<NodeInfo>(5);
		nis.add(new NodeInfo(n1, isa1));
		nis.add(new NodeInfo(n3, isa3));
		nis.add(new NodeInfo(n2, isa2));
		nis.add(new NodeInfo(n4, isa4));
		nis.add(new NodeInfo(n5, isa5));
		nis.add(new NodeInfo(n6, isa6));
		nis.add(new NodeInfo(n7, isa7));
		nis.add(new NodeInfo(n8, isa8));
		
		r8 = Router.createRouter(nis, 5, Router.Type.EVEN);
		System.out.println(r8.getAvailableRanges());
	}
	
	@Test
	public void testCreateRecoveryRouter() throws Exception {
		Router r = r8.createRecoveryRouter(Collections.singleton(isa7));
		System.out.println(r);
	}
}
