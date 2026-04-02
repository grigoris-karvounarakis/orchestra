package edu.upenn.cis.orchestra.p2pqp;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.orchestra.p2pqp.Router.NodeInfo;

public class TestPastryRouter {
	
	Router r, r5, r7;
	Id n1, n2, n3, n4, n5, n6, n7;
	InetSocketAddress isa1, isa2, isa3, isa4, isa5, isa6, isa7, isa8;
	
	@Before
	public void createRouter() throws Exception {
		n1 = Id.fromMSBBytes(new byte[] {5,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n2 = Id.fromMSBBytes(new byte[] {15,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n3 = Id.fromMSBBytes(new byte[] {25,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n4 = Id.fromMSBBytes(new byte[] {35,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n5 = Id.fromMSBBytes(new byte[] {45,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n6 = Id.fromMSBBytes(new byte[] {55,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		n7 = Id.fromMSBBytes(new byte[] {65,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
	
		
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
		
		r = Router.createRouter(nis, 3, Router.Type.PASTRY);
		
		nis.add(new NodeInfo(n4, isa4));
		nis.add(new NodeInfo(n5, isa5));
		r5 = Router.createRouter(nis, 3, Router.Type.PASTRY);
		
		nis.add(new NodeInfo(n6, isa6));
		nis.add(new NodeInfo(n7, isa7));
		r7 = Router.createRouter(nis, 5, Router.Type.PASTRY);
	}
	
	@Test
	public void testSimpleLookup() {
		Id id1 =  Id.fromMSBBytes(new byte[] {17,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		InetSocketAddress isa = r.getDest(id1);
		assertEquals("Incorrect routing", isa2, isa);
	}
	
	@Test
	public void testNodeIdLookup() {
		InetSocketAddress isa = r.getDest(n3);
		assertEquals("Incorrect routing for node id", isa3, isa);
	}
	
	@Test
	public void testRangeBottomLookup() {
		Id b1 = null;
		
		for (IdRange range : r.getOwnedRanges(isa1)) {
			b1 = range.getCCW();
			break;
		}
		InetSocketAddress isa = r.getDest(b1);
		assertEquals("Incorrect routing for node range bottom", isa1, isa);
	}

	@Test
	public void testRangeWrap() {
		Id id1 =  Id.fromMSBBytes(new byte[] {50,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		InetSocketAddress isa = r.getDest(id1);
		assertEquals("Incorrect routing for id higher than last node id", isa3, isa);
	}
	
	@Test
	public void testRangeWrap2() {
		Id id1 =  Id.fromMSBBytes(new byte[] {100,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		InetSocketAddress isa = r.getDest(id1);
		assertEquals("Incorrect routing for id higher than last node id", isa3, isa);
	}
	
	@Test
	public void testReplicatedLookup() {
		Id id1 =  Id.fromMSBBytes(new byte[] {7,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		Set<InetSocketAddress> actual = r5.getDests(id1);
		Set<InetSocketAddress> expected = new HashSet<InetSocketAddress>();
		
		expected.add(isa1);
		expected.add(isa2);
		expected.add(isa5);
		
		assertEquals("Incorrect result for replicated lookup", expected, actual);
	}
	
	@Test
	public void testReplicatedLoookUpForMoreThanAvailable() {
		Id id1 =  Id.fromMSBBytes(new byte[] {7,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		Router rr = Router.createRouter(r.getNodeInfo(), 17, Router.Type.PASTRY);
		Set<InetSocketAddress> actual = rr.getDests(id1);
		Set<InetSocketAddress> expected = new HashSet<InetSocketAddress>();
		
		expected.add(isa1);
		expected.add(isa2);
		expected.add(isa3);
		assertEquals("Incorrect result for replicated lookup", expected, actual);		
	}
	
	@Test
	public void testGetDestsForRange() {
		IdRange range1 = new IdRange(Id.fromMSBBytes(new byte[] {7,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}), Id.fromMSBBytes(new byte[] {27,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}));
		Set<InetSocketAddress> dests1 = r5.getDests(range1);
		Set<InetSocketAddress> expected1 = new HashSet<InetSocketAddress>(Arrays.asList(isa1, isa2, isa3));
		assertEquals("Incorrect results for range lookup", expected1, dests1);

		IdRange range2 = new IdRange(Id.fromMSBBytes(new byte[] {42,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}), Id.fromMSBBytes(new byte[] {3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}));
		Set<InetSocketAddress> dests2 = r5.getDests(range2);
		Set<InetSocketAddress> expected2 = new HashSet<InetSocketAddress>(Arrays.asList(isa1, isa5));
		assertEquals("Incorrect results for wrapping range lookup", expected2, dests2);
	}

	@Test
	public void testRecoveryRouter() {
		Router r = r7.createRecoveryRouter(Collections.singleton(isa6));
		IdRange r6 = null;
		for (IdRange range : r7.getOwnedRanges(isa6)) {
			r6 = range;
			break;
		}
		Set<InetSocketAddress> recoveryRanges = r.getDests(r6);
		assertEquals(4, recoveryRanges.size());
	}
	
	@Test
	public void testProblematicRecoveryRouter() {
		List<NodeInfo> nis = Arrays.asList(
				new NodeInfo(Id.fromString("2222000000000000000000000000000000000000"), isa4),
				new NodeInfo(Id.fromString("5555000000000000000000000000000000000000"), isa2),
				new NodeInfo(Id.fromString("8888000000000000000000000000000000000000"), isa5),
				new NodeInfo(Id.fromString("AAAA000000000000000000000000000000000000"), isa3),
				new NodeInfo(Id.fromString("FFFF000000000000000000000000000000000000"), isa1)				
				);
		Router r = Router.createRouter(nis, 5, Router.Type.PASTRY);
		System.out.println(r);
		Router rr = r.createRecoveryRouter(Collections.singleton(isa3));
		System.out.println(rr);
		IdRange r3 = null;
		for (IdRange range : r.getOwnedRanges(isa3)) {
			r3 = range;
			break;
		}
		Set<InetSocketAddress> recoveryRanges = rr.getDests(r3);
		assertEquals(4, recoveryRanges.size());		
	}
	
	@Test
	public void testRecoveryManyNodes() {
		List<NodeInfo> nis = Arrays.asList(
				new NodeInfo(Id.fromString("0000000000000000000000000000000000000000"), isa1),
				new NodeInfo(Id.fromString("2000000000000000000000000000000000000000"), isa2),
				new NodeInfo(Id.fromString("4000000000000000000000000000000000000000"), isa3),
				new NodeInfo(Id.fromString("6000000000000000000000000000000000000000"), isa4),
				new NodeInfo(Id.fromString("8000000000000000000000000000000000000000"), isa5),
				new NodeInfo(Id.fromString("A000000000000000000000000000000000000000"), isa6),
				new NodeInfo(Id.fromString("C000000000000000000000000000000000000000"), isa7),
				new NodeInfo(Id.fromString("E000000000000000000000000000000000000000"), isa8)
				);
		Router r = Router.createRouter(nis, 5, Router.Type.PASTRY);
		System.out.println(r);
		Router rr = r.createRecoveryRouter(Collections.singleton(isa3));
		System.out.println(rr);
		IdRange r3 = null;
		for (IdRange range : r.getOwnedRanges(isa3)) {
			r3 = range;
			break;
		}
		Set<InetSocketAddress> recoveryRanges = rr.getDests(r3);
		assertEquals(4, recoveryRanges.size());
		
		double origSize = r.getOwnedRanges(isa3).getSize();
		double augmentedSize = origSize * 1.25;
		for (InetSocketAddress node : rr.getParticipants()) {
			IdRangeSet owned = rr.getOwnedRanges(node);
			double ownedSize = owned.getSize();
			if (Math.abs(origSize - ownedSize) < (origSize / 1000)) {
				
			} else if (Math.abs(augmentedSize - ownedSize) < (origSize / 1000)) {
				
			} else {
				fail("Owned ranges " + owned + " for " + node + " has size " + ownedSize + ", expected either " + origSize + " or " + augmentedSize);
			}
		}
	}
}
