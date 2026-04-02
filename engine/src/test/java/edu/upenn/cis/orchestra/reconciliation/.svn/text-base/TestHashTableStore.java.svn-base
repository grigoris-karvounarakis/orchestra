package edu.upenn.cis.orchestra.reconciliation;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;

public class TestHashTableStore extends TestStore {

	StateStore getStore(AbstractPeerID ipi, SchemaIDBinding sch, Schema s) throws Exception {
		return new HashTableStore(ipi, sch, -1);
	}

}
