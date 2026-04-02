package edu.upenn.cis.orchestra.p2pqp;

import java.util.Iterator;

import edu.upenn.cis.orchestra.util.ByteArraySet;


public interface KeySource {
	int getNumKeys();
	Iterator<QpTupleKey> getKeys(QpSchema.Source findSchema);
	void addKeysTo(ByteArraySet set);
}
