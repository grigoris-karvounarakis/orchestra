/**
 * 
 */
package edu.upenn.cis.orchestra.reconciliation.p2pstore;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import rice.pastry.Id;

public class PastryIdFactory implements IdFactory {
	public Id getIdFromContent(byte[] material) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(material);
			return Id.build(md.digest());
		} catch (NoSuchAlgorithmException nsae) {
			throw new RuntimeException("Couldn't get SHA-1 digester!");
		}
	}

	public Id getIdFromByteArray(byte[] bytes) {
		return Id.build(bytes);
	}
	
}