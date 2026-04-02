package edu.upenn.cis.orchestra.p2pqp;

import java.util.Collection;

import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;

/**
 * Operations related to metadata
 * 
 * @author netaylor
 *
 * @param <M>		The type of the metadata
 */
public interface MetadataFactory<M> {
	Class<M> getMetadataClass();
	
	/**
	 * Generate the metadata for a scanned tuple
	 * 
	 * @param o				The operator doing the scan
	 * @param tuple			The tuple being scanned
	 * @return				The metadata supplied by the underlying
	 * 						storage, if any
	 */
	M scan(Operator<M> o, QpTuple<?> tuple, M inStorage);
		
	/**
	 * Generate the metadata record for an aggregate tuple
	 * 
	 * @param o				The operator performing the aggregation
	 * @param inputs		The metadata for the input tuples
	 * @return				The metadata for the output tuple
	 */
	M agg(HashAggregator<M> o, Collection<M> inputs);
	
	/**
	 * Print a metadata to a string
	 * @param m		The metadata record
	 * 
	 * @return		The string representation of the metadata
	 */
	String printMetadata(M m);
	
	/**
	 * Serialize a metadata record to a byte array
	 * @param m		The metadata record
	 * 
	 * @return		The byte array encoding of it
	 */
	byte[] toBytes(M m);
	
	/**
	 * Deserialize a the byte array representation of a
	 * metadata record
	 * @param bytes		The byte array containing an encoding of a metadata record
	 * @param offset	The offset of the metadata record in the byte array
	 * @param length	The length of the metadata record in the byte array
	 * @return		The decoded metadata record
	 */
	M fromBytes(Source findSchema, byte[] bytes, int offset, int length);
	
	
	/**
	 * Determine if tuples with a particular metadata value should be deleted from an operator's
	 * local storage
	 * 
	 * @param m			The metadata value
	 * @return			<code>true</code> if they should be deleted, <code>false</code<>
	 * 					if they should not
	 */
	boolean isZero(M m);
	
	/**
	 * Get the zero value for this kind of metadata
	 * 
	 * @return			A piece of metadata representing zero
	 */
	M zero();
	
	/**
	 * The difference metadata from old to new metadata
	 * 
	 * @param oldMetadata		The original metadata
	 * @param newMetadata		The new metadata
	 * @return					The difference metadata
	 */
	M differenceMetadata(M newMetadata, M oldMetadata);
	
	/**
	 * Determine the result of adding new metadata to existing metadata
	 * for a particular tuple
	 * 
	 * @param newMetadata
	 * @param oldMetadata
	 * @return
	 */
	M addMetadata(M newMetadata, M oldMetadata);	
	
	/**
	 * Determine the metadata resulting from joining a tuple with one piece of metadata
	 * to another tuple with a different piece of metadata
	 * 
	 * @param metadata1			Metadata from one tuple
	 * @param metadata2			Metadata from another tuple
	 * @return					The joined metadata
	 */
	
	M multiplyMetadata(M metadata1, M metadata2);
	
	/**
	 * Return the cardinality of the tuple annotated with this piece of metadata
	 * 
	 * @param metadata		The metadata
	 * @return				Its cardinality
	 */
	int getCardinality(M metadata);
	
	/**
	 * Determine if two pieces of metadata are equal
	 * 
	 * @param m1			A piece of metadata
	 * @param m2			Another piece of metadata
	 * @return				<code>true</code> if they are equal, <code>false</code> if they are not
	 */
	boolean equals(M m1, M m2);
	
	/**
	 * Derive a hashcode from a piece of metadata
	 * 
	 * @param metadata		A piece of metadata
	 * @return				Its hashcode
	 */
	int hashCode(M metadata);
	
}