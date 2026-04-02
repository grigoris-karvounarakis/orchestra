package edu.upenn.cis.orchestra.p2pqp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.AbstractImmutableTuple;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.ForeignKey;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.optimization.AtomVariable;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.XMLParseException;

public class QpSchema extends AbstractRelation {
	private static final long serialVersionUID = 1L;

	private int hashCols[] = null;
	private String namedLoc = null;
	private Set<Integer> keyCols = null;
	private int[] keyColsList = null;
	private boolean centralized = false;
	private boolean replicated = false;

	private boolean locationFinished = false;
	public final int relId;

	public QpSchema(String name, int relId) {
		super(name);
		this.relId = relId;
		
	}
	
	public static QpSchema rehashQpSchema(QpSchema qpSchema, int[] origHashCols, int [] newHashCol, int relID) {
		if (!qpSchema.isFinished()) {
			throw new IllegalStateException("qpSchema has not finished");
		}
		QpSchema newSchema = new QpSchema(qpSchema.getName()+"_REHASH", relID); 
		for (int i = 0; i < qpSchema.getNumCols(); i++) {
			try {
			newSchema.addCol(qpSchema.getColName(i), "", qpSchema.getColType(i));
			} catch (BadColumnName bcn) {
			}
		}
		newSchema.setHashCols(newHashCol);
		newSchema.setPrimaryKey(qpSchema.getPrimaryKey());
		newSchema.markFinished();
        return newSchema;
	}

	
	public void setHashColsFromFields(Set<RelationField> cols) {
		Set<String> colNames = new HashSet<String>(cols.size());
		for (RelationField f : cols) {
			colNames.add(f.getName());
		}
		setHashCols(colNames);
	}

	public void setHashCols(Set<String> cols) {
		int[] colPos = new int[cols.size()];
		int pos = 0;
		for (String col : cols) {
			Integer colNum = this.getColNum(col);
			if (colNum == null) {
				throw new IllegalArgumentException("Column " + col + " is not in relation");
			}
			colPos[pos++] = colNum;
		}
		setHashCols(colPos);
	}
	
	public void setHashCols(List<Integer> cols) {
		int[] colPos = new int[cols.size()];
		for (Integer col : cols) {
			if (col == null) {
				throw new IllegalArgumentException("Column " + col + " is not in relation");
			}
			colPos[col.intValue()] = col;
		}
		setHashCols(colPos);
	}
	

	public void setHashCols(int[] cols) {
		if (cols == null) {
			throw new NullPointerException("Cannot supply null array for hash cols");
		}
		if (cols.length == 0) {
			throw new NullPointerException("Cannot supply an empty array for hash cols");
		}
		if (locationFinished) {
			throw new IllegalStateException("Cannot change location of finished schema");
		}

		hashCols = new int[cols.length];
		System.arraycopy(cols, 0, hashCols, 0, cols.length);
		Arrays.sort(hashCols);
		if (hashCols[0] < 0 || hashCols[hashCols.length - 1] > (this.getNumCols() - 1)) {
			throw new IllegalArgumentException("All hash cols must be in the range [0,numCols)");
		}
		namedLoc = null;
		centralized = false;
		replicated = false;
	}

	public void setCentralized() {
		if (locationFinished) {
			throw new IllegalStateException("Cannot change location of finished schema");
		}
		namedLoc = null;
		hashCols = null;
		centralized = true;
		replicated = false;
	}

	public void setReplicated() {
		if (locationFinished) {
			throw new IllegalStateException("Cannot change location of finished schema");
		}
		namedLoc = null;
		hashCols = null;
		centralized = false;
		replicated = true;
	}

	public void setNamedLocation(String name) {
		if (name == null) {
			throw new NullPointerException("Cannot supply a null name as a location");
		}
		if (locationFinished) {
			throw new IllegalStateException("Cannot change location of a finished schema");
		}
		if (name.length() == 0) {
			throw new IllegalArgumentException("'' is not a valid location name");
		}
		hashCols = null;
		namedLoc = name;
		centralized = false;
		replicated = false;
	}
	
	

	public int[] getHashCols() {
		if (hashCols == null) {
			throw new IllegalStateException("Schema is not striped");
		}

		int[] retval = new int[hashCols.length];
		System.arraycopy(hashCols, 0, retval, 0, retval.length);
		return retval;
	}

	byte[] getBytesForId(AbstractImmutableTuple<QpSchema> t, byte[] scratch) {
		return getBytesForId(t, hashCols, scratch);
	}
	
	byte[] getBytesForId(AbstractImmutableTuple<QpSchema> t, int[] cols, byte[] scratch) {
		final int length = cols.length * IntType.bytesPerInt;
		byte[] retval;
		if (scratch != null) {
			if (scratch.length == length) {
				retval = scratch;
			} else {
				throw new IllegalArgumentException("Supplied scratch buffer has length " + scratch.length + ", but should have length " + length);
			}
		} else {
			retval = new byte[length];
		}
		int pos = 0;
		for (int col : cols) {
			int code = t.getColHashCode(col);
			IntType.putBytes(code, retval, pos);
			pos += IntType.bytesPerInt;
		}
		return retval;
	}

	byte[] getBytesForId(byte[] abstractTuple, int offset, int length, boolean onlyKey) {
		return getBytesForId(abstractTuple,offset,length,onlyKey,this.hashCols,null);
	}
	
	byte[] getBytesForId(byte[] abstractTuple, int offset, int length, boolean onlyKey, int[] cols) {
		return getBytesForId(abstractTuple,offset,length,onlyKey,cols,null);
	}
	
	byte[] getBytesForId(byte[] abstractTuple, int offset, int length, boolean onlyKey, byte[] scratch) {
		return getBytesForId(abstractTuple,offset,length,onlyKey,this.hashCols, scratch);
	}
	
	byte[] getBytesForId(byte[] abstractTuple, int offset, int length, boolean onlyKey, int[] cols, byte[] scratch) {
		final int retvalLength = cols.length * IntType.bytesPerInt;
		byte[] retval;
		if (scratch != null) {
			if (scratch.length == retvalLength) {
				retval = scratch;
			} else {
				throw new IllegalArgumentException("Supplied scratch buffer has length " + scratch.length + ", but should have length " + retvalLength);
			}
		} else {
			retval = new byte[length];
		}
		int pos = 0;
		for (int col : cols) {
			int code = this.getColHashCode(abstractTuple, onlyKey, offset, length, col);
			IntType.putBytes(code, retval, pos);
			pos += IntType.bytesPerInt;
		}
		return retval;
		
	}
		
	public int getNumHashCols() {
		if (hashCols == null) {
			throw new IllegalStateException("Schema is not striped");
		}
		return hashCols.length;
	}

	public String getNamedLocation() {
		if (namedLoc == null) {
			throw new IllegalStateException("Schema is not at a named location");
		}
		return namedLoc;
	}

	public Location getLocation() {
		if (namedLoc != null) {
			return Location.NAMED;
		} else if (hashCols != null) {
			return Location.STRIPED;
		} else if (centralized) {
			return Location.CENTRALIZED;
		} else if (replicated) {
			return Location.REPLICATED;
		} else {
			return null;
		}
	}

	public enum Location {
		CENTRALIZED, STRIPED, NAMED, REPLICATED
	}

	public edu.upenn.cis.orchestra.optimization.Location getOptimizerLocation() {
		if (namedLoc != null) {
			return new edu.upenn.cis.orchestra.optimization.Location(namedLoc);
		} else if (hashCols != null) {
			final String relName = getName();
			HashSet<AtomVariable> hashVars = new HashSet<AtomVariable>(hashCols.length);
			for (int hashCol : hashCols) {
				hashVars.add(new AtomVariable(relName, 1, hashCol, this.getColType(hashCol).getOptimizerType()));
			}
			return new edu.upenn.cis.orchestra.optimization.Location(hashVars);
		} else if (centralized) {
			return edu.upenn.cis.orchestra.optimization.Location.CENTRALIZED;
		} else if (replicated) {
			return edu.upenn.cis.orchestra.optimization.Location.FULLY_REPLICATED;
		} else {
			throw new IllegalStateException("Location is not specified");
		}
	}

	public void addForeignKey(String fkName, List<String> thisCols, QpSchema rel, List<String> relCols) throws UnknownRefFieldException {
		ForeignKey fk = new ForeignKey(fkName, this, thisCols, rel, relCols);
		this.addForeignKey(fk);
	}

	public void markFinishedExceptLocation() {
		super.markFinished();
		keyCols = Collections.unmodifiableSet(new HashSet<Integer>(this.getKeyCols()));
	}

	public void markFinished() {
		if (! finished) {
			markFinishedExceptLocation();
		}
		locationFinished = true;
		keyColsList = new int[keyCols.size()];
		int pos = 0;
		for (int col : getKeyCols()) {
			keyColsList[pos++] = col;
		}
		Arrays.sort(keyColsList);
	}

	public Set<Integer> getKeyColsSet() {
		return keyCols;
	}

	int[] getKeyColsListNoCopy() {
		return keyColsList;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(getName() + "(");
		int numCols = this.getNumCols();
		for (int i = 0; i < numCols; ++i) {
			RelationField f = this.getField(i);
			sb.append(f.getName() + " : " + f.getType());
			if (i != (numCols -1)) {
				sb.append(", ");
			}
		}
		sb.append(")");
		return sb.toString();
	}

	public Element serialize(Document doc) {
		Element el = doc.createElement("QpSchema");
		super.serialize(doc, el);

		if (namedLoc != null) {
			el.setAttribute("location", namedLoc);
		} else if (hashCols != null) {
			Element location = DomUtils.addChild(doc, el, "location");
			for (int col : hashCols) {
				Element colEl = DomUtils.addChild(doc, location, "hashCol");
				colEl.setAttribute("pos", Integer.toString(col));
			}
		} else if (centralized) {
			el.setAttribute("centralized", "true");
		}

		el.setAttribute("relId", Integer.toString(relId));

		return el;
	}

	public static QpSchema deserialize(Element el, GetSchema<QpSchema> schemas) throws XMLParseException {
		if (! el.getTagName().equals("QpSchema")) {
			throw new XMLParseException("Attempt to decode QpSchema from element with tag " + el.getTagName(), el);
		}

		try {
			AbstractRelationInfo info = deserializeAbstractRelation(el);

			String relIdString = el.getAttribute("relId");
			if (relIdString.length() == 0) {
				throw new XMLParseException("Missing relId attribute", el);
			}
			int relId;
			try {
				relId = Integer.parseInt(relIdString);
			} catch (NumberFormatException nfe) {
				throw new XMLParseException("Error parsing relation ID", nfe, el);
			}

			QpSchema s = new QpSchema(info.name, relId);
			info.decode(s, schemas);

			String namedLoc = el.getAttribute("location");
			String centralized = el.getAttribute("centralized");
			if (namedLoc.length() > 0) {
				s.setNamedLocation(namedLoc);
			} else if (centralized.length() > 0) {
				s.setCentralized();
			} else {
				Element loc = DomUtils.getChildElementByName(el, "location");
				if (loc == null) {
					s.setCentralized();
				} else {
					List<Element> hashCols = DomUtils.getChildElementsByName(loc, "hashCol");
					Set<String> hashColNames = new HashSet<String>();
					for (Element hashCol : hashCols) {
						String pos = hashCol.getAttribute("pos");
						if (pos.length() == 0) {
							throw new XMLParseException("Missing pos attribute", hashCol);
						}
						try {
							int posInt = Integer.parseInt(pos);
							hashColNames.add(s.getColName(posInt));
						} catch (NumberFormatException nfe) {
							throw new XMLParseException(nfe,hashCol);
						}
					}
					s.setHashCols(hashColNames);
				}
			}

			s.markFinished();
			return s;
		} catch (Exception e) {
			throw new XMLParseException(e, el);
		}
	}

	public interface Source {
		QpSchema getSchema(int tableId) throws IllegalArgumentException;
		QpSchema getSchema(String tableName) throws IllegalArgumentException;
	}

	public static class CollectionSource implements Source {
		private Map<Integer,QpSchema> schemasById;
		private Map<String,QpSchema> schemasByName;		

		public CollectionSource(QpSchema... schemas) {
			schemasById = new HashMap<Integer,QpSchema>(schemas.length);
			schemasByName = new HashMap<String,QpSchema>(schemas.length);

			for (QpSchema s : schemas) {
				schemasById.put(s.relId, s);
				schemasByName.put(s.getName(), s);
			}			
		}
		
		public CollectionSource(Collection<? extends QpSchema> schemas) {
			schemasById = new HashMap<Integer,QpSchema>(schemas.size());
			schemasByName = new HashMap<String,QpSchema>(schemas.size());

			for (QpSchema s : schemas) {
				schemasById.put(s.relId, s);
				schemasByName.put(s.getName(), s);
			}
		}

		public QpSchema getSchema(int tableId) throws IllegalArgumentException {
			QpSchema result = schemasById.get(tableId);
			if (result == null) {
				throw new IllegalArgumentException("Relation " + tableId + " not known");
			}
			return result;
		}

		public QpSchema getSchema(String tableName)
		throws IllegalArgumentException {
			QpSchema result = schemasByName.get(tableName);
			if (result == null) {
				throw new IllegalArgumentException("Relation " + tableName + " not known");
			}
			return result;
		}

	}

	public static class SingleSource implements Source {
		private final QpSchema s;

		public SingleSource(QpSchema s) {
			this.s = s;
		}


		public QpSchema getSchema(int tableId) throws IllegalArgumentException {
			if (s.relId == tableId) {
				return s;
			} else {
				throw new IllegalArgumentException("Table " + tableId + " not known");
			}
		}

		public QpSchema getSchema(String tableName)
		throws IllegalArgumentException {
			if (tableName.equals(s.getName())) {
				return s;
			} else {
				throw new IllegalArgumentException("Table " + tableName + " not known");
			}
		}

	}

	@Override
	public boolean quickEquals(AbstractRelation ar) {
		QpSchema qps = (QpSchema) ar;
		return this.relId == qps.relId;
	}
	
	public static List<QpSchema> sortByFKs(Collection<QpSchema> schemas, Set<String> relevantSchemaNames) {
		Set<String> alreadyOutput = new HashSet<String>(schemas.size());
		List<QpSchema> retval = new ArrayList<QpSchema>(schemas.size());
		
		while (retval.size() < schemas.size()) {
			SCHEMA: for (QpSchema schema : schemas) {
				if (alreadyOutput.contains(schema.getName())) {
					continue;
				}
				for (ForeignKey fk : schema.getForeignKeys()) {
					final String refRel = fk.getRefRelation().getName();
					if (relevantSchemaNames.contains(refRel) && (! alreadyOutput.contains(refRel))) {
						continue SCHEMA;
					}
				}
				alreadyOutput.add(schema.getName());
				retval.add(schema);
			}
		}
		return retval;
	}
	
}
