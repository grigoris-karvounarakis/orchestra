package edu.upenn.cis.data;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.ForeignKey;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.p2pqp.Null;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTuple;

public class BioData {
	public static <S extends AbstractRelation> Map<String,S> createSchemas(CreateSchema<S> cs) {
		Map<String,S> schemas = new HashMap<String,S>();

		try {
			S species = cs.createSchema("SPECIES");
			species.addCol("speciesId", new IntType(false, false));
			species.addCol("genus", new StringType(false, false,true,20));
			species.addCol("species", new StringType(false, false,true,20));
			species.setPrimaryKey(Collections.singleton("speciesId"));
			cs.finalize(species);
			schemas.put(species.getName(), species);

			S db1 = cs.createSchema("DB1");
			db1.addCol("pName", new StringType(false, false,false,8));
			db1.addCol("speciesId", new IntType(false, false));
			db1.addCol("desc", new StringType(false, false,true,75));
			db1.setPrimaryKey(Collections.singleton("pName"));
			db1.addForeignKey(new ForeignKey("DB1_fk", db1, Collections.singletonList("speciesId"), species, Collections.singletonList("speciesId")));
			cs.finalize(db1);
			schemas.put(db1.getName(), db1);

			S db1todb2 = cs.createSchema("DB1toDB2");
			db1todb2.addCol("pName", new StringType(false, false,false,8));
			db1todb2.addCol("pId", new IntType(false, false));
			db1todb2.setPrimaryKey(Arrays.asList("pName", "pId"));
			db1todb2.addForeignKey(new ForeignKey("DB1toDB2_fk", db1todb2, Collections.singletonList("pName"), db1, Collections.singletonList("pName")));
			cs.finalize(db1todb2);
			schemas.put(db1todb2.getName(), db1todb2);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return schemas;
	}

	public static Map<String,QpSchema> createQpSchemas(int firstRelId) {
		Map<String,Set<String>> hashCols = new HashMap<String,Set<String>>();
		hashCols.put("SPECIES", Collections.singleton("speciesId"));
		hashCols.put("DB1", Collections.singleton("pName"));
		hashCols.put("DB1toDB2", Collections.singleton("pName"));
		return createSchemas(new CreateQpSchema(firstRelId,hashCols));
	}

	public final Iterator<QpTuple<Null>> speciesIt, db1It, db1todb2It;
	public final QpSchema db1Schema, db1todb2Schema, speciesSchema;

	private static final char[] chars = new char[] { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
		'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};

	public BioData(Map<String,QpSchema> schemas, final int numProts, final int numSpecies, final int protsPerRef) {
		db1Schema = schemas.get("DB1");
		db1todb2Schema = schemas.get("DB1toDB2");
		speciesSchema = schemas.get("SPECIES");

		final Random r = new Random();
		final StringBuilder sb = new StringBuilder();

		if (db1Schema == null || db1todb2Schema == null || speciesSchema == null) {
			throw new IllegalArgumentException("Schemas must include DB1, DB1toDB2, and SPECIES");
		}
		
		speciesIt = new Iterator<QpTuple<Null>>() {
			int speciesNum = 0;
			@Override
			public boolean hasNext() {
				return speciesNum < numSpecies;
			}

			@Override
			public QpTuple<Null> next() {
				if (speciesNum >= numSpecies) {
					throw new NoSuchElementException();
				}

				QpTuple<Null> retval;
				sb.setLength(0);
				sb.append("Genus ");
				sb.append(speciesNum);
				sb.append(' ');
				while (sb.length() < 20) {
					sb.append(chars[r.nextInt(chars.length)]);
				}
				String genus = sb.toString();
				sb.setLength(0);
				sb.append("Species ");
				sb.append(speciesNum);
				sb.append(' ');
				while (sb.length() < 20) {
					sb.append(chars[r.nextInt(chars.length)]);
				}
				String species = sb.toString();
				try {
					retval = new QpTuple<Null>(speciesSchema, new Object[] {speciesNum, genus, species});
				} catch (ValueMismatchException e) {
					throw new RuntimeException("Error creating SPECIES tuple", e);
				}
				++speciesNum;
				return retval;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

		};
		db1It = new Iterator<QpTuple<Null>>() {
			int protCount = 0;
			@Override
			public boolean hasNext() {
				return protCount < numProts;
			}

			@Override
			public QpTuple<Null> next() {
				if (protCount >= numProts) {
					throw new NoSuchElementException();
				}
				sb.setLength(0);
				int speciesNum = protCount % numSpecies;
				sb.append(protCount);
				while (sb.length() < 8) {
					sb.append('A');
				}
				String nameStr = sb.toString();
				sb.setLength(0);
				sb.append("Description #");
				sb.append(protCount);
				sb.append(' ');
				while (sb.length() < 40) {
					sb.append(chars[r.nextInt(chars.length)]);
				}
				String desc = sb.toString();
				QpTuple<Null> retval;
				try {
					retval = new QpTuple<Null>(db1Schema, new Object[] {nameStr, speciesNum, desc});
				} catch (ValueMismatchException e) {
					throw new RuntimeException("Error creating DB1 tuple", e);
				}
				++protCount;
				return retval;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

		};
		db1todb2It = new Iterator<QpTuple<Null>>() {
			int protCount = 0;
			int db2Id = 0;
			@Override
			public boolean hasNext() {
				return protCount < numProts;
			}

			@Override
			public QpTuple<Null> next() {
				sb.setLength(0);
				sb.append(protCount);
				while (sb.length() < 8) {
					sb.append('A');
				}
				String nameStr = sb.toString();
				QpTuple<Null> retval;
				try {
					retval = new QpTuple<Null>(db1todb2Schema, new Object[] {nameStr, db2Id++});
				} catch (ValueMismatchException e) {
					throw new RuntimeException("Error creating SPECIES tuple", e);
				}
				protCount += protsPerRef;
				return retval;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

		};
	}
}
