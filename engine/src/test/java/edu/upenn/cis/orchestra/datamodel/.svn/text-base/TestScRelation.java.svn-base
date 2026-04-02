package edu.upenn.cis.orchestra.datamodel;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.orchestra.datamodel.exceptions.InvalidBeanException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.repository.model.beans.ScConstraintBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScFieldBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScRelationBean;

public class TestScRelation extends TestCase {

	private List<RelationField> _2fields;
	private ScRelationBean _emptyBean;
	private ScFieldBean _fldBean1;
	private ScFieldBean _fldBean2;

	protected Relation _rel1, _rel2;

	@Override
	@Before
	public void setUp() throws UnsupportedTypeException {
		_2fields = new ArrayList<RelationField>();
		RelationField fld = new RelationField("fld1", "fld1", false, "bool");
		_2fields.add(fld);
		fld = new RelationField("fld2", "fld2", true, "clob(1024)");
		_2fields.add(fld);

		_emptyBean = new ScRelationBean();
		_emptyBean.setDbCatalog("catalog");
		_emptyBean.setDbSchema("schema");
		_emptyBean.setDbRelName("name");
		_emptyBean.setName("name");
		_emptyBean.setDescription("description");
		_emptyBean.setMaterialized(true);
		_emptyBean.setFields(null);
		_emptyBean.setDirectConstraints(null);
		_emptyBean.setForeignKeys(null);

		_fldBean1 = new ScFieldBean();
		_fldBean1.setName("field1");
		_fldBean1.setDescription("description1");
		_fldBean1.setNullable(false);
		_fldBean1.setType(OrchestraTypesTranslator.typeFromString("FLOAT"));
		_fldBean1.setDbType("float");

		_fldBean2 = new ScFieldBean();
		_fldBean2.setName("field1");
		_fldBean2.setDescription("description1");
		_fldBean2.setNullable(true);
		_fldBean2.setType(OrchestraTypesTranslator.typeFromString("FLOAT"));
		_fldBean2.setDbType("float");

		RelationField field1 = new RelationField("field1", "descr1", false, "double");
		RelationField field2 = new RelationField("field2", "descr2", false, "double");
		RelationField field3 = new RelationField("field3", "descr3", false, "double");
		RelationField field4 = new RelationField("field4", "descr4", false, "double");
		List<RelationField> fields = new ArrayList<RelationField>();
		fields.add(field1.deepCopy());
		fields.add(field2.deepCopy());
		fields.add(field3.deepCopy());
		fields.add(field4.deepCopy());
		_rel1 = new Relation("dbCat", "dbSchem", "dbName1", "name1",
				"descr1", true, true, fields);
		fields = new ArrayList<RelationField>();
		fields.add(field1.deepCopy());
		fields.add(field2.deepCopy());
		fields.add(field3.deepCopy());
		fields.add(field4.deepCopy());
		_rel2 = new Relation("dbCat", "dbSchem", "dbName1", "name2",
				"descr1", false, true, fields);

	}

	@Test
	public void testBasicProperties() throws InvalidBeanException, UnsupportedTypeException {
		List<RelationField> flds = new ArrayList<RelationField>(_2fields);

		Relation rel = new Relation("dbCatal", "dbSchema", "dbName",
				"name", "description", true, true, flds);
		Relation rel2 = rel.deepCopy();

		assertTrue(rel2.getDbCatalog().equals("dbCatal"));
		assertTrue(rel2.getDbSchema().equals("dbSchema"));
		assertTrue(rel2.getName().equals("name"));
		assertTrue(rel2.getDescription().equals("description"));
		assertTrue(rel2.isMaterialized());
		assertTrue(rel2.getFields().size() == 2);
		// modif list fields pour voir si chgt
		flds.remove(0);
		assertTrue(rel.getFields().size() == 2);
		assertTrue(rel2.getFields().size() == 2);
		rel.getFields().remove(0);
		assertTrue(rel2.getFields().size() == 2);

		ScRelationBean bean = rel2.toBean();
		assertTrue(bean.getDbCatalog().equals("dbCatal"));
		assertTrue(bean.getDbSchema().equals("dbSchema"));
		assertTrue(bean.getName().equals("name"));
		assertTrue(bean.getDescription().equals("description"));
		assertTrue(bean.isMaterialized());
		assertTrue(bean.getFields().size() == 2);

		ScRelationBean bean2 = new ScRelationBean(bean);
		assertTrue(bean2.getDbCatalog().equals("dbCatal"));
		assertTrue(bean2.getDbSchema().equals("dbSchema"));
		assertTrue(bean2.getName().equals("name"));
		assertTrue(bean2.getDescription().equals("description"));
		assertTrue(bean2.isMaterialized());
		assertTrue(bean2.getFields().size() == 2);

		Relation rel3 = new Relation(bean2);
		assertTrue(rel3.getDbCatalog().equals("dbCatal"));
		assertTrue(rel3.getDbSchema().equals("dbSchema"));
		assertTrue(rel3.getName().equals("name"));
		assertTrue(rel3.getDescription().equals("description"));
		assertTrue(rel3.isMaterialized());
		assertTrue(rel3.getFields().size() == 2);

		// Foreign key is not mandatory, default must be null
		assertTrue(rel3.getPrimaryKey() == null);
	}

	@Test
	public void testBeanNullLists() throws InvalidBeanException, UnsupportedTypeException {
		// Init basic bean
		ScRelationBean bean = new ScRelationBean(_emptyBean);

		// It is mandatory to have at least one field, should fail
		createRelShouldFail(bean, true);

		// Loading from bean must support null lists since most of DAO layers
		// will
		// set the list to null if there is no data
		List<ScFieldBean> lstFld = new ArrayList<ScFieldBean>();
		ScFieldBean fldBean = new ScFieldBean(_fldBean1);
		lstFld.add(fldBean);
		bean.setFields(lstFld);
		ScRelationBean bean2 = new ScRelationBean(bean);

		Relation rel = new Relation(bean2);
		assertTrue(rel.getFields().size() == 1);
		// List should be an empty list, not null
		assertTrue(rel.getForeignKeys().size() == 0);
		assertTrue(rel.getUniqueIndexes().size() == 0);
		assertTrue(rel.getPrimaryKey() == null);
	}

	@SuppressWarnings(value = { "unused" })
	@Test
	public void testConstrainsDirectCreation() throws UnknownRefFieldException {
		Relation rel1 = _rel1.deepCopy();
		Relation rel2 = _rel2.deepCopy();

		PrimaryKey pk1 = new PrimaryKey("pk1", rel1, new String[] {
				"field1", "field2" });
		assertTrue(pk1.getFields().size() == 2);
		rel1.setPrimaryKey(pk1);

		try {
			pk1 = new PrimaryKey("pk1", rel1, new String[] { "field1",
					"field5" });
			assertTrue(false);
		} catch (UnknownRefFieldException ex) {
			assertTrue(true);
		}

		RelationIndexUnique uk1 = new RelationIndexUnique("uk1", rel2, new String[] {
				"field1", "field2" });
		assertTrue(uk1.getFields().size() == 2);
		rel1.addUniqueIndex(uk1);
		assertTrue(rel1.getUniqueIndexes().size() == 1);
		assertTrue(rel1.getUniqueIndexes().get(0).toString().equals(
				uk1.toString()));
		rel1.removeUniqueIndex(uk1);
		assertTrue(rel1.getUniqueIndexes().size() == 0);
		rel1.addUniqueIndex(uk1);
		rel1.clearUniqueIndexes();
		assertTrue(rel1.getUniqueIndexes().size() == 0);

		try {
			uk1 = new RelationIndexUnique("uk1", rel2, new String[] { "field1",
					"field6" });
			assertTrue(false);
		} catch (UnknownRefFieldException ex) {
			assertTrue(true);
		}

		try {
			RelationIndexNonUnique nuidx1 = new RelationIndexNonUnique("nuidx1", rel2,
					new String[] { "field1", "field5", "field6" });
			assertTrue(false);
		} catch (UnknownRefFieldException ex) {
			assertTrue(true);
		}

		RelationIndexNonUnique nuidx1 = new RelationIndexNonUnique("nuidx1", rel2,
				new String[] { "field1", "field2" });
		rel1.addNonUniqueIndex(nuidx1);
		assertTrue(rel1.getNonUniqueIndexes().size() == 1);
		assertTrue(rel1.getNonUniqueIndexes().get(0).getFields().size() == 2);

		rel1.removeNonUniqueIndex(nuidx1);
		assertTrue(rel1.getNonUniqueIndexes().size() == 0);
		rel1.addNonUniqueIndex(nuidx1);
		rel1.clearNonUniqueIndexes();
		assertTrue(rel1.getNonUniqueIndexes().size() == 0);

		ForeignKey fk1 = new ForeignKey("fk", rel2, new String[] {
				"field3", "field4" }, rel1, new String[] { "field3", "field4" });
		assertTrue(fk1.getFields().size() == 2);
		assertTrue(fk1.getRefFields().size() == 2);
		rel1.addForeignKey(fk1);
		assertTrue(rel1.getForeignKeys().size() == 1);
		assertTrue(rel1.getForeignKeys().get(0).toString().equals(
				fk1.toString()));
		rel1.removeForeignKey(fk1);
		assertTrue(rel1.getForeignKeys().size() == 0);
		rel1.addForeignKey(fk1);
		rel1.clearForeignKeys();
		assertTrue(rel1.getForeignKeys().size() == 0);

		try {
			fk1 = new ForeignKey("fk", rel2, new String[] { "field3",
					"field6" }, rel1, new String[] { "field3", "field4" });
			assertTrue(false);
		} catch (UnknownRefFieldException ex) {
			assertTrue(true);
		}

		try {
			fk1 = new ForeignKey("fk", rel2, new String[] { "field3",
					"field4" }, rel1, new String[] { "field3", "field7" });
			assertTrue(false);
		} catch (UnknownRefFieldException ex) {
			assertTrue(true);
		}
	}

	@Test
	public void testConstraintsInBeanConversions() {
		ScRelationBean bean = new ScRelationBean(_emptyBean);
		List<ScFieldBean> lstFld = new ArrayList<ScFieldBean>();
		ScFieldBean fldBean = new ScFieldBean(_fldBean1);
		lstFld.add(fldBean);
		fldBean = new ScFieldBean(_fldBean2);
		lstFld.add(fldBean);
		bean.setFields(lstFld);

		List<ScConstraintBean> directCst = new ArrayList<ScConstraintBean>();
		ScConstraintBean cstBean = new ScConstraintBean();
		cstBean.setType("P");
		cstBean.setName("PK");
		List<String> strFlds = new ArrayList<String>();
		strFlds.add("field1");
		cstBean.setFields(strFlds);
		directCst.add(cstBean);

		cstBean = new ScConstraintBean();
		cstBean.setType("U");
		cstBean.setName("UNIQ");
		strFlds = new ArrayList<String>();
		strFlds.add("field2");
		directCst.add(cstBean);

		bean.setDirectConstraints(directCst);

		// Foreign keys will be tested from schema (schema needed for
		// conversion)
		try {
			ScRelationBean bean2 = new ScRelationBean(bean);
			Relation rel = new Relation(bean2);

			assertTrue(rel.getPrimaryKey().getName().equals("PK"));
			assertTrue(rel.getPrimaryKey().getFields().size() == 1);

			assertTrue(rel.getUniqueIndexes().size() == 1);
			assertTrue(rel.getUniqueIndexes().get(0).getName().equals("UNIQ"));

			Relation rel2 = rel.deepCopy();
			rel.getUniqueIndexes().remove(0);
			assertTrue(rel2.getUniqueIndexes().size() == 1);

			bean2 = rel2.toBean();
			assertTrue(bean2.getDirectConstraints().size() == 2);

		} catch (Exception ex) {
			ex.printStackTrace();
			assertTrue(false);
		}

	}

	@Test
	public void testUnknownConstraintsFieldsFromBean() throws UnsupportedTypeException {
		ScRelationBean bean = new ScRelationBean(_emptyBean);
		List<ScFieldBean> lstFld = new ArrayList<ScFieldBean>();
		ScFieldBean fldBean = new ScFieldBean(_fldBean1);
		lstFld.add(fldBean);
		bean.setFields(lstFld);

		ScConstraintBean cstBean = new ScConstraintBean();
		cstBean.setName("PK");
		cstBean.setType("P");
		List<String> fields = new ArrayList<String>();
		fields.add("unknown");
		cstBean.setFields(fields);

		List<ScConstraintBean> cstLst = new ArrayList<ScConstraintBean>();
		cstLst.add(cstBean);
		bean.setDirectConstraints(cstLst);

		createRelShouldFail(bean, true);
	}

	@Test
	public void testMandatoryBeanSimpleFields() throws UnsupportedTypeException {
		ScRelationBean bean = new ScRelationBean(_emptyBean);
		List<ScFieldBean> fields = new ArrayList<ScFieldBean>();
		fields.add(_fldBean1);
		bean.setFields(fields);
		bean.setDbRelName(null);
		createRelShouldFail(bean, true);

		bean.setDbRelName("dbName");
		bean.setName(null);
		createRelShouldFail(bean, true);

		bean.setName("name");
		bean.setDbCatalog(null);
		bean.setDbSchema(null);
		createRelShouldFail(bean, false);
	}

	@SuppressWarnings(value = { "unused" })
	public void createRelShouldFail(ScRelationBean bean, boolean shouldFail) throws UnsupportedTypeException {
		try {
			Relation rel = new Relation(bean);
			assertTrue(!shouldFail);
		} catch (InvalidBeanException ex) {
			assertTrue(shouldFail);
		}
	}

}
