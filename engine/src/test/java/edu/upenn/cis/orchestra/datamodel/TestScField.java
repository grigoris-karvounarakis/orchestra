package edu.upenn.cis.orchestra.datamodel;

import org.junit.Test;

import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.repository.model.beans.ScFieldBean;

public class TestScField extends junit.framework.TestCase {


	@Test
	public void testBasicProperties () throws UnsupportedTypeException
	{
		RelationField fld = new RelationField ("field1", "dfield1", true, "INT");
		
		// Check that deep copy keeps the field properties
		RelationField fld2 = fld.deepCopy();		
		assertTrue(fld2.getName().equals("field1"));
		assertTrue(fld2.getDescription().equals("dfield1"));
		assertTrue(OrchestraTypesTranslator.typeToString(fld2.getSqlTypeCode()).equals("INTEGER"));
		assertTrue(fld2.isNullable());
		
		// Check that converting to a bean then back to a field keeps all properties
		ScFieldBean fldBean = fld.toBean();
		assertTrue(fldBean.getName().equals("field1"));
		assertTrue(fldBean.getDescription().equals("dfield1"));
		assertTrue(OrchestraTypesTranslator.typeToString(fldBean.getType()).equals("INTEGER"));
		assertEquals(fldBean.getDbType(),"INT");
		assertTrue(fldBean.isNullable());
		RelationField fld3 = new RelationField (fldBean);
		assertTrue(fld3.getName().equals("field1"));
		assertTrue(fld3.getDescription().equals("dfield1"));
		assertEquals(fld3.getSQLTypeName(),"INT");
		assertTrue(OrchestraTypesTranslator.typeToString(fld3.getSqlTypeCode()).equals("INTEGER"));
		assertTrue(fld3.isNullable());		
		
	}
	
}
