package edu.upenn.cis.orchestra.dbms;
import java.util.*;


import edu.upenn.cis.orchestra.datamodel.Atom;
import edu.upenn.cis.orchestra.datamodel.AtomConst;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlConstant;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlExpression;


public class UDFunctions{
	
	protected static Map<String, RelationMapping> _dependencies = new HashMap<String, RelationMapping>();
	private static Map<Relation, String> _depRelations = new HashMap<Relation, String>();
	
	public static class RelationMapping{
		private Map<Integer, RelationFieldContext> _mappings;
		
		protected RelationMapping(){
				_mappings = new HashMap<Integer, RelationFieldContext>();
		}
		protected void insertMapping(int pos, RelationFieldContext rfc){
			_mappings.put(pos, rfc);
		}
		public boolean equals(Object other){
			if(!(other instanceof RelationMapping))
				return false;
			
			RelationMapping otherMapping = (RelationMapping)other;
			return _mappings.equals(otherMapping._mappings);
		}
		protected RelationFieldContext getRelationFieldContext(Integer pos){
			return _mappings.get(pos);
		}
		
		protected List<Relation> getAllRelations(){
			List<Relation> relList = new ArrayList<Relation>();
			for(RelationFieldContext rf:_mappings.values()){
				if(!relList.contains(rf._rel))
					relList.add(rf._rel);
			}
			return relList;
		}
	}
	protected static class RelationFieldContext{
		private Relation _rel;
		private RelationField _relF;
		
		protected RelationFieldContext(Relation rel, RelationField rf){
			_rel = rel;
			_relF = rf;
		}
	}
	
	
	public static void registerFunction(Relation function){
		_dependencies.put(function.getName(), null);
	}
	
	public static void makeMappingFor(Relation function, RelationField functionField,
											Relation mappedRel, RelationField mappedRelField){
		
		RelationMapping rm = _dependencies.get(function.getName());
		RelationFieldContext rfc = new RelationFieldContext(mappedRel,mappedRelField);
		if(rm ==null){
			rm = new RelationMapping();
			_dependencies.put(function.getName(), rm);
		}
		int position = function.getFields().indexOf(functionField);
		
		if(position == -1)
			throw new RuntimeException("UDFunctions: Relation Field "+ functionField +" does not exist in function "+ function);
		rm.insertMapping(position, rfc);
		_depRelations.put(mappedRel, function.getName());
	}
	
	protected static boolean isUDF(String function)
	{
		if(function==null)
			return false;
		return _dependencies.containsKey(function);
	}
	
	protected static RelationField argumentFor(String fn, Integer pos)
	{
		RelationMapping rm = _dependencies.get(fn);
		RelationFieldContext rfc = rm.getRelationFieldContext(pos);
		//indexForAttr = rfc._rel.getFields().indexOf(rfc._relF);
		
		return rfc._relF;
	}
	protected static Relation argumentFor(String fn, Integer pos, Relation rel)
	{
		RelationMapping rm = _dependencies.get(fn);
		RelationFieldContext rfc = rm.getRelationFieldContext(pos);
		//rel = rfc._rel;
		//indexForAttr = rfc._rel.getFields().indexOf(rfc._relF);
		
		return rfc._rel;
	}
	protected static List<Relation> getDependenciesFor(String fn)
	{
		if(!_dependencies.containsKey(fn))
			throw new RuntimeException("Function "+fn + " was not registered as a UDF.");
		
		RelationMapping rm = _dependencies.get(fn);
		
		return rm.getAllRelations();
	}
	protected static String isDependentRelation(Relation r){
		if(r==null || _depRelations==null)
			return null;
		return _depRelations.get(r);
	}
}