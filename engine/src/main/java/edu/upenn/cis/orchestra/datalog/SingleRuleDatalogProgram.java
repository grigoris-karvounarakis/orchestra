package edu.upenn.cis.orchestra.datalog;

//import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
//import edu.upenn.cis.orchestra.engine.IDb;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.exchange.RuleQuery;

/**
 * 
 * @author gkarvoun
 *
 */
public class SingleRuleDatalogProgram extends NonRecursiveDatalogProgram {
	//protected List<PreparedStatement> _stmts;
	protected RuleQuery _stmts;
	protected String _queryString = new String("");
	protected boolean preparedFlag = false;
	
	public SingleRuleDatalogProgram(List<Rule> r, boolean c4f){
		super(r, c4f);
		_stmts=null;
	}
	
	public SingleRuleDatalogProgram(List<Rule> r){
		super(r);
		_stmts=null;
	}
	
	public SingleRuleDatalogProgram(Rule r){
		super(null);
		List<Rule> l = new ArrayList<Rule>();
		l.add(r);
		_rules = l;
		_stmts=null;
	}
	
	public SingleRuleDatalogProgram(Rule r, boolean c4f){
		super(null);
		List<Rule> l = new ArrayList<Rule>();
		l.add(r);
		_rules = l;
		_stmts=null;
		_c4f = c4f;
	}
	
	/*
	public boolean isPrepared(){
//		return(_stmts != null);
		return preparedFlag;
	}
	
	public void initialize(RuleQuery q, Map<ScField, String> typesMap) {
		if (_stmts != null)
			return;
		
		StringBuffer qString = new StringBuffer();
		
		List<String> qs = getQuery(typesMap);
		
		for (String query : qs){
			q.add(query);
			qString.append("\n");
			qString.append(query);
		}

		_queryString = new String(qString);
		_stmts = q;
	}
	
	public int evaluate() {
		return _stmts.evaluateSelf();
	}
	
	//public List<PreparedStatement> statements(){
	public RuleQuery statements() {
		return _stmts;
	}
	
	public List<String> getQuery(Map<ScField, String> typesMap) {
		Rule rule = (Rule)getRules().get(0);
		List<String> queries = rule.toUpdate(typesMap);
		
		return queries;//new String(qString);
	}
	
	public void prepare() {
		preparedFlag = true;
		try {
			_stmts.prepare();
		} catch (Exception e) {
			throw new RuntimeException("Unable to prepare:\n" + e.getStackTrace());
		}
		
		//public List<PreparedStatement> prepare(
			//SqlDb con, Map<ScField, String> typesMap){
	}*/
	
	@Override
	public String toString ()
	{
		if(_queryString != null){
			return _queryString;
		}else{
			StringBuffer buffer = new StringBuffer ();
			Rule r = (Rule)getRules().get(0);
//			Debug.println("EVALUATE: " + r.toString());

			buffer.append("\n" + r.toString() + "\n");

			return buffer.toString();
		}
	}
	
	public String toQueryString ()
	{
		if(_queryString != null){
			return _queryString;
		}else{
			StringBuffer buffer = new StringBuffer ();
			Rule r = (Rule)getRules().get(0);
//			Debug.println("EVALUATE: " + r.toString());
			for(String query : r.toUpdate(0)){
				buffer.append("\n" + query);
			}
			buffer.append("\n");
			
			return buffer.toString();
		}
	}

}
