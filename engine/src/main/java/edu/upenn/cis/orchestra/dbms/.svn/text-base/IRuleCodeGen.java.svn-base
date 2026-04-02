package edu.upenn.cis.orchestra.dbms;

import java.util.List;

/**
 * Basic interface to DB-specific code generation
 * @author zives
 *
 */
public interface IRuleCodeGen {
	public enum UPDATE_TYPE {CLEAR_AND_COPY, DELETE_FROM_HEAD, OTHER};
    
	//public String toQuery(Map<ScField, String> typesMap);
	
    public List<String> getCode(UPDATE_TYPE u, int curIterCnt);

    public List<String> getCode(List<String> existing, UPDATE_TYPE u, int curIterCnt);
}
