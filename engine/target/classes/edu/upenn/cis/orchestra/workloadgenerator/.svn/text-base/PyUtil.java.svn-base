package edu.upenn.cis.orchestra.workloadgenerator;

import java.util.HashMap;
import java.util.Map;

import org.python.core.PyDictionary;
import org.python.core.PyList;
import org.python.core.PyTuple;

public class PyUtil {

	/**
	 * Extract a Python dictionary into a Java Map.
	 * 
	 * @param pyDict
	 *            the dictionary to convert.
	 * 
	 * @return a <code>Map</code> with the contents of <code>pyDict</code>.
	 */

	public static Map<Object, Object> toMap(PyDictionary pyDict) {

		PyList pa = pyDict.items();
		Map<Object, Object> map = new HashMap<Object, Object>();
		while (pa.__len__() != 0) {
			PyTuple po = (PyTuple) pa.pop();
			Object first = po.__finditem__(0).__tojava__(Object.class);
			Object second = po.__finditem__(1).__tojava__(Object.class);
			map.put(first, second);
		}
		return map;
	}
}
