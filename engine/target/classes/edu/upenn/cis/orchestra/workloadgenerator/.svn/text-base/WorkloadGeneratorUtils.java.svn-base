package edu.upenn.cis.orchestra.workloadgenerator;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Utilties for the workloadgenerator package.
 * 
 * @author Sam Donnelly
 */
public class WorkloadGeneratorUtils {

	public static String spart(int j) {
		return "S" + String.valueOf(j) + "_";
	}

	public static String ppart(int i) {
		return "P" + String.valueOf(i) + "_";
	}

	public static String rpart(int k) {
		return "R" + String.valueOf(k);
	}

	public static String relname(int i, int j, int k) {
		// peer i, schema j, relation k
		return ppart(i) + spart(j) + rpart(k);
	}
	
	public static String stamp() {
		return new SimpleDateFormat("HH:mm:ss MM/dd/yy zzz").format(new Date());
	}

	/**
	 * Prevent inheritance and instantiation.
	 */
	private WorkloadGeneratorUtils() {

	}
}
