package edu.upenn.cis.orchestra;
/**
 * This class contains functions that control the output of debug messages.
 * By default, such messages are printed but they can also be disabled.
 * 
 * @author Grigoris Karvounarakis
 */
public class Debug 
{
	protected static boolean s_midline = false;

	protected static String debugTag() {
		if (Config.getFullDebug() && !s_midline) {
			StackTraceElement[] stack = Thread.getAllStackTraces().get(Thread.currentThread());
			StackTraceElement caller = stack[4];
			String fullclass = caller.getClassName();
			int index = fullclass.lastIndexOf(".");
			String clazz = fullclass.substring(index+1);
			return clazz + "." + caller.getMethodName() + "@" + caller.getLineNumber() + ": ";
		} else {
			return "";
		}
	}
	
	/**
	 * Print a given message if in debug mode.
	 */
	public static void print(String msg) {
		if (Config.getDebug()) {
			//System.err.print(debugTag() + msg);
			System.out.print(debugTag() + msg);
			s_midline = !msg.endsWith("\n");
		}
	}
	/**
	 * Print a given message if in debug mode.
	 */
	public static void println(String msg) {
		if (Config.getDebug()) {
			//System.err.println(debugTag() + msg);
			System.out.println(debugTag() + msg);
			s_midline = false;
		}
	}
}
