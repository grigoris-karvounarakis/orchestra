package edu.upenn.cis.orchestra.workload;

import java.io.FileOutputStream;
import java.io.PrintWriter;

public class PrintZipf {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Double s = Double.parseDouble(args[0]);
		int n = Integer.parseInt(args[1]);
		PrintWriter pw = new PrintWriter(new FileOutputStream(args[2]));
		
		RandomizedSet rs = new RandomizedSet(false);
		
		for (int i = 0; i < n; ++i) {
			rs.addElement(Integer.toString(i));
		}
		
		rs.makeWeightedZipfian(s);
		
		pw.print(rs.toString());
		pw.close();
	}

}
