package edu.upenn.cis.orchestra.p2pqp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;

public class BandwidthParser {
	public static void main(String args[]) throws Exception{
		int buckets[][] = new int[16][360000];
		int finishTime[]= new int[16];
		
		for (int i=0; i<16; i++) {
			for (int j=0; j< 360000; j++) {
				buckets[i][j] = 0; 
			}	
			finishTime[i] = 0;
		}
		
		String filename = "tcp23.out";
		File inputFile = new File(filename);
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		boolean first = true;
		String str = br.readLine();
		int mintime = 0;
		int time = 0;
	
		while (str != null) {
			if (str.length() == 0)  {
				str = br.readLine();
				continue;
			}
			if (str.contains("dbclust") || str.contains("DBCLUST")) {
			  if (str.indexOf('>') < 0) {
				  str = br.readLine();
				  continue;
			  }
			  String str1 = str.substring(str.indexOf('>'));
			  if (str1.contains("dbclust") || str1.contains("DBCLUST")) {
				  if (str.contains("length")) {
					  String str2 = str.substring(str.indexOf("length"));
					  str2 = str2.substring(str2.indexOf(' ')+1, str2.indexOf(')'));
					  int packetsLength = Integer.parseInt(str2);
					  if (str.charAt(2)!=':') {
						  throw new RuntimeException("Wrong parser!");
					  }
					  int minute = Integer.parseInt(str.substring(3,5));
					  if (str.charAt(5)!=':') {
						  throw new RuntimeException("Wrong parser!");
					  }
				      int second = Integer.parseInt(str.substring(6,8));
				      if (str.charAt(8)!='.') {
						  throw new RuntimeException("Wrong parser!");
					  }
				      int msecond = Integer.parseInt(str.substring(9,11));
				      
				      time = minute * 6000 + second * 100 + msecond;
				      
				      if (!first) {
				    	  time = time - mintime;
				      } else {
				    	  mintime = time;
				    	  time = 0; 
				    	  first = false; 
				      }
				      if (time >= Integer.MAX_VALUE) {
				    	  //System.out.println(time + " " + Integer.MAX_VALUE);
				    	  throw new RuntimeException("Too big time!");
				      }
				      if (time < 0) {
				    	  System.out.println(str);
				    	  System.out.println(minute+" "+second+" "+msecond+" " + time + " " + mintime);
				    	  throw new RuntimeException("Time is smaller than zero!");
				      } 
				      try {
				      buckets[0][time] += packetsLength;
				      } catch (Exception e){
				    	  System.out.println(time);
				      }
				  }
			  }
			}
			str = br.readLine();	
		}
		finishTime[0] = time; 
		
              int firstNode = Integer.parseInt(args[0]);
		int nodeNum = Integer.parseInt(args[1]);
		
		for (int node = firstNode; node <= firstNode+nodeNum-2; node++) {
			filename = "tcp"+node+".out";
			inputFile = new File(filename);
			br = new BufferedReader(new FileReader(inputFile));
			first = true;
			str = br.readLine();
			time = 0;
		
			while (str != null) {
				if (str.length() == 0)  {
					str = br.readLine();
					continue;
				}
				if (str.contains("dbclust") || str.contains("DBCLUST")) {
				  if (str.indexOf('>') < 0) {
					  str = br.readLine();
					  continue;
				  }
				  String str1 = str.substring(str.indexOf('>'));
				  if (str1.contains("dbclust") || str1.contains("DBCLUST")) {
					  if (str.contains("length")) {
						  String str2 = str.substring(str.indexOf("length"));
                                           
						  str2 = str2.substring(str2.indexOf(' ')+1, str2.indexOf(')'));

   						  int packetsLength = 0; 

                                           try {
						   packetsLength = Integer.parseInt(str2);
						  } catch (Exception e) {
                                             System.out.println(node);
                                             System.out.println(str);
						  }
 						  if (str.charAt(2)!=':') {
							  throw new RuntimeException("Wrong parser!");
						  }
						  int minute = Integer.parseInt(str.substring(3,5));
						  if (str.charAt(5)!=':') {
							  throw new RuntimeException("Wrong parser!");
						  }
					      int second = Integer.parseInt(str.substring(6,8));
					      if (str.charAt(8)!='.') {
							  throw new RuntimeException("Wrong parser!");
						  }
					      int msecond = Integer.parseInt(str.substring(9,11));
					      
					      time = minute * 6000 + second * 100 + msecond;
					      
                                        if (time < mintime) {
                                           str = br.readLine();
					    	  continue;
                                        }
					      time = time - mintime; 

					      if (time >= Integer.MAX_VALUE) {
					    	  //System.out.println(time + " " + Integer.MAX_VALUE);
					    	  throw new RuntimeException("Too big time!");
					      }
					      try {
					      buckets[node-firstNode+1][time] += packetsLength;
					      } catch (Exception e){
					    	  System.out.println(time);
					      }
					  }
				  }
				}
				str = br.readLine();	
			}
			finishTime[node-firstNode+1] = time;
		}
		
		PrintStream resultFile = new PrintStream("bandwidth.dat");
		PrintStream detailResultFile = new PrintStream("bandwidth_detail_bootstrap.dat");
		resultFile.flush();
		long sum = 0; 
		long max = 0;
		int num = 0; 
		for (int i = 0; i < finishTime[0]; i++) {
			detailResultFile.println(i + "," + buckets[0][i]);
			sum += buckets[0][i];
			num++;
			if (buckets[0][i] > max) {
				max = buckets[0][i];
			}
		}
		
		resultFile.println("AverageBandwidth on bootstrap node(KB/s):"+(int)((double)sum*100/num/1024));
		resultFile.println("PeakBandwidth on bootstrap node(KB/s):"+ (int)((double)max*100/1024));
		resultFile.println("TotalMessages on bootstrap node(KB):"+ (int)((double)sum/1024));
		detailResultFile.close();
		
		
		detailResultFile = new PrintStream("bandwidth_detail_system.dat");
		int [] sumPerTime = new int[360000];
		
		sum = 0;
		max = 0; 
		for (int i=0; i<finishTime[0]; i++) {
			sumPerTime[i] = 0;
			for (int j=0; j<nodeNum; j++) {
				sumPerTime[i] += buckets[j][i];
			}
			detailResultFile.println(i + "," + sumPerTime[i]);
			if (sumPerTime[i] > max) {
				max = sumPerTime[i];
			}
			sum+=sumPerTime[i];
		}
		
		resultFile.println("AverageBandwidth per peer(KB/s):"+(int)((double)sum*100/nodeNum/finishTime[0]/1024));
		resultFile.println("PeakBandwidth per peer(KB/s):"+ (int)((double)max*100/nodeNum/1024));
		resultFile.println("TotalMessages per peer(KB):"+ (int)((double)sum/nodeNum/1024));
		
		detailResultFile.close();
		resultFile.close();
		
	}
}