package edu.upenn.cis.orchestra.skolem;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Test driver for Skolems
 * 
 * @author zives
 *
 */
public class SkolemUDFs {
	private static Socket connection = null;
	public static final String CONNECTION = "localhost";
	public static final int PORT = 7770;
	private static ObjectOutputStream send = null;
	private static ObjectInputStream receive = null;
	
	public static void main(String[] parms) throws IOException {
		connection = new Socket(CONNECTION, PORT);

		try {
			System.out.println("Skolem value: ");
			
			
			int l = skolemStr("Abc", "123");
			
			System.out.println(l);
			
			l = skolemStr("Abc", "123");

			System.out.println(l);
		} catch (IOException se) {
			
		}
	}
	
	/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStr(String skolemName, String StrVal0) throws IOException {
		Integer skVal;
		
		if (send == null)
			send = new ObjectOutputStream(connection.getOutputStream());
		if (receive == null)
			receive = new ObjectInputStream(connection.getInputStream());

		Integer parms = new Integer(1);
		send.writeObject(parms);
		
		Integer arg1 = new Integer(0);
		// String
		send.writeObject(arg1);
		
		send.writeObject(skolemName);
		send.writeObject(StrVal0);
		send.flush();
		
		try {
			skVal = (Integer)receive.readObject();
		} catch (ClassNotFoundException ce) {
			return -1;
		}
		return skVal;
	}

}
