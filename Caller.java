/* Author : Rajesh Sajjan & Nisha Halyal
 * Version : 1.0
 * Description : Program execution begins from here. This is the main program which takes node name and calls server and client thread
 * and also handles calling maekawa algorithm 20 times if the node is client.
 */

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Caller {

	public static volatile boolean[] locker = new boolean[8];
	public static boolean abort=true;
	public static volatile AtomicInteger acc_count = new AtomicInteger();
	public static HashMap<Integer, LinkedList<String>> waitlist = new HashMap<Integer,LinkedList<String>>();
	public static HashSet<String> grant=new HashSet<String>();  
	static String serial;
	public static boolean entry = true;
	
	static int setup;
	static volatile int count;
	static AtomicInteger size = new AtomicInteger();
	static int RQ_count;
	static int RE_count;
	public static boolean flag;
	public static boolean isFirst;
	public static int nodes;
	public static HashMap<String,Socket> ips = new HashMap<String,Socket>();
	
	public static void main(String[] args) throws InterruptedException, IOException {
		
		//Reading node number from user
		System.out.println("Enter name of the node");
		Scanner in = new Scanner(System.in);
        String s = in.nextLine();
        
        serial = s;	
		
		//Creating server class thread.
		Server s1 = new Server();
		Thread t1 = new Thread(s1);
		t1.start();

		Thread.sleep(25000);
		
		//Calling client method
		Client c1 = new Client();
		Thread t2 = new Thread(c1);
		t2.start();
		
		//Creates object of Maekawa algorithm
		Thread.sleep(20000);  
		Ricart r1 = new Ricart();		
		
		for(int i=1; i <8;i++)
		waitlist.put(i, new LinkedList<String>());		

		//Waits till client connects to all seven servers
		if(serial.contains("c"))
		{
		while(true)
		{
			if(ips.size() != 7)
			{
				Thread.sleep(300);
			}
			else
				break;
		}

		//Creates object for quorum definition
		queue.define_quorum();

		
		Thread.sleep(25000);
		Thread.sleep(3000);

		isFirst=true;
		System.out.println("***********************Maekawa***************************************");
		//System.out.println("FIRST TIME = TRUE");
			
		
		//Runs maekawa for twenty times
			for(int i=1; i <= 20 ; i++)
			{
				System.out.println("\n************ Turn "+i+" *************");
				r1.agarwal();
				while (flag == false)
				{
					Thread.sleep(50);
				}
				Caller.acc_count.set(0);
			}
			
		//Sends completion notification after completion of maekawa algorithm runs for twenty times.
			String node = "s1";
			Socket i = ips.get(node);
			DataOutputStream dout=new DataOutputStream(i.getOutputStream());  
			dout.writeUTF("Completion notification from node "+Caller.serial+" ."); 
			
			System.out.println("Total msg sent "+RQ_count);
			System.out.println("Total msg received "+RE_count);
	}

		
	}
}
