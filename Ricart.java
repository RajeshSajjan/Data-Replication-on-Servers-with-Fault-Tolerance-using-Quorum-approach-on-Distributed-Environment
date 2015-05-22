/* Author : Rajesh Sajjan & Nisha Halyal
 * Version : 1.0
 * Description : Program implements meakawa algorithm critical section for file replication. It also implements timer function which generates random
 * waiting time for all the clients and request function which broadcasts request to all the servers.
 * Program ensures consistancy of files and its replicas after each update.
 */

import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.atomic.AtomicInteger;


public class Ricart {
	public static volatile HashMap<Integer,Long> defer = new HashMap<Integer,Long>();
	private static volatile long timer;
	public static volatile boolean isRequest=false;
	public static volatile boolean hasEnteredCS;
	public static volatile boolean inCS=true;
	public static volatile long starttime,endtime,totaltime;
	public static AtomicInteger rep_count = new AtomicInteger();
	public static AtomicInteger req_count = new AtomicInteger();
	
	public static volatile boolean ALcount;
	public static volatile ArrayList<Integer> al = new ArrayList<Integer>();
	public static volatile Lock lock= new Lock();
	
	//Getters and setters to get cuurent time.
	public static long getTimer() {
		return timer;
	}

	public static void setTimer(long timer) {
		Ricart.timer = timer;
	}

	//Function to call timer and request functions.
	public void agarwal() throws IOException, InterruptedException  {
		//System.out.println(" Process started for CS");
		Caller.flag= false;
		Timer();
		Request();	
	}

	//Function to send request to all servers.
	public  void Request() throws IOException, InterruptedException {
		isRequest=true;
		req_count.set(0);
		hasEnteredCS=false;
		starttime=System.nanoTime();
		broadcast b1 = new broadcast();
		b1.broadsend("REQUEST");

	}

	//Random wait time generation function
	public void Timer() throws InterruptedException {
		int min = 5 , max = 10 ;
		int time = 5 + (int)(Math.random() * ((max - min) + 1));
		System.out.println("Entering random time "+time);
		for(int i =0 ;i < time ; i++)
		{
		Thread.sleep(100);
		}
	}

	//Function to send grant to the client based on the queue of request maintained.
	public  static  void Send_grant(String node, int obj) throws IOException {
		//Caller.RQ_count++;
		Socket i = Caller.ips.get(node);
		DataOutputStream dout=new DataOutputStream(i.getOutputStream());  
		dout.writeUTF("GRANT "+Caller.serial+" "+obj);  
		//System.out.println("sent GRANT from "+Caller.serial+" to "+node);
		//System.out.println();
	}
	
	
	//Function called when client wants to modify replicas of a object
	//Server send accepted message if file exists
	public  static  void send_writeaccept(String node, int obj, int folder) throws IOException{
		Socket sock = (Socket) Caller.ips.get(node);
		System.out.println("Sending write accept for "+node+" on "+obj);
		DataOutputStream dout=new DataOutputStream(sock.getOutputStream());
		dout.writeUTF("WRITE-ACCEPTED"+" "+node+" "+obj+" "+folder); 
		
	}
	
	//Function called when client wants to modify replicas of a object
	//Server send Abort message if file does not exist
	public  static  void send_writeabort(String node) throws IOException
	{
		Socket sock = (Socket) Caller.ips.get(node);
		DataOutputStream dout=new DataOutputStream(sock.getOutputStream());
		dout.writeUTF("ABORT "+Caller.serial+""); 
	}

	//Critical section function implements that only one client enters at a time and exits. It writes entry and exit time to a 
	//separate file. And sends release message to all the servers from whom grant was received.
	public static  void CriticalSection(String serial, int obj) throws InterruptedException, IOException {
		inCS=false;
		Caller.isFirst=false;
		endtime=System.nanoTime();
		totaltime=(endtime-starttime)/1000000;
		System.out.println("Latency in milliseconds : "+totaltime);
		System.out.println("******* Node "+Caller.serial+" has entered critical section******");
		//System.out.println("Resetting isFirst and isRequest to false and request count and reply count to zero");
	
		rep_count.set(0);
		req_count.set(0);
		Caller.size.set(0);
		lock.lock();
		
		FileWriter fileWriter = new FileWriter("CSEntryExitTime",true);
		synchronized(fileWriter){
			BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
			bufferWriter.write("Node "+Caller.serial+" Entering Critical Section at "+System.currentTimeMillis()+"\n");

			//Critical section entry point.
			//Send write-request to 3 replicas of object obj
			int[] arr = new int[3];
			arr[0] = obj;
		
			int sec = (obj+1)%7;
			if(sec == 0)
				sec = 7;
			arr[1] = sec;
		
			int thr = (obj+2)%7;
			if(thr == 0)
				thr = 7;
		
			arr[2] = thr;
		
			String s = "s";
			for(int i=0 ; i<arr.length ; i++){
				String ser = s+arr[i];
				Socket sock = (Socket) Caller.ips.get(ser);
				DataOutputStream dout=new DataOutputStream(sock.getOutputStream());  
				dout.writeUTF("WRITE-REQ"+" "+Caller.serial+" "+obj+" "+arr[i]);
				//Thread.sleep(200);
			}
		
			Thread.sleep(3000);
			
			
			/*if(!Caller.abort)
				System.out.println("aborted here");
			*/
			//System.out.println("Caller write count "+Caller.acc_count.get());
			
			
			//If did not receive abort from any server and received proper acknowledgment then proceed with writing on replicas
			if(Caller.abort)
			{
				for(int i=0 ; i<3 ; i++){
					String filename = "/home/004/r/rx/rxs130830/Project3/"+arr[i]+"/file"+obj+"";
			
					FileWriter fileWriter1 = new FileWriter(filename,true);
					synchronized(fileWriter1){
						BufferedWriter bufferWriter1 = new BufferedWriter(fileWriter1);
						bufferWriter1.write("Node "+Caller.serial+" was here"+"\n");
						bufferWriter1.close();
						fileWriter1.close();
					}
		    
				}
				//Critical section exit point.
			}
			
			//If received abort from any server then do not write and abort	
			 else {
				 Caller.abort=true;
				System.out.println("ABORT");
			}
			bufferWriter.write("Node "+Caller.serial+" Exiting Critical Section at "+System.currentTimeMillis()+"\n");
			bufferWriter.close();
			fileWriter.close();	 
		}
			lock.unlock();
			System.out.println();
		
			System.out.println("************ Exiting Critical SEction *************");
			System.out.println("************* Sending RELEASE messages**************");
		
			isRequest=false;
			timer=0;
		
			//Sending of release messages based on the grant list maintained.
			Queue<String> release = new LinkedList<String>(Caller.grant);
			Caller.grant.clear();
			Iterator itr=release.iterator();  
		
			while(itr.hasNext())
			{  
				String node =(String) itr.next();
				Socket sock = (Socket) Caller.ips.get(node);
				//Caller.RQ_count++;
				DataOutputStream dout=new DataOutputStream(sock.getOutputStream());
				dout.writeUTF("RELEASE "+Caller.serial+" "+obj);  
				//System.out.println("sent release from "+Caller.serial+" to "+node);
				itr.remove();
			}  
		
		    if(Caller.grant.size() != 0)
		    {
		    	Queue<String> release1 = new LinkedList<String>(Caller.grant);
				Caller.grant.clear();
				Iterator itr1=release1.iterator();
				
				while(itr1.hasNext())
				{  
				String node =(String) itr1.next();
				Socket sock = (Socket) Caller.ips.get(node);
		        //Caller.RQ_count++;
		        DataOutputStream dout=new DataOutputStream(sock.getOutputStream());
				dout.writeUTF("RELEASE "+Caller.serial+" "+obj);  
				//System.out.println("sent release from "+Caller.serial+" to "+node);
				itr1.remove();
				}  
		    }
		    
		    HashMap<String,Socket> ipstemp = new HashMap<String,Socket>(Caller.ips);
		    Iterator itr2=ipstemp.entrySet().iterator();  
		    while (itr2.hasNext()) 
		    {
		        Map.Entry pair = (Map.Entry)itr2.next();
		        String tim = (String) pair.getKey();
		        Socket sock = (Socket) Caller.ips.get(tim);
		        Caller.RQ_count++;
		        DataOutputStream dout=new DataOutputStream(sock.getOutputStream());
				dout.writeUTF("RELEAS-WAIT "+Caller.serial+" "+obj);  
				itr2.remove();
		    }
		
		System.out.println("***** Round Completed****\n");
		ALcount=false;
		inCS=true;
		Caller.flag=true;
		Caller.entry = true;
		Caller.acc_count.set(0);

	}
  }

