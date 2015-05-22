/* Author : Rajesh Sajjan & Nisha Halyal
 * Version : 1.0
 * Description : Program broadcasts messages to all servers.
 */

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


public class broadcast {
	
	public synchronized  void broadsend(String string) throws IOException, InterruptedException {
	
		// Send request message all the servers along with client which requesting to broadcast.
		if(string.contains("REQUEST"))
		{
		  Iterator it = Caller.ips.entrySet().iterator();
		  Ricart.setTimer(System.nanoTime());
		  System.out.println(" TIMER ************* "+Ricart.getTimer());
		  
		  int obj = random_object();
		  
		  //Gets socket connection from hashmap and sends message.
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        Socket sock = (Socket) pair.getValue();
		        Ricart.req_count.getAndIncrement(); // to keep count of all requests sent. so that we can track of how many reply to receive
		        Caller.count++;
		        Caller.RQ_count++;
		        DataOutputStream dout=new DataOutputStream(sock.getOutputStream()); 
				String str2="REQUEST "+Ricart.getTimer()+" "+Caller.serial+" "+obj;
				//System.out.println("******** "+str2+"*********");
				dout.writeUTF(str2); 
		    }
		}
	}
	
	public static int random_object(){
		
		int min = 1 , max = 7 ;
		int time = 1 + (int)(Math.random() * ((max - min) + 1));
		
		return time;
	}

}