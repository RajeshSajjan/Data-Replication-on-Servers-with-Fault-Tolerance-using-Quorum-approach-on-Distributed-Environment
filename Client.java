/* Author : Rajesh Sajjan & Nisha Halyal
 * Version : 1.0
 * Description : Client program send request to all the servers based on the configuration file i.e it sets up the TCP/IP connection
 * and stores all the sockets in hashmap.
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.net.Socket;

public class Client implements Runnable {
	public void run() {
		
		
		try {
			int port;
			String ipaddr;
			String node;
			int count =0;
			String filename = "Configuration";
			
			//Reads configuration file to fetch IP address and port number of al the servers.
			  FileReader inputFile = new FileReader(filename);
	          BufferedReader bufferReader = new BufferedReader(inputFile);
	          String line;
	        
	          //Creates socket connection with all server if the node is client.
	          if(Caller.serial.charAt(0) == 'c')
	          {
	          while ( count != 7)   {
	        	line = bufferReader.readLine();
	            String[] liner = line.split(" ");
	            node=liner[0];
	            ipaddr=liner[1];
	            port=Integer.parseInt(liner[2]);
	           
				Socket sock = new Socket(ipaddr, port);
				System.out.println("Node "+Caller.serial+ " is connected to node " + node);
				Caller.ips.put(node, sock);

	            count++;
	          }
	          }
	          else{
	        	  // If the entered node is server it connects will all other clients and servers and stores it.
	        	  
	        	  while ( (line = bufferReader.readLine()) != null)   {
	  	            String[] liner = line.split(" ");
	  	            node=liner[0];
	  	            ipaddr=liner[1];
	  	            port=Integer.parseInt(liner[2]);
	  	            
	  	            //Condition to make sure if does not store connection with itself.
	  	           if(!(Caller.serial.equalsIgnoreCase(node)))
	  	            {
	  				Socket sock = new Socket(ipaddr, port);
	  				System.out.println("Node "+Caller.serial+ " is connected to node " + node);
	  				Caller.ips.put(node, sock);
	  	           }
	  	          }
	          }
	          //Close the buffer reader
	          bufferReader.close();
	          inputFile.close();

		} catch (Exception exc) {
			exc.printStackTrace();
		}
	}
}
