/* Author : Rajesh Sajjan & Nisha Halyal
 * Version : 1.0
 * Description : Server program waits to accept the client connection and creates a listen thread to receive messages on the socket.
 */

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.*;
import java.util.*;


public class Server implements Runnable {

	
	public static ArrayList<Socket> connection = new ArrayList<Socket>();
	public static ArrayList<String> nodes = new ArrayList<String>();
	
	public void run()  {

		try{
			 int port = 0;
			  String filename= "Configuration";
			  
			  //Reads configuration file.
			  FileReader inputFile = new FileReader(filename);
	          BufferedReader bufferReader = new BufferedReader(inputFile);
	          String line;

	          // Seraches for the port number of itself.
	          while ((line = bufferReader.readLine()) != null) {
	        	  
	        	  String[] splitmsg = line.split(" ");
	        	  String name = splitmsg[0];
	
	        	  if(name.equals(Caller.serial))
	        	  {
	        		  String[] parts = line.split(" ");
	        		  port = Integer.parseInt(parts[2]);
	        		  //System.out.println("port " +port);
	        		  break;
	        	  }
	          }
	          
	          //closes the file.
	          bufferReader.close();
	          inputFile.close();
	          
	          if(port == 0)
	          {
	        	  System.out.println("error occured while initializing port number");
	        	  System.exit(1);
	          }
	          
			ServerSocket server = new ServerSocket(port);
			
			//Accepts the socket connection and spawns listener thread to receive messages.
			while(true)
			{
				Socket sock = server.accept();				
				//create a thread which handles ricart agarwal 
				Listen listener = new Listen(sock);
				Thread l1 = new Thread(listener);
				l1.start();
				
			}
		}
		catch(IOException E) {
			System.out.println("Caought IO Exception in Main Server.java"+E);
		}
	}
}
