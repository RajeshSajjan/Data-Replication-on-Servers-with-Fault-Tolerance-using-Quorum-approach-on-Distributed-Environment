/* Author : Rajesh Sajjan & Nisha Halyal
 * Version : 1.0
 * Description : Program defines maekawa quorum. Stores all the possible quorums in a tree into arraylist of arraylist.
 */

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class queue {

	public static ArrayList<ArrayList<String>> group = new ArrayList<ArrayList<String>>(15);

	public static void define_quorum()
	{
		 for (int i = 0; i < 15; i++)
				group.add(new ArrayList<String>());
		
		 for(int i=0; i <15; i++)
		 {
			 if(i ==0)
			 {
				 group.get(i).add("s1");
				 group.get(i).add("s2");
				 group.get(i).add("s4");
			 }
			 else if(i ==1)
			 {
				 group.get(1).add("s1");
				 group.get(1).add("s2");
				 group.get(1).add("s5");

			 }
			 else if(i==2)
			 {
				 group.get(2).add("s1");
				 group.get(2).add("s4");
				 group.get(2).add("s5");
			 }
			 else if(i==3)
			 {
				 group.get(i).add("s1");
				 group.get(i).add("s3");
				 group.get(i).add("s6");
			 }
			 else if(i==4)
			 {
				 group.get(i).add("s1");
				 group.get(i).add("s3");
				 group.get(i).add("s7");
			 }
			 else if(i==5)
			 {
				 group.get(i).add("s1");
				 group.get(i).add("s6");
				 group.get(i).add("s7");
			 }
			 else if(i==6)
			 {
				 group.get(i).add("s2");
				 group.get(i).add("s4");
				 group.get(i).add("s3");
				 group.get(i).add("s6");
			 }
			 else if(i==7)
			 {
				 group.get(i).add("s2");
				 group.get(i).add("s4");
				 group.get(i).add("s3");
				 group.get(i).add("s7");
			 }
			 else if(i==8)
			 {
				 group.get(i).add("s2");
				 group.get(i).add("s4");
				 group.get(i).add("s6");
				 group.get(i).add("s7");
			 }
			 else if(i==9)
			 {
				 group.get(i).add("s2");
				 group.get(i).add("s5");
				 group.get(i).add("s3");
				 group.get(i).add("s6");
			 }
			 else if(i==10)
			 {
				 group.get(i).add("s2");
				 group.get(i).add("s5");
				 group.get(i).add("s3");
				 group.get(i).add("s7");
			 }
			 else if(i==11)
			 {
				 group.get(i).add("s2");
				 group.get(i).add("s5");
				 group.get(i).add("s6");
				 group.get(i).add("s7");
			 }
			 else if(i==12)
			 {
				 group.get(i).add("s4");
				 group.get(i).add("s5");
				 group.get(i).add("s3");
				 group.get(i).add("s6");
			 }
			 else if(i==13)
			 {
				 group.get(i).add("s4");
				 group.get(i).add("s5");
				 group.get(i).add("s3");
				 group.get(i).add("s7");
			 }
			 else{
				 group.get(i).add("s4");
				 group.get(i).add("s5");
				 group.get(i).add("s6");
				 group.get(i).add("s7");
			 }
		 }
	}
}
