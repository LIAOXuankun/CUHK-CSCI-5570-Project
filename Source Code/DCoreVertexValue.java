package Dcore;
//methods need to implement:getInCoreness(return a int) /getCoreness(return a ArrayList)
//import org.apache.log4j.Logger;

import Dcore.DCoreMessage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.json.JSONArray;
import org.apache.hadoop.io.Writable;
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;

/**
* Vertex value used for the Core implementation 
*/

public class DCoreVertexValue implements Writable {

  //private final static Logger logger = Logger.getLogger(KCore.class.getName());

  private static final long[][] InNeiCorness = null;
/** 
  * Core is an integer that represents the exact estimate of the 
  * corness of u, initialized with the local degree.
  */
	private boolean Out;
  private double InCoreness;
 // private ArrayList<Integer> Coreness;
 //private int[]  Coreness;
  
  private int[][]  InNeiCoreness;
  private int[][]  OutNeiCoreness;
  
  /**
  * Default constructor
  */
  public DCoreVertexValue() {
	  this.Out = true;
	  
    this.InCoreness = 0;
    //this.Coreness= new ArrayList<Integer>();
   this.InNeiCoreness = new int [2][2];
   this.OutNeiCoreness = new int [2][2];
  }
  
 
 
  
  
  
  
  
  public DCoreVertexValue(boolean Out, double InCoreness,int [][] InNeiCoreness, int [][] OutNeiCoreness) { //used in outcoreness computation, the InNeiCoreness will only be used in the verification part
	    this.Out = Out;   // used in the first supersteps of out coreness computation
	    this.InCoreness = InCoreness;
	    this.InNeiCoreness = InNeiCoreness;
	    this.OutNeiCoreness = OutNeiCoreness;
	  }

  // Serialization functions -----------------------------------------------

  @Override
  public void readFields(DataInput input) throws IOException {
	this.Out = input.readBoolean();
    this.InCoreness = input.readDouble();
    String s = input.readUTF();
    if(!s.equals("")){
    	ArrayList<ArrayList<Integer>> Inall  = new ArrayList<ArrayList<Integer>>();
    	ArrayList<ArrayList<Integer>> Outall  = new ArrayList<ArrayList<Integer>>();
    	String[] split_string = s.split("]");
    	for(int i=0;i<split_string.length;i++){
    	}
     	int t = 1;
    	for (String r: split_string){
    	  if(t==1){    // the InNeiCoreness
    		  ArrayList<Integer> tmp = new ArrayList<Integer>();
    		String[] sub_str = r.split(" ");
    		for (String str: sub_str) {
    			if(!str.equals("-")&&!str.equals("")) tmp.add(Integer.valueOf(str));
            	else  {
            		Inall.add(tmp);
            		
                	for (int j=0;j <Inall.size();j++){	
                	}         		
            		tmp.clear();
            	}   			  			
    		}  
    		t++;    		
    	  }
    	  
    	  
    	  
    	  
    	  
    	  else{// the OutNeiCoreness
    		  ArrayList<Integer> tmp = new ArrayList<Integer>();
    		  
    		  String[] sub_str = r.split(" ");
      		  for (String str: sub_str) {
      			if(!str.equals("-")&&!str.equals("")) tmp.add(Integer.valueOf(str));
              	else  {
              		Outall.add(tmp);
              		tmp.clear();
              	}
      		}
    	  
    	  }
    	
    	} 	
    	
    	
    	
    	
    	
    	this.InNeiCoreness = new int[Inall.size()][];
    	for(int i = 0;i<Inall.size();i++){
    		
    		InNeiCoreness[i] = new int [Inall.get(i).size()];
    		for(int j = 0;j<Inall.get(i).size();j++){
    			InNeiCoreness[i][j] = Inall.get(i).get(j);
    		}
    	
    	}
    	
    	this.OutNeiCoreness = new int[Outall.size()][];
    	for(int i = 0;i<Outall.size();i++){
    		OutNeiCoreness[i] = new int [Outall.get(i).size()];
    		for(int j = 0;j<Outall.get(i).size();j++){
    			OutNeiCoreness[i][j] = Outall.get(i).get(j);
    		}
    		
    	}
    	
    	
    	
    	
    	
    }   
  }

  @Override
  public void write(DataOutput output) throws IOException {
	output.writeBoolean(Out);
    output.writeDouble(this.InCoreness);
    String s = "";
    
    for (int[] arr:InNeiCoreness) {
    	
    	for (int n : arr){
    		s += String.valueOf(n);
    		s+=" ";    		
    	}
    	s+="-";//use ] to seperate different rows of 2d Array
    	s+=" ";
    	
    }
    s+="]";  // to seperate two 2d array
   
    
    for (int[] arr:OutNeiCoreness) {
    	
    	for (int n : arr){
    		s += String.valueOf(n);
    		s+=" ";    		
    	}
    	s+="-";//use ] to seperate different rows of 2d Array
    	s+=" ";
    	
    }
    
   
    
 
    
   
    output.writeUTF(s);
  }

  // Accessors -------------------------------------------------------------

  /**
  * Gets the node's core value.
  * @return coreness
  */
  public int getInCoreness() {
    return (int)InCoreness;
  }
  
  
  /**
   * Gets the node's coreness estimate, which is a integer arraylist .
   * @return Coreness
   */
   public int [][] getInNeiCoreness() {
     return this.InNeiCoreness;
   }
   
   public int [][] getOutNeiCoreness() {
	     return this.OutNeiCoreness;
	   } 
   
public boolean getOut(){
	return this.Out;
}
 

}
