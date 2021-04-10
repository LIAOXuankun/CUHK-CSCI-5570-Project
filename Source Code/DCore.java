package  Dcore;

/* Giraph dependencies */
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;

/* Hadoop dependencies */
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

import Test.DCoreMasterCompute;
import Test.DCoreMessage;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.log4j.Level;

/* Aggregator information */
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.aggregators.DoubleSumAggregator;

import java.io.IOException;

/**
 * Calculates the Dcores for each vertices in a dircted graph. The whole procedure can be divided into two steps.
 * Step1: Calculation of Incoreness.    Each vertices send their incoreness estimate (an integer) via out edges, each vertices take those estimates as input to calculate new incoreness estimate of themselves.
 * 
 * 
 * Step2: Calculation of Outcoreness.   With the Incoreness obtained from Step1, we can initialize an array with length being Incoreness+1 for each vertices, we calculate the items(outcoreness estimate) in that array in Step2.
 *                                       We send messages via incoming edges in this Step. We take the according value of outneighbors as input to calculate the outcoreness estimate. e.g, We want to calculate Vertex V1([2,2,2]),
 *                                       whose outneighbors are Vo1[2,3,4] and Vo2[2,3]. then the first item in V1's array should be H-index of[2,2], which is 2. The second item in V1's array should be H-index of [3,3] = 2, and 
 *                                       third item should be H-index of [4], which is 1. Then V1's array is updated as   [2,2,1]. We then send changed value and its index via incoming edges, e,g,[2,1](index is 2 and value is 1)  
 *                                       
 */
public class DCore extends BasicComputation<
    LongWritable, DCoreVertexValue, LongWritable, DCoreMessage> {
  @Override
  public void compute(
      Vertex<LongWritable, DCoreVertexValue, LongWritable> vertex, 
      Iterable<DCoreMessage> messages) 
    throws IOException {
    //The first superstep are computing the indegree of every vertex
    if (getSuperstep() == 0) {
    
      /** 
      * Tell out-neighbors that this vertex is their in-neighbor, 
      * for the computation of indegree, this can be done when we process the raw graph data.
      */
    	Iterable<Edge<LongWritable, LongWritable>> edges = vertex.getEdges();
    	
    	byte which=0; 
		for (Edge<LongWritable, LongWritable> edge : edges) {
			/* Tell out-neighbors that this vertex is their in-neighbor, destinations here are all outneighbors, a byte is to reduce communication overhead */
			sendMessage(edge.getTargetVertexId(),new DCoreMessage(which));
			} 
		aggregate(DCoreMasterCompute.InCorenessAggregator,new IntWritable(1));  //for the judgment of which computation step we are in 
    } 
    
    
    else if (getSuperstep() == 1) {
    	
    	
    	int sum = 0;
		for (DCoreMessage message : messages) {
			/* Count number of messages, this implies how many in-neighbors the vertex has */
			sum++;
		}
		
		
		DCoreVertexValue value = new DCoreVertexValue(false,sum,new int [2][2],new int [2][2]); //the int variable(sum) is incoreness estimate, these two 2D arrays are D-core estimate of InNeighbor 
        vertex.setValue(value);                                                                 //and outNeighbor. the boolean variable is used for the judgment of beginning of each computation
                                                                                                //step, when a initialization of these two 2D arrays are done. 
        
        //send messages via outedges
       
        byte which = 1;
        Iterable<Edge<LongWritable, LongWritable>> edges = vertex.getEdges();
		for (Edge<LongWritable, LongWritable> edge : edges) {
			sendMessage(edge.getTargetVertexId(),new DCoreMessage(which, sum, (int)vertex.getId().get()));// which is used to differentiate different situations of computation, 
			}                                                                                              //by sending information according to the situations we can reduce communication overhead 
		aggregate(DCoreMasterCompute.InCorenessAggregator,new IntWritable(1));
		
    }
   
    
    //below is computation of InCoreness/OutCoreness 
    else {
    	 
     //when the inCorenessAggregator = 0, incoreness computation is done
    	IntWritable NumOfVerticesIncoreness = getAggregatedValue(DCoreMasterCompute.InCorenessAggregator); 
    	
    	
    	
    	//below is computation of Incoreness estimate (an integer)
    	if(NumOfVerticesIncoreness.get()!= 0){
    		 boolean flag= false;    //whether there is a  InNeighbor with a smaller InCoreness than this  vertex, true mean there is a smaller InCoreness
    		 ArrayList<Integer> InNeighborDegree = new ArrayList<Integer>();
    		 int estimate = vertex.getValue().getInCoreness();    //original InCoreness estimate of this vertex
    		 
    		//the current coreness estimate of the vertex
    		 
    		 for (DCoreMessage message: messages) {
    			 if(message.getInCoreness1()<vertex.getValue().getInCoreness()) flag=true;     			 
    			 InNeighborDegree.add(message.getInCoreness1());
    		 }
    		 
    		 if(flag) {
    			 int[] arr=new int[InNeighborDegree.size()];
    			 for(int i=0;i<InNeighborDegree.size();i++) {
    				 arr[i]=InNeighborDegree.get(i);
    			 }
    			 Arrays.sort(arr);//sort the neighbor indegree to do the h-operation
    			 estimate=H_operation.h_operation(arr);  //a binary search is applied here and the function return H-index of input array
    			 
    			  
    			 if(estimate<vertex.getValue().getInCoreness()){     //only when new estimate is smaller than the original one, the InCoreness estimate will be updated		 
    				 DCoreVertexValue value = new DCoreVertexValue(false,estimate,new int [2][2],new int [2][2]);
    			     vertex.setValue(value);
    				 aggregate(DCoreMasterCompute.InCorenessAggregator,new IntWritable(1));
    			 }
    		 }    		
    		 
    		 //send messages through outedges
    		 byte which = 1;
    		 Iterable<Edge<LongWritable, LongWritable>> edges = vertex.getEdges();
    		 for (Edge<LongWritable, LongWritable> edge : edges) {
    		 sendMessage(edge.getTargetVertexId(), new DCoreMessage(which,estimate,(int)vertex.getId().get())); 		
    	} 		
    }
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	//below is the computation of outcoreness
   else if(NumOfVerticesIncoreness.get()== 0) {
    		ArrayList<Integer> source = new ArrayList<Integer>();    //record the source of the message
            boolean MsgFromOut = false;    //Is there any message from OutNeighbor? True means yes 
    		for (DCoreMessage message: messages) {
    			if(message.getState()==2||message.getState()==1){  //1 means the message is sent in the incoreness computation step, 1 and 2 both means the message is from InNeighbors
    			source.add(message.getSource());
    			}
    			if(message.getState()==3) MsgFromOut=true;//is there any message from outneighbor?
    		}
    		ArrayList<Integer> UpdatedCorenessEstimate = new  ArrayList<Integer>();//to record the changed OutCoreness estimate value of this vertex, will be sent to the out neighbors
	
    		
    		//if vertex.getValue().getOutCoreness() == int[2][2], which is to say this is the first Superstep of OutCoreness computation. Initialize the Outcoreness as out degree for each item in the arrry for each vertices
    		if (vertex.getValue().getOutNeiCoreness().length==2&&vertex.getValue().getOutNeiCoreness()[0][0]==0&&vertex.getValue().getOutNeiCoreness()[1][0]==0){   //the first superstep of outcoreness computation
    		   int [][]initial=new int[1][vertex.getValue().getInCoreness()+1];
               for(int i=0;i<=vertex.getValue().getInCoreness();i++) {
    				initial[0][i]=vertex.getNumEdges();
    				UpdatedCorenessEstimate.add(vertex.getNumEdges());
    			}
              
    			DCoreVertexValue value = new DCoreVertexValue(false,vertex.getValue().getInCoreness(),new int [2][2],initial); //false means the first superstep of outcoreness computation
    	        vertex.setValue(value); 
    	        aggregate(DCoreMasterCompute.OutCorenessAggregator,new IntWritable(1));
    		}
    		
    		
    		else if(!vertex.getValue().getOut()){//sencond superstep of outcoreness computation, this function returns the boolean value of of the vertex value
                                                 //in this superstep, the D-core estimate of all the OutNeighbor will be initialized for each vertices.Each row of the 2D array:
    			                                 //[VertexId, OutCoreness estimate of InCoreness 0, OutCoreness estimate of InCoreness 1, ...], the length this row should be the Incoreness estimate of according vertex + 2
    			                                 //if we have a vertex in(0,3)-core (1,2)-core and (2,2)-core with Id of 5, then its according row in the 2d array is:[5,3,2,2] 
    			    UpdatedCorenessEstimate.add(vertex.getValue().getInCoreness());//[vertex's incoreness,idx 1, changed outcoreness value1,idx 2,changed outcoreness value2... ]
    				int i = 1;
    				int [][]OutNeighborEstimate = new int [vertex.getNumEdges()+1][];
    				OutNeighborEstimate [0] = new int[vertex.getValue().getInCoreness()+2];
    				OutNeighborEstimate[0][0] = (int)vertex.getId().get();

    				System.arraycopy(vertex.getValue().getOutNeiCoreness()[0], 0, OutNeighborEstimate [0], 1, vertex.getValue().getInCoreness()+1);    //First row of OutNeighbors DCore estimate is the Dcore array of this vertex
    				
    				for(DCoreMessage message: messages) {
    					if(message.getState()==3){  // message is from outneighbor
    						OutNeighborEstimate[i] = new int[message.getInCoreness2()+2];
    						OutNeighborEstimate[i][0] = message.getSource();
    						System.arraycopy(message.getCoreness(), 0 , OutNeighborEstimate [i], 1 ,message.getCoreness().length);
    						i++;
    					}
    				}
    				
    				DCoreVertexValue value = new DCoreVertexValue(true,vertex.getValue().getInCoreness(),new int[2][2],OutNeighborEstimate); //false means the first superstep of outcoreness computation
        	        vertex.setValue(value);  			
        	        for(int j=0;j<=vertex.getValue().getInCoreness();j++) {
        				UpdatedCorenessEstimate.add(j);
        				UpdatedCorenessEstimate.add(vertex.getNumEdges());
        			}
        	        aggregate(DCoreMasterCompute.OutCorenessAggregator,new IntWritable(1));  
        	        
    			}
    			
    		//After the second superstep of OutCoreness computation
    		else{
    			 
    		UpdatedCorenessEstimate.add(vertex.getValue().getInCoreness());//[vertex's incoreness,idx 1, changed outcoreness value,idx 2,changed outcoreness value ]
    		
    		//first get the outcoreness estimate of outneighbor, update them according to the message
    		int [][]UpdatedOutNeighborEstimate = vertex.getValue().getOutNeiCoreness();
           
    		for(DCoreMessage message: messages) {   //update the OutNeighbor D-Core estimate according to the message  		
    			if(message.getState()==3&&message.getCoreness().length>1){ //getCoreness function return the D-core estimate(1D array) of message source vertex
    				for(int i=1;i<UpdatedOutNeighborEstimate.length;i++){
    					if(UpdatedOutNeighborEstimate[i][0]==message.getSource()){
    						int [] Coreness=message.getCoreness();  //[incoreness,idx1.updated outcoreness estimate2,idx2 updated outcoreness estimate2...]    						
        					int l=Coreness.length-1;    				   
        					for(int k =0;k<l/2;k++){
        						UpdatedOutNeighborEstimate[i][Coreness[2*k+1]+1]=Coreness[2*k+2]; //updated the according row of the 2d outneighbor coreness estimate 						
        					}   				
    					}		
    		}		
    		}
    		}
    		//compute the outcoreness
    		
			//the current coreness estimate of the vertex

    		int  []CorenessArray = new int [vertex.getValue().getInCoreness()+2];
    		CorenessArray = vertex.getValue().getOutNeiCoreness()[0];    //the first row of OutNeighbor D-core estimate is the estimate of this vertex itself 
    		
    		
    		boolean changed =false;//for all the incoreness of this vertex, any of the according outcoreness changed?
    		boolean flag= false;//whether there is a neighbor with a smaller outcoreness than this  vertex
    		
    		for(int i=0;i<=vertex.getValue().getInCoreness();i++) {
    		    int	estimate=UpdatedOutNeighborEstimate[0][i+1];//when the incoreness is i, what is the according outcoreness?  ????
    		    
    		    ArrayList<Integer> OutNeighborDegree = new ArrayList<Integer>();
    		
    		    
    		    flag = false;
    		 
    		    
    		    for(int j=1;j<UpdatedOutNeighborEstimate.length;j++){   //get the according outcoreness of the outNeighbors
    		    	if(UpdatedOutNeighborEstimate[j].length-2>=i){    //this OutNeighbor has incoreness estimate >= i
    		    		OutNeighborDegree.add(UpdatedOutNeighborEstimate[j][i+1]);
    		    		if(UpdatedOutNeighborEstimate[j][i+1]<vertex.getValue().getOutNeiCoreness()[0][i+1]) flag = true;	   	
    		    	}
    		    }
    		    if(OutNeighborDegree.size()==0&&MsgFromOut) { //there are messages from outneighbor, but none of them have incoreness >=i
    		    	CorenessArray[i+1]=0;
    		     }
    		    
    		    else{
    		    	if(flag) {    // only when there is outneighbors with outcoreness estimate smaller than the outcoreness estimate of this vertex, computation is needed    		    	
    				 int[] arr=new int[OutNeighborDegree.size()];  
        			 for(int k=0;k<OutNeighborDegree.size();k++) {
        			 arr[k]=OutNeighborDegree.get(k);
        			 } 
        		  Arrays.sort(arr);    //sort the OutNeighbor Outdegree estimate (small to big) to do the h-operation, return H-index 
       			  estimate=H_operation.h_operation(arr); 
       			
    			 } 	
    		    if(estimate<vertex.getValue().getOutNeiCoreness()[0][i+1]) {    // if new estimate is smaller than the original OutCoreness estimate, it will be sent to OutNeighbors
    		    	changed = true;
   		    	    CorenessArray[i+1]=estimate; 
   		    	    UpdatedCorenessEstimate.add(i);
   		    	    UpdatedCorenessEstimate.add(estimate);
    		    }
    		}
    		}
    		
    		if(changed) {
  			
    			UpdatedOutNeighborEstimate[0]=CorenessArray;  //update the first row of the 2d outcoreness estimate table, which is the outcoreness estimate of the vertex itself 
    			
    			DCoreVertexValue value = new DCoreVertexValue(true,vertex.getValue().getInCoreness(),new int [2][2], UpdatedOutNeighborEstimate);
    			vertex.setValue(value);
    			
    			aggregate(DCoreMasterCompute.OutCorenessAggregator,new IntWritable(1));//to judge whether outcoreness computation is done		
		    }
    		
    		}
    		
    		int [] UpdatedCorenessEstimateArray = new int [UpdatedCorenessEstimate.size()]; // tranform Arraylist to array, this array store the updated values needed to be sent
    		for(int i=0;i<UpdatedCorenessEstimate.size();i++){
				UpdatedCorenessEstimateArray[i]=UpdatedCorenessEstimate.get(i).intValue();
			}
    		
    		
    		
    		//send messages through inedges
    		Iterable<Edge<LongWritable, LongWritable>> edges = vertex.getEdges();
    		//send messages through  inedges 
    		byte which = 3; 
       		for(int i=0;i<source.size();i++) {
    			LongWritable lw=new LongWritable();
                lw.set((long)source.get(i)); 
    			sendMessage(lw, new DCoreMessage(which,(int) vertex.getId().get(),UpdatedCorenessEstimateArray));
    		}
       		    
       		 //through outedges
       		 which  = 2;
       		 for (Edge<LongWritable, LongWritable> edge : edges) {
             sendMessage(edge.getTargetVertexId(), new DCoreMessage(which,(int) vertex.getId().get()));       		    
    	   }  
    	
  }
    	
	   
   }
    
    	IntWritable NumOfVerticesOutcoreness = getAggregatedValue(DCoreMasterCompute.OutCorenessAggregator);
    	IntWritable NumOfVerticesIncoreness = getAggregatedValue(DCoreMasterCompute.InCorenessAggregator);
    	if(NumOfVerticesOutcoreness.get()==0&& NumOfVerticesIncoreness.get()==1) vertex.voteToHalt();
}
}

  

