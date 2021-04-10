package Dcore;

/* Giraph dependencies */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.worker.WorkerContext;

/* Hadoop dependencies */
import org.apache.log4j.Logger;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/* Aggregator information */
import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

/*MasterCompute is executed in the master node at the beginning of each superstep/before the computation at worker nodes.
 * 
 */




public class DCoreMasterCompute extends DefaultMasterCompute {
  /** 
  * Class logger 
  */
  /*private final static Logger logger = Logger.getLogger(DCore.class.getName());*/

  /**
  * Aggregator values.
  */
	//Text InCorenessAggregator;
	//Text OutCorenessAggregator;
  
	public static String InCorenessAggregator = "InCoreness";
    public static String OutCorenessAggregator = "OutCoreness";
    
    

  @Override
  public void initialize() throws InstantiationException,
      IllegalAccessException {
    
    registerAggregator(InCorenessAggregator, IntSumAggregator.class);
    registerPersistentAggregator(OutCorenessAggregator, IntSumAggregator.class);
    
  }

  @Override
  public void compute() {
	  
	  
	  if(getSuperstep() == 0){
		  setAggregatedValue(InCorenessAggregator, new IntWritable(0));//0 means incoreness computation is done
		  setAggregatedValue(OutCorenessAggregator, new IntWritable(0)); // 1 means outcoreness computation  is done
	  }
	  else{
		  
	  //int InOrOut=0;// to check which computation is ongoing: 0:incoreness 1:outcoreness
	  
      IntWritable NumOfActivateVerticesIncoreness = getAggregatedValue(InCorenessAggregator);
      IntWritable NumOfActivateVerticesOutcoreness  = getAggregatedValue(OutCorenessAggregator);
     
	  
	  
	  
	  if(((int)NumOfActivateVerticesIncoreness.get() ==0)&&((int)NumOfActivateVerticesOutcoreness.get()==1)){
		  haltComputation();   // verify = 1 means the verification is done 
	  }
	  
	  if(NumOfActivateVerticesOutcoreness.get()!=0){setAggregatedValue(OutCorenessAggregator, new IntWritable(1));}
   }
  }
  
  @Override
  public void readFields(DataInput In) throws IOException {
	  // To serialize this class fields (global variables)
	  this.InCorenessAggregator=In.readUTF();
	  this.OutCorenessAggregator=In.readUTF();
	  
	   }
  @Override
  public void write(DataOutput Out) throws IOException {
		// To serialize this class fields (global variables) if any
	  Out.writeUTF(this.InCorenessAggregator);
	  Out.writeUTF(this.OutCorenessAggregator);
	  
	  
	}
  
} 

