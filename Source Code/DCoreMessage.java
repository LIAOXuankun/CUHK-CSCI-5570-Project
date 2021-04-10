package Dcore;
//meghods to be implemented: getSource/getInCoreness/getState/getCoreness
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;







import javax.swing.text.html.HTMLDocument.Iterator;

import net.sf.cglib.core.CollectionUtils;

import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

public class DCoreMessage implements Writable, Configurable, Cloneable {

  private Configuration conf;
  private int  Source;
  private int InCoreness;
  //private ArrayList<Integer> Coreness; 
  private int[] Coreness;
  private byte Which=0;

  /**
  * Default Constructor.
  */
  public DCoreMessage() {
	  //this.Coreness = new ArrayList<Integer>();
      //this.Coreness =  ;  
  }

  /**
  * Paramaterized Constructor.
  * @param neighbor id of the orginating vertex
  * @param deleted  deleted count
  */
  
  public DCoreMessage(byte which) {  //used in SP 0, which = 0
	this.Which=which;
    
  }
  
 
  
  public DCoreMessage(byte which, int InCoreness,int Source) {//used in incoreness computation,which = 1 
		this.Which=which;	    
	    this.InCoreness = InCoreness;
	    this.Source = Source;
	  }
  
  public DCoreMessage(byte which, int Source) {// used in outcoreness computation ,which = 2, messages sent via outedges  
		this.Which=which;	    
	    this.Source = Source;
	  }
  
  public DCoreMessage(byte which, int source, int [] coreness) {//used in outcoreness computation,which = 3  
		this.Which=which;
	    this.Source = source;
	    this.Coreness = new int [coreness.length] ;
	    System.arraycopy(coreness, 0, Coreness, 0, coreness.length);
	    
	  }
  
  
  
  
  @Override
  public void write(DataOutput output) throws IOException {
	  
	  output.writeByte(this.Which);
	  //System.out.println("serilization state:"+which);
	  if (Which  ==2){
		  output.writeInt(this.Source);
	  }
	  else if(Which == 1){
		  output.writeInt(this.InCoreness);
		  output.writeInt(this.Source);
	  }
	  else if (Which == 3 ||Which ==4){
		  //System.out.println(Which);
		  output.writeInt(this.Source);
		  String s = "";		  
		    for (int r:this.Coreness) {
		    	s += String.valueOf(r);
		    	s += " ";
		    }   		  
		  output.writeUTF(s);
	  }
	  
	  
	  
    
  }
  
  
 @Override 
 public void readFields(DataInput input) throws IOException {
	 this.Which = input.readByte();
	 if(Which == 2){
		 this.Source = input.readInt(); 
	 }
	 else if(Which ==1){
		 this.InCoreness = input.readInt();
		 this.Source = input.readInt();
	 }
	 else if (Which == 3||Which==4){
		 this.Source = input.readInt();
		 String s = input.readUTF();
		    if(!s.equals("")){
		    ArrayList<Integer> tmp = new ArrayList<Integer>();
		    
		    
		    
		    //s = s.trim();
		    String[] split_string = s.split(" ");
		    for(String r: split_string) {
		    	tmp.add(Integer.valueOf(r));
		    }
		    this.Coreness = new int [tmp.size()];
		    for(int i=0;i<tmp.size();i++){
		    	Coreness[i]=tmp.get(i);
		    }
		    }
	 }
	 
 }




  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  /**
   * @return int/ the  source id
   */
  public int getSource() {
    return Source;
  }

  /**
   * @return int/ the InCoreness
   */
  public int getInCoreness2() {  //used in the outcoreness computation
    
	  return Coreness.length-1;
  }

  public int getInCoreness1() {  //used in the incoreness computation
	    return InCoreness;
		  
	  }
  
  
  
  
  /**
   * @return int/ the State  1: received message was sent through outedges  2:received message was sent through inedges
   */
  public byte getState() {
    return Which;
  }
  
  /**
   * @return ArrayList  the Coreness
   */
  public  int[] getCoreness() {
    return Coreness;
  }
  
  
}
