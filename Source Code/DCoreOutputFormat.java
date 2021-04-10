package Dcore;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import Dcore.DCoreVertexValue;

import java.io.IOException;


public class DCoreOutputFormat extends TextVertexOutputFormat
  <LongWritable, DCoreVertexValue, LongWritable> {

  @Override
  public TextVertexWriter createVertexWriter(
      TaskAttemptContext context) {
    return new DCoreOutputFormatVertexWriter();
  }

  /**
  * Vertex Writer that outputs the following JSON:
  * [vertex id, vertex in-coreness value, according out-coreness array]
  * An example of this is:
  * [1,2,[5,4,2]]  (vertex id of 1 and in-coreness value of 2, and it belongs to (0,5)/(1,4)/(2,2)-core)
  */
  private class DCoreOutputFormatVertexWriter extends
    TextVertexWriterToEachLine {
    @Override

    public Text convertVertexToLine(
      Vertex<LongWritable, DCoreVertexValue, LongWritable> vertex
    ) throws IOException {

      JSONArray jsonVertex = new JSONArray();

      jsonVertex.put(vertex.getId().get());
	  jsonVertex.put(vertex.getValue().getInCoreness());
	  jsonVertex.put(vertex.getValue().getOutNeiCoreness()[0]);
      return new Text(jsonVertex.toString());
      
    }
  }
}
