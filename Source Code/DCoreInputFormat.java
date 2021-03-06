package Dcore;


import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
  * VertexInputFormat that features <code>long</code> vertex ID's,
  * <code>double</code> vertex values and <code>float</code>
  * out-edge weights, and <code>double</code> message types,
  *  specified in JSON format.
  */
public class DCoreInputFormat extends
  TextVertexInputFormat<LongWritable, DCoreVertexValue, LongWritable> {
    
  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new DCoreInputFormatVertexReader();
  }

 /**
  * VertexReader that features <code>double</code> vertex
  * values and <code>float</code> out-edge weights. The
  * files should be in the following JSON format:
  * JSONArray(<vertex id>, <vertex value>,
  *   JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
  * Here is an example with vertex id 1, vertex value 4.3, and two edges.
  * First edge has a destination vertex 2, edge value 2.1.
  * Second edge has a destination vertex 3, edge value 0.7.
  * [1,4.3,[[2,2.1],[3,0.7]]]
  */
  class DCoreInputFormatVertexReader extends
    TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
    JSONException> {

    @Override
    protected JSONArray preprocessLine(Text line) throws JSONException {
      return new JSONArray(line.toString());
    }

    @Override
    protected LongWritable getId(JSONArray jsonVertex) throws JSONException,
              IOException {
      return new LongWritable(jsonVertex.getLong(0));
    }

    @Override
    protected DCoreVertexValue getValue(JSONArray jsonVertex) throws
      JSONException, IOException {
       return new DCoreVertexValue();
    	 //return new DoubleWritable(jsonVertex.getDouble(1));
    }

    @Override
    protected Iterable<Edge<LongWritable, LongWritable>>
    getEdges(JSONArray jsonVertex) throws JSONException, IOException {

      JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);

      /* get the edges */
      List<Edge<LongWritable, LongWritable>> edges =
          Lists.newArrayListWithCapacity(jsonEdgeArray.length());

      for (int i = 0; i < jsonEdgeArray.length(); ++i) {
        LongWritable targetId;
        LongWritable tag;
        JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);

        targetId = new LongWritable(jsonEdge.getLong(0));
        tag = new LongWritable((long) jsonEdge.getLong(1));
        edges.add(EdgeFactory.create(targetId, tag));
      }
      return edges;
    }

    @Override
    protected Vertex<LongWritable, DCoreVertexValue,
      LongWritable> handleException(Text line, JSONArray jsonVertex,
          JSONException e) {
      throw new IllegalArgumentException(
          "Couldn't get vertex from line " + line, e);
    }

  }
}
