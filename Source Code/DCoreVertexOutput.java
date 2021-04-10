package Dcore;

import java.io.IOException;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import Dcore.DCoreVertexValue;
/**
 * Simple vertex output format for weighted graphs.
 */
public class DCoreVertexOutput extends
    TextVertexOutputFormat<LongWritable, DCoreVertexValue, DoubleWritable> {
  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new DCoreVertexWriter();
  }

  /**
   * Vertex writer used with
   * {@link KCoreVertexOutputFormat}.
   */
  public class DCoreVertexWriter extends TextVertexWriter {
    @Override
    public void writeVertex(
      Vertex<LongWritable, DCoreVertexValue, DoubleWritable> vertex)
      throws IOException, InterruptedException {
      StringBuilder output = new StringBuilder();
      output.append(vertex.getId().get());
      output.append('\t');   //a tab in String
      output.append(vertex.getValue().getInCoreness());
      output.append('\t');   //a tab in String
      output.append(vertex.getValue().getOutNeiCoreness()[0]);
      getRecordWriter().write(new Text(output.toString()), null);
    }
  }
}
