package pagerank.input_preprocessing;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pojo.PageRankValueWritable;

public class InputFormatReducer extends
		Reducer<IntWritable, PageRankValueWritable, IntWritable, Text> {
	private IntWritable blockIdWritable = new IntWritable();
	private Text outputText = new Text();
	private Map<Integer, Set<Integer>> edgeSet = new HashMap<Integer, Set<Integer>>();
	private Map<Integer, Integer> vertexToBlock = new HashMap<Integer, Integer>();

	@Override
	public void reduce(IntWritable key, Iterable<PageRankValueWritable> values,
			Context context) throws IOException, InterruptedException {
		for (PageRankValueWritable value : values) {
			if (value.isNodeInformation()) {

				if (!edgeSet.containsKey(value.getVertexId())) {
					edgeSet.put(value.getVertexId(), new HashSet<Integer>());
				}
				Set<Integer> destinations = edgeSet.get(value.getVertexId());
				destinations.add(value.getEdgeVertex());
				edgeSet.put(value.getVertexId(), destinations);

				vertexToBlock.put(value.getEdgeVertex(), value.getEdgeBlock());
			}
		}

		this.blockIdWritable.set(key.get());

		for (PageRankValueWritable value : values) {
			if (value.isNodeInformation()) {
				if (!edgeSet.containsKey(value.getVertexId())) {
					System.err.println("serious error");
					break;
				}
				Set<Integer> destinations = edgeSet.get(value.getVertexId());

				String ouput = "" + value.getVertexId() + " "
						+ value.getCurrentPageRank() + " "
						+ value.getEdgeBlock() + " " + value.getEdgeVertex()
						+ " " + destinations.size();

				outputText.set(ouput);
				context.write(blockIdWritable, outputText);
			}
		}
	}

}
