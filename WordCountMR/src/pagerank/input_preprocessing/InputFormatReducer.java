package pagerank.input_preprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
	private List<PageRankValueWritable> list = new ArrayList<PageRankValueWritable>();

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
				
				list.add(value.clone());
			}
		}


		this.blockIdWritable.set(key.get());

		for (PageRankValueWritable value : list) {
			if (value.isNodeInformation()) {
				if (!edgeSet.containsKey(value.getVertexId())) {
					System.err.println("serious error");
					ouputTestInfo(-1, "serious error", context);
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

	private void ouputTestInfo(int key, String str, Context context)
			throws IOException, InterruptedException {
		this.blockIdWritable.set(key);
		this.outputText.set(str);
		context.write(blockIdWritable, outputText);
	}

}
