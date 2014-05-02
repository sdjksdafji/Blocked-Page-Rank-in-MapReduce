package pagerank.preprocessing;

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

public class FormatInputReducer extends
		Reducer<IntWritable, PageRankValueWritable, IntWritable, Text> {
	private IntWritable blockIdWritable = new IntWritable();
	private Text outputText = new Text();
	private Map<Integer, Set<Integer>> edgeSet = new HashMap<Integer, Set<Integer>>();
	private List<PageRankValueWritable> list = new ArrayList<PageRankValueWritable>();
	private Set<Integer> writtenSelfLoopedNode = new HashSet<Integer>();

	@Override
	public void reduce(IntWritable key, Iterable<PageRankValueWritable> values,
			Context context) throws IOException, InterruptedException {
		for (PageRankValueWritable value : values) {
			if (value.isNodeInformation()) {
				if (!value.isSelfLooped()) {
					if (!edgeSet.containsKey(value.getVertexId())) {
						edgeSet.put(value.getVertexId(), new HashSet<Integer>());
					}
					Set<Integer> destinations = edgeSet
							.get(value.getVertexId());
					destinations.add(value.getEdgeVertex());
					edgeSet.put(value.getVertexId(), destinations);
					
				}
				list.add(value.clone());
			}
		}

		this.blockIdWritable.set(key.get());

		for (PageRankValueWritable value : list) {
			if (value.isNodeInformation()) {
				if (!value.isSelfLooped()) {
					if (!edgeSet.containsKey(value.getVertexId())) {
						System.err.println("serious error");
						ouputTestInfo(-1, "serious error", context);
						break;
					}
					emitEdgeInfo(value, context);
				} else {
					if (!this.writtenSelfLoopedNode.contains(value.getVertexId())) {
						emitEdgeInfo(value, context);
						this.writtenSelfLoopedNode.add(value.getVertexId());
					}
				}
			}
		}
	}

	private void ouputTestInfo(int key, String str, Context context)
			throws IOException, InterruptedException {
		this.blockIdWritable.set(key);
		this.outputText.set(str);
		context.write(blockIdWritable, outputText);
	}

	private void emitEdgeInfo(PageRankValueWritable value, Context context)
			throws IOException, InterruptedException {
		int degree = 0;
		if (edgeSet.containsKey(value.getVertexId())) {
			Set<Integer> destinations = edgeSet.get(value.getVertexId());
			degree = destinations.size();
		}

		String ouput = "" + value.getVertexId() + " "
				+ value.getCurrentPageRank() + " " + value.getEdgeBlock() + " "
				+ value.getEdgeVertex() + " " + degree;

		outputText.set(ouput);
		context.write(blockIdWritable, outputText);
	}

}
