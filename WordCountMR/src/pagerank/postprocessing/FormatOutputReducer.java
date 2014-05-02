package pagerank.postprocessing;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pojo.PageRankValueWritable;

public class FormatOutputReducer extends
		Reducer<IntWritable, PageRankValueWritable, Text, Text> {
	private Set<Integer> finishedVertex = new HashSet<Integer>();

	private Text keyText = new Text();
	private Text valueText = new Text();

	@Override
	public void reduce(IntWritable key, Iterable<PageRankValueWritable> values,
			Context context) throws IOException, InterruptedException {
		valueText.set("");
		for (PageRankValueWritable value : values) {
			if (!finishedVertex.contains(value.getVertexId())) {
				String outputStr = "[ Vertex: " + value.getVertexId()
						+ " ] -> [ Page Rank: " + value.getCurrentPageRank()
						+ " ]";
				valueText.set(outputStr);
				context.write(keyText, valueText);
				finishedVertex.add(value.getVertexId());
			}
		}
	}
}
