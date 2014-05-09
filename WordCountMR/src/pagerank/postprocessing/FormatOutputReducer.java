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

	private double maxPageRank = -1;
	private int maxVertex = -1;
	private int maxBlock = -1;

	private Text keyText = new Text();
	private Text valueText = new Text();

	@Override
	public void reduce(IntWritable key, Iterable<PageRankValueWritable> values,
			Context context) throws IOException, InterruptedException {
		valueText.set("");
		for (PageRankValueWritable value : values) {
			if (value.getCurrentPageRank() > this.maxPageRank) {
				this.maxPageRank = value.getCurrentPageRank();
				this.maxVertex = value.getVertexId();
				maxBlock = key.get();
			}
		}
		keyText.set("The vertex has largest PR in block : " + maxBlock);
		String outputStr = "[ Vertex: " + maxVertex + " ] -> [ Page Rank: "
				+ maxPageRank + " ]";
		valueText.set(outputStr);
		context.write(keyText, valueText);
	}
}
