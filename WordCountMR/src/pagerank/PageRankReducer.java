package pagerank;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pojo.PageRankValueWritable;

public class PageRankReducer extends
		Reducer<IntWritable, PageRankValueWritable, IntWritable, Text> {
	
	private static final double DAMPING_FACTOR = 0.8;
	private static boolean jacobAndGaussian = true;
	private Map<Integer,Double> previousPageRank = new HashMap<Integer,Double>();
	private Map<Integer,Double> pageRank = new HashMap<Integer,Double>();
	
	@Override
	public void reduce(IntWritable key, Iterable<PageRankValueWritable> values, Context context){
		for(PageRankValueWritable value:values){
			if(value.isNodeInformation()){
				this.previousPageRank.put(value.getVertexId(), 0.0);
			}
		}
	}

}
