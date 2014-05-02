package pagerank.preprocessing;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import driver.PAGE_RANK_COUNTER;

public class CaclulateTotalNumberOfNodeReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable totalNumberOfNodes = new IntWritable();
	private Set<Integer> nodeSet = new HashSet<Integer>();
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		for(IntWritable value:values){
			nodeSet.add(value.get());
		}
		
		totalNumberOfNodes.set(nodeSet.size());
		context.write(key, totalNumberOfNodes);
		context.getCounter(PAGE_RANK_COUNTER.NUM_OF_NODES)
		.increment(nodeSet.size());
	}
}
