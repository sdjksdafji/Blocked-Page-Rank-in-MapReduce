package pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pojo.PageRankValueWritable;

public class PageRankReducer extends
		Reducer<IntWritable, PageRankValueWritable, IntWritable, Text> {
	
	@Override
	public void reduce(IntWritable key, Iterable<PageRankValueWritable> values, Context context){
		for(PageRankValueWritable value:values){
			if(value.isNodeInformation()){
				
			}else if(value.isSumInformation()){
				
			}
		}
	}

}
