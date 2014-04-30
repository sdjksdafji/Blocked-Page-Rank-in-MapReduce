package pagerank.input_preprocessing;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CaclulateTotalNumberOfNodeMapper extends
		Mapper<Object, Text, Text, IntWritable> {
	public void map(Object key, Text value, Context contex) throws IOException,
			InterruptedException {

	}
}
