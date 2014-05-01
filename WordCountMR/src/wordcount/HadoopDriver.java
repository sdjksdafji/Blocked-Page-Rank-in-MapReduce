package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class HadoopDriver {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}

		System.out.println("start running");
		Job job = getJobForCalculatingPageRank(args);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	private static Job getJobForWordCount(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "word count");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(HadoopDriver.class);
		return job;
	}

	private static Job getJobForCalculateTotalNumberOfNode(String[] args)
			throws Exception {
		Job job = Job.getInstance(new Configuration(),
				"calculate total num of node");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(pagerank.input_preprocessing.CaclulateTotalNumberOfNodeMapper.class);
		job.setReducerClass(IntSumReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setNumReduceTasks(1);

		job.setJarByClass(HadoopDriver.class);
		return job;
	}

	private static Job getJobForInputFormat(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "formatting input");
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(pojo.PageRankValueWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(pagerank.input_preprocessing.InputFormatMapper.class);
		job.setReducerClass(pagerank.input_preprocessing.InputFormatReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(HadoopDriver.class);
		return job;
	}
	
	private static Job getJobForCalculatingPageRank(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration(), "calculating page rank");
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(pojo.PageRankValueWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(pagerank.PageRankMapper.class);
		job.setReducerClass(pagerank.PageRankReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(HadoopDriver.class);
		return job;
	}
}