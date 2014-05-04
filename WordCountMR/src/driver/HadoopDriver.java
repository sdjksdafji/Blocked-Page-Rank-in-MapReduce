package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import pagerank.BlockedPageRankReducer;
import pagerank.preprocessing.FormatInputMapper;
import wordcount.WordMapper;
import wordcount.WordReducer;

public class HadoopDriver {
	private static String JOB_HOME_DIR;
	private static String INPUT_FILE;

	public static void main(String[] args) throws Exception {

		validateInputArguments(args);

		startJobFlow();

	}

	private static Job getJobForWordCount(String inputDir, String outputDir)
			throws Exception {
		Job job = Job.getInstance(new Configuration(), "word count");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WordMapper.class);
		job.setReducerClass(WordReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.setJarByClass(HadoopDriver.class);
		return job;
	}

	private static Job getJobForCalculateTotalNumberOfNode(String inputDir,
			String outputDir) throws Exception {
		Job job = Job.getInstance(new Configuration(),
				"calculate total num of node");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(pagerank.preprocessing.CaclulateTotalNumberOfNodeMapper.class);
		job.setReducerClass(pagerank.preprocessing.CaclulateTotalNumberOfNodeReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.setNumReduceTasks(1);

		job.setJarByClass(HadoopDriver.class);
		return job;
	}

	private static Job getJobForInputFormat(String inputDir, String outputDir,
			int numOfNodes) throws Exception {
		Job job = Job.getInstance(new Configuration(), "formatting input");
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(pojo.PageRankValueWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(pagerank.preprocessing.FormatInputMapper.class);
		job.setReducerClass(pagerank.preprocessing.FormatInputReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		Configuration conf = job.getConfiguration();
		conf.set("Number of Nodes", Integer.toString(numOfNodes));

		job.setJarByClass(HadoopDriver.class);
		return job;
	}

	private static Job getJobForOutputFormat(String inputDir, String outputDir)
			throws Exception {
		Job job = Job.getInstance(new Configuration(), "formatting onput");
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(pojo.PageRankValueWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(pagerank.postprocessing.FormatOutputMapper.class);
		job.setReducerClass(pagerank.postprocessing.FormatOutputReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		job.setJarByClass(HadoopDriver.class);
		return job;
	}

	private static Job getJobForCalculatingPageRank(String inputDir,
			String outputDir, int numOfNodes, boolean method) throws Exception {
		Job job = Job.getInstance(new Configuration(), "calculating page rank");
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(pojo.PageRankValueWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(pagerank.BlockedPageRankMapper.class);
		job.setReducerClass(pagerank.BlockedPageRankReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(inputDir));
		FileOutputFormat.setOutputPath(job, new Path(outputDir));

		Configuration conf = job.getConfiguration();
		conf.set("Method", Boolean.toString(method));
		conf.set("Number of Nodes", Integer.toString(numOfNodes));

		job.setJarByClass(HadoopDriver.class);
		return job;
	}

	private static void validateInputArguments(String[] args) {
		if (args.length != 3) {
			System.out
					.println("usage: \n[use Gauss-Seidel or Jacobi]\n[input file or directory]\n[home directory of the job in s3n protocol]");
			System.exit(-1);
		}
		if (args[0].charAt(0) == 'j' || args[0].charAt(0) == 'J') {
			BlockedPageRankReducer.jacobAndGaussian = true;
		} else if (args[0].charAt(0) == 'g' || args[0].charAt(0) == 'G') {
			BlockedPageRankReducer.jacobAndGaussian = false;
		} else {
			System.out
					.println("usage: \n[use gaussian or jacobi]\n[home directory of the job in s3n protocol]");
			System.exit(-1);
		}

		INPUT_FILE = args[1];

		JOB_HOME_DIR = args[2];
		if (JOB_HOME_DIR.charAt(JOB_HOME_DIR.length() - 1) != '/') {
			JOB_HOME_DIR = JOB_HOME_DIR + "/";
		}

		System.out
				.println("Using "
						+ (BlockedPageRankReducer.jacobAndGaussian ? "Jacobi"
								: "Gauss-Seidel")
						+ " method to calculate page rank");
		System.out.println("The output directory for this job is \""
				+ JOB_HOME_DIR + "\"");
	}

	private static void startJobFlow() throws Exception {
		System.out.println("Start running job flow");

		Job job = getJobForCalculateTotalNumberOfNode(INPUT_FILE, JOB_HOME_DIR
				+ ConfigurationParameter.NODE_NUMBER_DIR);

		System.out
				.println("Start calculating total number of nodes fits in the range...");
		System.out.println("Taking input file from \"" + INPUT_FILE + "\"");
		System.out.println("Writing output file to \"" + JOB_HOME_DIR
				+ ConfigurationParameter.NODE_NUMBER_DIR + "\"");

		job.waitForCompletion(true);

		Counters counters = job.getCounters();
		Counter c = counters.findCounter(PAGE_RANK_COUNTER.NUM_OF_NODES);

		System.out.println("Total number of nodes in this graph is "
				+ c.getValue());

		int numOfNodes = (int) c.getValue();

		// ---------------------------------------------------------

		job = getJobForInputFormat(INPUT_FILE, JOB_HOME_DIR
				+ ConfigurationParameter.getPageRankIterationDirectory(0),
				numOfNodes);

		System.out.println("Start formatting input...");
		System.out.println("Taking input file from \"" + INPUT_FILE + "\"");
		System.out.println("Writing output file to \"" + JOB_HOME_DIR
				+ ConfigurationParameter.getPageRankIterationDirectory(0)
				+ "\"");

		job.waitForCompletion(true);

		System.out.println("Waiting for eventual consistency of S3");
		Thread.sleep(ConfigurationParameter.EVENTUAL_CONSISTENCY_WAIT_TIME * 1000);
		// ---------------------------------------------------------

		int iter = 0;
		while (true) {
			job = getJobForCalculatingPageRank(
					JOB_HOME_DIR
							+ ConfigurationParameter
									.getPageRankIterationDirectory(iter),
					JOB_HOME_DIR
							+ ConfigurationParameter
									.getPageRankIterationDirectory(iter + 1),
					numOfNodes, BlockedPageRankReducer.jacobAndGaussian);

			System.out.println("Start iteration " + (iter + 1) + "...");

			job.waitForCompletion(true);

			counters = job.getCounters();
			c = counters.findCounter(PAGE_RANK_COUNTER.TOTAL_INNER_ITERATION);
			System.out.println("The number of iterations in all reducers: "
					+ c.getValue());
			c = counters.findCounter(PAGE_RANK_COUNTER.UNCONVERGED_REDUCER);
			System.out.println("The number of unconverged reducer: "
					+ c.getValue());

			iter++;
			System.out.println("Waiting for eventual consistency of S3");
			Thread.sleep(ConfigurationParameter.EVENTUAL_CONSISTENCY_WAIT_TIME * 1000);

			if (c.getValue() == 0) {
				System.out.println("The page ranks have converged...");
				break;
			}
		}
		// ---------------------------------------------------------

		job = getJobForOutputFormat(
				JOB_HOME_DIR
						+ ConfigurationParameter
								.getPageRankIterationDirectory(iter),
				JOB_HOME_DIR + ConfigurationParameter.OUTPUT_DIR);

		System.out.println("Start formatting output...");
		System.out.println("Taking input file from \"" + JOB_HOME_DIR
				+ ConfigurationParameter.getPageRankIterationDirectory(iter)
				+ "\"");
		System.out.println("Writing output file to \"" + JOB_HOME_DIR
				+ ConfigurationParameter.OUTPUT_DIR + "\"");

		job.waitForCompletion(true);

	}
}