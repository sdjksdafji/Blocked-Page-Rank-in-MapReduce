package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import pagerank.preprocessing.FormatInputMapper;
import pojo.PageRankValueWritable;
import driver.ConfigurationParameter;
import driver.PAGE_RANK_COUNTER;

public class SimplePageRankReducer extends
		Reducer<IntWritable, PageRankValueWritable, IntWritable, Text> {

	private IntWritable blockIdWritable = new IntWritable();
	private Text outputText = new Text();

	private double previousPageRank = 0;
	private double pageRank = 0;
	private List<PageRankValueWritable> outcomingEdges;
	private List<PageRankValueWritable> sumInfo;

	@Override
	public void reduce(IntWritable key, Iterable<PageRankValueWritable> values,
			Context context) throws IOException, InterruptedException {
		initAlgorithm(context);

		readDataFromMapper(key, values);

		computePageRank(context);

		emitTheLocallyConvergedPageRank(context);
	}

	private void initAlgorithm(Context context) {
		Configuration conf = context.getConfiguration();
		String numOfNodesStr = conf.get("Number of Nodes");
		FormatInputMapper.numOfNodes = Integer.parseInt(numOfNodesStr);

		this.blockIdWritable.set(0); // block id is irrelevant in simple page
										// rank algorithm

		sumInfo = new ArrayList<PageRankValueWritable>();
		outcomingEdges = new ArrayList<PageRankValueWritable>();
	}

	private void readDataFromMapper(IntWritable key,
			Iterable<PageRankValueWritable> values) {
		for (PageRankValueWritable value : values) {
			if (value.isNodeInformation()) {
				this.previousPageRank = value.getCurrentPageRank();
				this.outcomingEdges.add(value.clone());
			} else if (value.isSumInformation()) {
				this.sumInfo.add(value.clone());
			}
		}
	}

	private void computePageRank(Context context) throws IOException,
			InterruptedException {
		double maxDiff = 0.0;
		this.pageRank = (1.0 - BlockedPageRankReducer.DAMPING_FACTOR)
				/ FormatInputMapper.numOfNodes;
		for (PageRankValueWritable incomeVertex : sumInfo) {
			this.pageRank += BlockedPageRankReducer.DAMPING_FACTOR
					* (incomeVertex.getCurrentPageRank() / ((double) incomeVertex
							.getDegree()));
		}

		maxDiff = Math.abs(pageRank - previousPageRank) / pageRank;

		setResidualError(context);

		context.getCounter(PAGE_RANK_COUNTER.TOTAL_INNER_ITERATION)
				.increment(1);
		if (maxDiff > BlockedPageRankReducer.EPSILON) {
			context.getCounter(PAGE_RANK_COUNTER.UNCONVERGED_REDUCER)
					.increment(1);
		}

	}

	private void setResidualError(Context context) {
		double accumulativeResidualError = Math
				.abs(pageRank - previousPageRank) / pageRank;
		long normalizedAccumulativeResidualError = (long) (accumulativeResidualError * ConfigurationParameter.RESIDUAL_ERROR_ACCURACY);
		context.getCounter(PAGE_RANK_COUNTER.ACCUMULATIVE_RESIDUAL_ERROR)
				.increment(normalizedAccumulativeResidualError);
		context.getCounter(PAGE_RANK_COUNTER.NUM_OF_RESIDUAL_ERROR)
				.increment(1);
	}

	private void emitTheLocallyConvergedPageRank(Context context)
			throws IOException, InterruptedException {
		for (PageRankValueWritable value : outcomingEdges) {

			String ouput = "" + value.getVertexId() + " " + this.pageRank + " "
					+ value.getEdgeBlock() + " " + value.getEdgeVertex() + " "
					+ value.getDegree();

			outputText.set(ouput);
			context.write(blockIdWritable, outputText);

		}
	}

}
