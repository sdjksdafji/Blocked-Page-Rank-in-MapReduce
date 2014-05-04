package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import driver.ConfigurationParameter;
import driver.PAGE_RANK_COUNTER;
import pagerank.preprocessing.FormatInputMapper;
import pojo.PageRankValueWritable;

public class BlockedPageRankReducer extends
		Reducer<IntWritable, PageRankValueWritable, IntWritable, Text> {

	public static final double EPSILON = 1e-3;
	public static final double DAMPING_FACTOR = 0.85;
	public static boolean jacobAndGaussian = false; // true = jacobi; false =
													// gaussian

	private IntWritable blockIdWritable = new IntWritable();
	private Text outputText = new Text();

	private Map<Integer, Double> initialPageRank;
	private Map<Integer, Double> previousPageRank;
	private Map<Integer, Double> pageRank;
	private Map<Integer, List<PageRankValueWritable>> incomingEdges;
	private List<PageRankValueWritable> outcomingEdges;
	private Set<Integer> finishedNodes;

	@Override
	public void reduce(IntWritable key, Iterable<PageRankValueWritable> values,
			Context context) throws IOException, InterruptedException {
		initAlgorithm(context);

		readDataFromMapper(key, values);

		computeUntilPageRanksConverge(context);

		emitTheLocallyConvergedPageRank(context);
	}

	private void initAlgorithm(Context context) {
		Configuration conf = context.getConfiguration();
		String methodStr = conf.get("Method");
		jacobAndGaussian = Boolean.parseBoolean(methodStr);
		String numOfNodesStr = conf.get("Number of Nodes");
		FormatInputMapper.numOfNodes = Integer.parseInt(numOfNodesStr);

		previousPageRank = new HashMap<Integer, Double>();
		initialPageRank = new HashMap<Integer, Double>();
		pageRank = new HashMap<Integer, Double>();
		incomingEdges = new HashMap<Integer, List<PageRankValueWritable>>();
		outcomingEdges = new ArrayList<PageRankValueWritable>();
		finishedNodes = new HashSet<Integer>();
	}

	private void readDataFromMapper(IntWritable key,
			Iterable<PageRankValueWritable> values) {
		this.blockIdWritable.set(key.get());
		for (PageRankValueWritable value : values) {
			if (value.isNodeInformation()) {
				this.previousPageRank.put(value.getVertexId(),
						value.getCurrentPageRank());
				this.initialPageRank.put(value.getVertexId(),
						value.getCurrentPageRank());
				outcomingEdges.add(value.clone());
			} else if (value.isSumInformation()) {
				if (!this.incomingEdges.containsKey(value.getVertexId())) {
					this.incomingEdges.put(value.getVertexId(),
							new ArrayList<PageRankValueWritable>());
				}
				List<PageRankValueWritable> list = this.incomingEdges.get(value
						.getVertexId());
				list.add(value.clone());
				this.incomingEdges.put(value.getVertexId(), list);
			}
		}
	}

	private void computeUntilPageRanksConverge(Context context)
			throws IOException, InterruptedException {
		boolean isReducerConverged = true;
		while (true) {
			double maxDiff = 0.0;
			this.finishedNodes = new HashSet<Integer>();
			for (PageRankValueWritable value : outcomingEdges) {
				if (!this.finishedNodes.contains(value.getVertexId())) {

					double currentPageRank = (1.0 - DAMPING_FACTOR)
							/ FormatInputMapper.numOfNodes;
					double previousPageRank = this.previousPageRank.get(value
							.getVertexId());
					List<PageRankValueWritable> list = this.incomingEdges
							.get(value.getVertexId());
					if (list != null) {
						for (PageRankValueWritable incomeVertex : list) {
							currentPageRank += DAMPING_FACTOR
									* (getLatestPageRankOfVertex(incomeVertex) / ((double) incomeVertex
											.getDegree()));
						}
					}
					this.pageRank.put(value.getVertexId(), currentPageRank);
					this.finishedNodes.add(value.getVertexId());
					double diff = Math.abs(currentPageRank - previousPageRank)
							/ currentPageRank;
					maxDiff = diff > maxDiff ? diff : maxDiff;
				}
			}

			this.previousPageRank = this.pageRank;
			this.pageRank = new HashMap<Integer, Double>();

			context.getCounter(PAGE_RANK_COUNTER.TOTAL_INNER_ITERATION)
					.increment(1);

			if (maxDiff < EPSILON || this.finishedNodes.size() == 0) {
				break;
			}
			isReducerConverged = false;

		}

		setResidualError(context);

		if (!isReducerConverged) {
			context.getCounter(PAGE_RANK_COUNTER.UNCONVERGED_REDUCER)
					.increment(1);
		}
	}

	private void setResidualError(Context context) {
		double accumulativeResidualError = 0;
		for (int vertexId : this.finishedNodes) {
			double startPageRank = this.initialPageRank.get(vertexId);
			double endPageRank = this.previousPageRank.get(vertexId);
			accumulativeResidualError += Math.abs(startPageRank - endPageRank)
					/ endPageRank;
		}
		long normalizedAccumulativeResidualError = (long) (accumulativeResidualError * ConfigurationParameter.RESIDUAL_ERROR_ACCURACY);
		context.getCounter(PAGE_RANK_COUNTER.ACCUMULATIVE_RESIDUAL_ERROR)
				.increment(normalizedAccumulativeResidualError);
		context.getCounter(PAGE_RANK_COUNTER.NUM_OF_RESIDUAL_ERROR).increment(
				finishedNodes.size());
	}

	private double getLatestPageRankOfVertex(PageRankValueWritable incomeVertex) {
		int vertexId = incomeVertex.getEdgeVertex();
		int blockId = incomeVertex.getEdgeBlock();
		if (blockId != this.blockIdWritable.get()) {
			return incomeVertex.getCurrentPageRank();
		} else {
			if (jacobAndGaussian) {
				return previousPageRank.get(vertexId);
			} else {
				if (this.finishedNodes.contains(vertexId)) {
					return pageRank.get(vertexId);
				} else {
					return previousPageRank.get(vertexId);
				}
			}
		}
	}

	private void emitTheLocallyConvergedPageRank(Context context)
			throws IOException, InterruptedException {
		for (PageRankValueWritable value : outcomingEdges) {

			String ouput = "" + value.getVertexId() + " "
					+ this.previousPageRank.get(value.getVertexId()) + " "
					+ value.getEdgeBlock() + " " + value.getEdgeVertex() + " "
					+ value.getDegree();

			outputText.set(ouput);
			context.write(blockIdWritable, outputText);

		}
	}

}
