package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import driver.PAGE_RANK_COUNTER;
import pagerank.preprocessing.FormatInputMapper;
import pojo.PageRankValueWritable;

public class PageRankReducer extends
		Reducer<IntWritable, PageRankValueWritable, IntWritable, Text> {

	private static final double EPSILON = 1e-5;
	private static final double DAMPING_FACTOR = 0.85;
	public static boolean jacobAndGaussian = false; // true = jacobi; false =
														// gaussian

	private IntWritable blockIdWritable = new IntWritable();
	private Text outputText = new Text();

	private Map<Integer, Double> previousPageRank = new HashMap<Integer, Double>();
	private Map<Integer, Double> pageRank = new HashMap<Integer, Double>();
	private Map<Integer, List<PageRankValueWritable>> incomingEdges = new HashMap<Integer, List<PageRankValueWritable>>();
	private List<PageRankValueWritable> outcomingEdges = new ArrayList<PageRankValueWritable>();
	private Set<Integer> finishedNodes = new HashSet<Integer>();

	@Override
	public void reduce(IntWritable key, Iterable<PageRankValueWritable> values,
			Context context) throws IOException, InterruptedException {

		readDataFromMapper(key, values);

		computeUntilPageRanksConverge(context);

		emitTheLocallyConvergedPageRank(context);
	}

	private void readDataFromMapper(IntWritable key,
			Iterable<PageRankValueWritable> values) {
		this.blockIdWritable.set(key.get());
		for (PageRankValueWritable value : values) {
			if (value.isNodeInformation()) {
				this.previousPageRank.put(value.getVertexId(),
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
					double diff = Math.abs(currentPageRank - previousPageRank);
					maxDiff = diff > maxDiff ? diff : maxDiff;
				}
			}
			if (maxDiff < EPSILON || this.finishedNodes.size() == 0) {
				break;
			}
			isReducerConverged = false;
			this.previousPageRank = this.pageRank;
			this.pageRank = new HashMap<Integer, Double>();
			this.finishedNodes = new HashSet<Integer>();
			context.getCounter(PAGE_RANK_COUNTER.TOTAL_INNER_ITERATION)
					.increment(1);
		}
		if (!isReducerConverged) {
			context.getCounter(PAGE_RANK_COUNTER.UNCONVERGED_REDUCER)
					.increment(1);
		}
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
