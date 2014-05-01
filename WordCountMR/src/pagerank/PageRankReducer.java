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

import pagerank.input_preprocessing.InputFormatMapper;
import pojo.PageRankValueWritable;

public class PageRankReducer extends
		Reducer<IntWritable, PageRankValueWritable, IntWritable, Text> {

	private static final double EPSILON = 1e-5;
	private static final double DAMPING_FACTOR = 0.8;
	private static boolean jacobAndGaussian = true; // true = jacob; false =
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
				this.incomingEdges.put(value.getEdgeVertex(), list);
			}
		}
	}

	private void computeUntilPageRanksConverge(Context context)
			throws IOException, InterruptedException {
		double maxDiff = 0.0;
		while (true) {
			for (PageRankValueWritable value : outcomingEdges) {
				List<PageRankValueWritable> list = this.incomingEdges.get(value
						.getVertexId());
				if (list == null) {
					System.err.println("serious error");
					this.outputText.set("serious error");
					context.write(blockIdWritable, outputText);
					break;
				}
				double currentPageRank = (1.0 - DAMPING_FACTOR)
						/ InputFormatMapper.numOfNodes;
				double previousPageRank = this.previousPageRank.get(value
						.getVertexId());
				for (PageRankValueWritable incomeVertex : list) {
					currentPageRank += DAMPING_FACTOR
							* (getLatestPageRankOfVertex(incomeVertex) / ((double) incomeVertex
									.getDegree()));
				}
				this.pageRank.put(value.getVertexId(), currentPageRank);
				this.finishedNodes.add(value.getVertexId());
				double diff = Math.abs(currentPageRank - previousPageRank);
				maxDiff = diff > maxDiff ? diff : maxDiff;
			}
			if (maxDiff < EPSILON || this.finishedNodes.size() == 0) {
				break;
			}
			this.previousPageRank = this.pageRank;
			this.pageRank = new HashMap<Integer, Double>();
			this.finishedNodes = new HashSet<Integer>();
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
