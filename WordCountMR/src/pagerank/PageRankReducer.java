package pagerank;

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
	
	private static final double DAMPING_FACTOR = 0.8;
	private static boolean jacobAndGaussian = true; // true = jacob; false = gaussian
	private Map<Integer,Double> previousPageRank = new HashMap<Integer,Double>();
	private Map<Integer,Double> pageRank = new HashMap<Integer,Double>();
	private Map<Integer,List<PageRankValueWritable>> incomingEdges = new HashMap<Integer,List<PageRankValueWritable>>();
	private List<PageRankValueWritable> outcomingEdges = new ArrayList<PageRankValueWritable>();
	private Set<Integer> finishedNodes = new HashSet<Integer>();
	
	@Override
	public void reduce(IntWritable key, Iterable<PageRankValueWritable> values, Context context){
		for(PageRankValueWritable value:values){
			if(value.isNodeInformation()){
				this.previousPageRank.put(value.getVertexId(), value.getCurrentPageRank());
				this.pageRank.put(value.getVertexId(), (1.0-DAMPING_FACTOR)/InputFormatMapper.numOfNodes);
				outcomingEdges.add(value);
			}else if(value.isSumInformation()){
				if(!this.incomingEdges.containsKey(value.getVertexId())){
					this.incomingEdges.put(value.getVertexId(), new ArrayList<PageRankValueWritable>());
				}
				List<PageRankValueWritable> list = this.incomingEdges.get(value.getVertexId());
				list.add(value);
				this.incomingEdges.put(value.getEdgeVertex(), list);
			}
		}
	}
	
	private double getLatestPageRankOfVertex(int vertexId){
		if(jacobAndGaussian){
			return previousPageRank.get(vertexId);
		}else{
			if(this.finishedNodes.contains(vertexId)){
				return pageRank.get(vertexId);
			}else{
				return previousPageRank.get(vertexId);
			}
		}
	}

}
