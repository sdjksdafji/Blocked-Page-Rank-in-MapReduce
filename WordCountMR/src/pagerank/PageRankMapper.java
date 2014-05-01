package pagerank;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pojo.PageRankValueWritable;

public class PageRankMapper extends
		Mapper<Object, Text, IntWritable, PageRankValueWritable> {
	private IntWritable blockIdWritable = new IntWritable();
	private PageRankValueWritable pageRankValueWritable = new PageRankValueWritable();

	private int blockId;
	private int vertexId;
	private double currentPageRank;
	private int dstBlockId;
	private int dstVertexId;
	private int vertexDegree;

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Scanner sc = new Scanner(value.toString());
		while (sc.hasNextInt()) {
			if (readInputFromScanner(sc) == false) {
				break;
			}
			emitNodeInfo(context);
			emitSumInfo(context);
		}
		sc.close();
	}

	private boolean readInputFromScanner(Scanner sc) {
		this.blockId = sc.nextInt();
		if (!sc.hasNextInt()) {
			return false;
		}
		this.vertexId = sc.nextInt();
		if (!sc.hasNextDouble()) {
			return false;
		}
		this.currentPageRank = sc.nextDouble();
		if (!sc.hasNextInt()) {
			return false;
		}
		this.dstBlockId = sc.nextInt();
		if (!sc.hasNextInt()) {
			return false;
		}
		this.dstVertexId = sc.nextInt();
		
		if(!sc.hasNextInt()){
			return false;
		}
		this.vertexDegree = sc.nextInt();
		return true;
	}

	private void emitNodeInfo(Context context) throws IOException,
			InterruptedException {
		this.blockIdWritable.set(blockId);

		this.pageRankValueWritable.setNodeInformation();
		this.pageRankValueWritable.setVertexId(vertexId);
		this.pageRankValueWritable.setCurrentPageRank(currentPageRank);
		this.pageRankValueWritable.setEdgeBlock(dstBlockId);
		this.pageRankValueWritable.setEdgeVertex(dstVertexId);
		this.pageRankValueWritable.setDegree(vertexDegree);

		context.write(blockIdWritable, pageRankValueWritable);
	}

	private void emitSumInfo(Context context) throws IOException,
			InterruptedException {
		this.blockIdWritable.set(dstBlockId);

		this.pageRankValueWritable.setSumInformation();
		this.pageRankValueWritable.setVertexId(dstVertexId);
		this.pageRankValueWritable.setCurrentPageRank(currentPageRank);
		this.pageRankValueWritable.setEdgeBlock(blockId);
		this.pageRankValueWritable.setEdgeVertex(vertexId);
		this.pageRankValueWritable.setDegree(vertexDegree);

		context.write(blockIdWritable, pageRankValueWritable);
	}
	
}
