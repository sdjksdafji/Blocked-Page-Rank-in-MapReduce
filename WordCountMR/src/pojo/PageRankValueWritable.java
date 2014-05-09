package pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PageRankValueWritable implements Writable, Cloneable {

	private boolean nodeInfoOrSumInfo; // true = node info; false = sum info
	private int vertexId;
	private double currentPageRank;
	private int edgeVertex;
	private int edgeBlock;
	private int degree;

	public void readFields(DataInput in) throws IOException {
		this.nodeInfoOrSumInfo = in.readBoolean();
		this.vertexId = in.readInt();
		this.currentPageRank = in.readDouble();
		this.edgeVertex = in.readInt();
		this.edgeBlock = in.readInt();
		this.degree = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(nodeInfoOrSumInfo);
		out.writeInt(vertexId);
		out.writeDouble(currentPageRank);
		out.writeInt(edgeVertex);
		out.writeInt(edgeBlock);
		out.writeInt(degree);
	}

	public boolean isSelfLooped() {
		// return this.vertexId == this.edgeVertex;
		return this.edgeVertex < 0;
	}

	public boolean isNodeInformation() {
		return this.nodeInfoOrSumInfo;
	}

	public boolean isSumInformation() {
		return !this.nodeInfoOrSumInfo;
	}

	public void setNodeInformation() {
		this.nodeInfoOrSumInfo = true;
	}

	public void setSumInformation() {
		this.nodeInfoOrSumInfo = false;
	}

	public int getVertexId() {
		return vertexId;
	}

	public void setVertexId(int vertexId) {
		this.vertexId = vertexId;
	}

	public double getCurrentPageRank() {
		return currentPageRank;
	}

	public void setCurrentPageRank(double currentPageRank) {
		this.currentPageRank = currentPageRank;
	}

	public int getEdgeVertex() {
		return edgeVertex;
	}

	public void setEdgeVertex(int edgeVertex) {
		this.edgeVertex = edgeVertex;
	}

	public int getEdgeBlock() {
		return edgeBlock;
	}

	public void setEdgeBlock(int edgeBlock) {
		this.edgeBlock = edgeBlock;
	}

	public int getDegree() {
		return degree;
	}

	public void setDegree(int degree) {
		this.degree = degree;
	}

	@Override
	public String toString() {
		return "PageRankValueWritable [nodeInfoOrSumInfo=" + nodeInfoOrSumInfo
				+ ", vertexId=" + vertexId + ", currentPageRank="
				+ currentPageRank + ", edgeVertex=" + edgeVertex
				+ ", edgeBlock=" + edgeBlock + ", degree=" + degree + "]";
	}

	@Override
	public PageRankValueWritable clone() {
		PageRankValueWritable returnVal = new PageRankValueWritable();
		if (this.isNodeInformation()) {
			returnVal.setNodeInformation();
		} else if (this.isSumInformation()) {
			returnVal.setSumInformation();
		}
		returnVal.setVertexId(this.getVertexId());
		returnVal.setCurrentPageRank(this.getCurrentPageRank());
		returnVal.setEdgeBlock(this.getEdgeBlock());
		returnVal.setEdgeVertex(this.getEdgeVertex());
		returnVal.setDegree(this.getDegree());
		return returnVal;
	}

}
