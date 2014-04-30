package pojo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PageRankValueWritable implements Writable {

	private boolean nodeInfoOrSumInfo; // true = node info; false = sum info
	private int vertexId;
	private double value;
	private int edgeVertex;
	private int edgeBlock;

	public void readFields(DataInput in) throws IOException {
		this.nodeInfoOrSumInfo = in.readBoolean();
		this.vertexId = in.readInt();
		this.value = in.readDouble();
		this.edgeVertex = in.readInt();
		this.edgeBlock = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeBoolean(nodeInfoOrSumInfo);
		out.writeInt(vertexId);
		out.writeDouble(value);
		out.writeInt(edgeVertex);
		out.writeInt(edgeBlock);
	}

	public boolean isNodeInformation() {
		return this.nodeInfoOrSumInfo;
	}

	public boolean isSumInformation() {
		return !this.nodeInfoOrSumInfo;
	}

	public int getVertexId() {
		return vertexId;
	}

	public void setVertexId(int vertexId) {
		this.vertexId = vertexId;
	}

	public double getValue() {
		return value;
	}

	public void setValue(double value) {
		this.value = value;
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

}
