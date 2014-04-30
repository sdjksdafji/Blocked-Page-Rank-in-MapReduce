package pojo;

public class Node {
	private int vertexId;
	private int blockId;

	public Node() {
		super();
		this.vertexId = -1;
		this.blockId = -1;
	}

	public Node(int vertexId, int blockId) {
		super();
		this.vertexId = vertexId;
		this.blockId = blockId;
	}

	public int getVertexId() {
		return vertexId;
	}

	public void setVertexId(int vertexId) {
		this.vertexId = vertexId;
	}

	public int getBlockId() {
		return blockId;
	}

	public void setBlockId(int blockId) {
		this.blockId = blockId;
	}
}
