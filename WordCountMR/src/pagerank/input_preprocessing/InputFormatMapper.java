package pagerank.input_preprocessing;

import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import pojo.PageRankValueWritable;

/* format input to form:
 *	 block index of v + v + initial page rank of v + all edges start from v (in form block of destination + destination vertex) + degree of v
 */
public class InputFormatMapper extends
		Mapper<Object, Text, IntWritable, PageRankValueWritable> {
	private static final double REJECT_MIN = 0.895 * 0.99;
	private static final double REJECT_LIMIT = REJECT_MIN + 0.01;
	public static final int numOfNodes = 5;// 7524402;
	private static final double INIT_PR = 1.0 / numOfNodes;
	private IntWritable blockIdWritable = new IntWritable();
	private PageRankValueWritable pageRankValueWritable = new PageRankValueWritable();

	@Override
	public void map(Object key, Text value, Context contex) throws IOException,
			InterruptedException {
		Scanner sc = new Scanner(value.toString());
		while (sc.hasNextInt()) {
			int src, dst;
			float rand;
			src = sc.nextInt();
			if (!sc.hasNextInt()) {
				break;
			}
			dst = sc.nextInt();
			if (!sc.hasNextFloat()) {
				break;
			}
			rand = sc.nextFloat();

			if (!selectInputLine(rand)) {
				continue;
			}

			formatDataAndEmit(src, dst, contex);
		}
		sc.close();
	}

	private int getBlockIdOfVertexId(int n) {
		int[] boundary = new int[] { 10328, 20373, 30629, 40645, 50462, 60841,
				70591, 80118, 90497, 100501, 110567, 120945, 130999, 140574,
				150953, 161332, 171154, 181514, 191625, 202004, 212383, 222762,
				232593, 242878, 252938, 263149, 273210, 283473, 293255, 303043,
				313370, 323522, 333883, 343663, 353645, 363929, 374236, 384554,
				394929, 404712, 414617, 424747, 434707, 444489, 454285, 464398,
				474196, 484050, 493968, 503752, 514131, 524510, 534709, 545088,
				555467, 565846, 576225, 586604, 596585, 606367, 616148, 626448,
				636240, 646022, 655804, 665666, 675448, 685230 };

		int start = 0;
		int end = boundary.length - 1;
		if (n >= 0 && n < 10328)
			return 0; // first block: block 0
		if (n >= 685230 || n < 0)
			return -1;
		while (start < end) {
			if (Math.abs(start - end) <= 1) {
				return start + 1;
			}

			int mid = (start + end) / 2;

			if (n == boundary[mid]) {
				return mid + 1;
			} else if (boundary[mid] < n) {
				start = mid;
			} else {
				end = mid;
			}

		}
		return -1;
	}

	public static boolean selectInputLine(double x) {
		return (((x >= REJECT_MIN) && (x < REJECT_LIMIT)) ? false : true);
	}

	private void formatDataAndEmit(int src, int dst, Context contex)
			throws InterruptedException, IOException {
		int srcBlock = getBlockIdOfVertexId(src);
		int dstBlock = getBlockIdOfVertexId(dst);

		this.blockIdWritable.set(srcBlock);

		this.pageRankValueWritable.setNodeInformation();
		this.pageRankValueWritable.setVertexId(src);
		this.pageRankValueWritable.setCurrentPageRank(INIT_PR);
		this.pageRankValueWritable.setEdgeBlock(dstBlock);
		this.pageRankValueWritable.setEdgeVertex(dst);

		contex.write(blockIdWritable, pageRankValueWritable);
		
		// write out a self looped edge in case a node do not have out-going edge
		
		this.blockIdWritable.set(dstBlock);
		
		this.pageRankValueWritable.setNodeInformation();
		this.pageRankValueWritable.setVertexId(dst);
		this.pageRankValueWritable.setCurrentPageRank(INIT_PR);
		this.pageRankValueWritable.setEdgeBlock(dstBlock);
		this.pageRankValueWritable.setEdgeVertex(dst);

		contex.write(blockIdWritable, pageRankValueWritable);
	}
}
