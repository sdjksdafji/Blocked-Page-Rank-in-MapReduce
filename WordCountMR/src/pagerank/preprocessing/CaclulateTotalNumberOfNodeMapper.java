package pagerank.preprocessing;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import driver.PAGE_RANK_COUNTER;

public class CaclulateTotalNumberOfNodeMapper extends
		Mapper<Object, Text, Text, IntWritable> {
	private final static Text outputText = new Text("Total number of node: ");
	private static IntWritable vertexId = new IntWritable(1);

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
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

			if (!FormatInputMapper.selectInputLine(rand)) {
				continue;
			}

			this.vertexId.set(src);
			context.write(outputText, vertexId);

			this.vertexId.set(dst);
			context.write(outputText, vertexId);
		}
		sc.close();
	}
}
