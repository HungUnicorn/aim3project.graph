package degree;

import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

/* Get Top K out-degree: 
 * 1.map:output (1, nodeId, degree)
 * 2.filter degree, ex. degree > avgDegree
 * 3.reduce: firstN by Flink
 * 4.join nodeID with name
 */

public class TopKOutDegree {

	private static int topK = 10;
	private static int degreeFilter = 0;

	private static String argPathToIndex = "";
	private static String argPathToArc = "";
	private static String argPathOut = "";

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputArc = env.readTextFile(argPathToArc);

		DataSource<String> inputIndex = env.readTextFile(argPathToIndex);

		DataSet<Tuple2<String, Long>> nodes = inputIndex
				.flatMap(new NodeReader());

		/* Convert the input to edges, consisting of (source, target) */
		DataSet<Tuple2<Long, Long>> arcs = inputArc.flatMap(new ArcReader());

		/* Compute the degree of every vertex */
		DataSet<Tuple2<Long, Long>> verticesWithDegree = arcs
				.<Tuple1<Long>> project(0).groupBy(0)
				.reduceGroup(new DegreeOfVertex());

		// Focus on the nodes' degree higher than average degree
		DataSet<Tuple2<Long, Long>> highOutDegree = verticesWithDegree
				.filter(new DegreeFilter());

		// Output 1, ID, degree for group by
		DataSet<Tuple3<Long, Long, Long>> topKMapper = highOutDegree
				.flatMap(new TopKMapper());

		// Get topK
		DataSet<Tuple3<Long, Long, Long>> topKReducer = topKMapper.groupBy(0)
				.sortGroup(2, Order.DESCENDING).first(topK);

		// Node ID joins with node's name
		DataSet<Tuple2<String, Long>> topKwithName = topKReducer.join(nodes)
				.where(1).equalTo(1).flatMap(new ProjectNodeWithName());

		topKwithName.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();
	}

	public static class ArcReader implements
			FlatMapFunction<String, Tuple2<Long, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<Long, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				long source = Long.parseLong(tokens[0]);
				long target = Long.parseLong(tokens[1]);

				collector.collect(new Tuple2<Long, Long>(source, target));
			}
		}
	}

	public static class NodeReader implements
			FlatMapFunction<String, Tuple2<String, Long>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<String, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				String node = tokens[0];
				long nodeIndex = Long.parseLong(tokens[1]);

				collector.collect(new Tuple2<String, Long>(node, nodeIndex));
			}
		}
	}

	public static class DegreeOfVertex implements
			GroupReduceFunction<Tuple1<Long>, Tuple2<Long, Long>> {
		@Override
		public void reduce(Iterable<Tuple1<Long>> tuples,
				Collector<Tuple2<Long, Long>> collector) throws Exception {

			Iterator<Tuple1<Long>> iterator = tuples.iterator();
			Long vertexId = iterator.next().f0;

			long count = 1L;
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}

			collector.collect(new Tuple2<Long, Long>(vertexId, count));
		}
	}

	public static class ProjectNodeWithName
			implements
			FlatMapFunction<Tuple2<Tuple3<Long, Long, Long>, Tuple2<String, Long>>, Tuple2<String, Long>> {

		@Override
		public void flatMap(
				Tuple2<Tuple3<Long, Long, Long>, Tuple2<String, Long>> value,
				Collector<Tuple2<String, Long>> collector) throws Exception {

			collector
					.collect(new Tuple2<String, Long>(value.f1.f0, value.f0.f2));

		}
	}

	public static class TopKMapper implements
			FlatMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>> {

		@Override
		public void flatMap(Tuple2<Long, Long> tuple,
				Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
			collector.collect(new Tuple3<Long, Long, Long>((long) 1, tuple.f0,
					tuple.f1));
		}
	}

	public static class DegreeFilter implements
			FilterFunction<Tuple2<Long, Long>> {

		@Override
		public boolean filter(Tuple2<Long, Long> value) throws Exception {
			return value.f1 > degreeFilter;
		}
	}

	public static boolean parseParameters(String[] args) {

		if (args.length < 5 || args.length > 5) {
			System.err
					.println("Usage: [path to index file] [path to arc file] [output path] [topK] [degreeFilter]");
			return false;
		}

		argPathToIndex = args[0];
		argPathToArc = args[1];
		argPathOut = args[2];

		topK = Integer.parseInt(args[3]);
		degreeFilter = Integer.parseInt(args[4]);

		return true;
	}
}
