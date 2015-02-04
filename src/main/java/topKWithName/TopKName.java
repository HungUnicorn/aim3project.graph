package topKWithName;

import java.util.Iterator;
import java.util.regex.Pattern;

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

/*TopK: a general class to get TopK nodes with names 
 * Input should be CSV(writeAsCsv) but not txt(writeAsTxt)
 * */


public class TopKName{

	private static int topK = 10;	

	private static String argPathToIndex = "";
	private static String argPathToNodesAndValues = "";
	private static String argPathOut = "";

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputNodesAndValue = env.readTextFile(argPathToNodesAndValues);

		DataSource<String> inputIndex = env.readTextFile(argPathToIndex);

		DataSet<Tuple2<String, Long>> nodes = inputIndex
				.flatMap(new NodeReader());

		/* Convert the input to edges, consisting of (source, target) */
		DataSet<Tuple2<Long, Double>> nodesAndValue = inputNodesAndValue.flatMap(new ValueReader());		

		// Output 1, ID, degree for group by
		DataSet<Tuple3<Long, Long, Double>> topKMapper = nodesAndValue
				.flatMap(new TopKMapper());

		// Get topK
		DataSet<Tuple3<Long, Long, Double>> topKReducer = topKMapper.groupBy(0)
				.sortGroup(2, Order.DESCENDING).first(topK);

		// Node ID joins with node's name
		DataSet<Tuple2<String, Double>> topKwithName = topKReducer.join(nodes)
				.where(1).equalTo(1).flatMap(new ProjectNodeWithName());

		topKwithName.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();
	}

	public static class ValueReader implements
			FlatMapFunction<String, Tuple2<Long, Double>> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<Long, Double>> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				long node = Long.parseLong(tokens[0]);
				Double value = Double.parseDouble(tokens[1]);

				collector.collect(new Tuple2<Long, Double>(node, value));
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
			FlatMapFunction<Tuple2<Tuple3<Long, Long, Double>, Tuple2<String, Long>>, Tuple2<String, Double>> {

		@Override
		public void flatMap(
				Tuple2<Tuple3<Long, Long, Double>, Tuple2<String, Long>> value,
				Collector<Tuple2<String, Double>> collector) throws Exception {

			collector
					.collect(new Tuple2<String, Double>(value.f1.f0, value.f0.f2));

		}
	}

	public static class TopKMapper implements
			FlatMapFunction<Tuple2<Long, Double>, Tuple3<Long, Long, Double>> {	

		@Override
		public void flatMap(Tuple2<Long, Double> tuple,
				Collector<Tuple3<Long, Long, Double>> collector) throws Exception {
			collector.collect(new Tuple3<Long, Long, Double>((long) 1, tuple.f0,
					tuple.f1));
		}
	}

	public static boolean parseParameters(String[] args) {

		if (args.length < 4 || args.length > 4) {
			System.err
					.println("Usage: [path to index file] [path to node and value file] [output path] [topK]");
			return false;
		}

		argPathToIndex = args[0];
		argPathToNodesAndValues = args[1];
		argPathOut = args[2];

		topK = Integer.parseInt(args[3]);		

		return true;
	}
}


