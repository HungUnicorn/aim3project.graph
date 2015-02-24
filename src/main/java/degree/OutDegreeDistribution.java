package degree;

import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import com.google.common.collect.Iterables;

//Get in-degree distribution (degree, count)
public class OutDegreeDistribution {

	private static String argPathToArc = "";
	private static String argPathOut = "";

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> input = env.readTextFile(argPathToArc);

		/* Convert the input to edges, consisting of (source, target) */
		DataSet<Tuple2<Long, Long>> edges = input.flatMap(new EdgeReader());

		/* Create a dataset of all vertex ids and count them */
		DataSet<Long> numVertices = edges.<Tuple1<Long>> project(0)
				.union(edges.<Tuple1<Long>> project(1)).distinct()
				.reduceGroup(new CountVertices());

		/* Compute the degree of every vertex */
		DataSet<Tuple2<Long, Long>> verticesWithDegree = edges
				.<Tuple1<Long>> project(0)
				// difference of out-degree and in-degree is project(0), group
				// by source
				.groupBy(0).reduceGroup(new DegreeOfVertex());

		/* Compute the degree distribution */
		DataSet<Tuple2<Long, Double>> degreeDistribution = verticesWithDegree
				.groupBy(1).reduceGroup(new DistributionElement())
				.withBroadcastSet(numVertices, "numVertices");

		degreeDistribution.writeAsCsv(argPathOut,
				FileSystem.WriteMode.OVERWRITE);

		env.execute();
	}

	public static class EdgeReader implements
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

	public static class CountVertices implements
			GroupReduceFunction<Tuple1<Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple1<Long>> vertices,
				Collector<Long> collector) throws Exception {
			collector.collect(new Long(Iterables.size(vertices)));
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

	public static class DistributionElement extends
			RichGroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

		private long numVertices;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			numVertices = getRuntimeContext().<Long> getBroadcastVariable(
					"numVertices").get(0);
		}

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> verticesWithDegree,
				Collector<Tuple2<Long, Double>> collector) throws Exception {

			Iterator<Tuple2<Long, Long>> iterator = verticesWithDegree
					.iterator();
			Long degree = iterator.next().f1;

			long count = 1L;
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}

			collector.collect(new Tuple2<Long, Double>(degree, (double) count
					/ numVertices));
		}
	}

	public static boolean parseParameters(String[] args) {

		if (args.length < 2 || args.length > 2) {
			System.err.println("Usage: [path to arc file] [output path]");
			return false;
		}

		argPathToArc = args[0];
		argPathOut = args[1];

		return true;
	}

}
