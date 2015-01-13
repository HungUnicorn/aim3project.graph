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
import org.apache.flink.util.Collector;

import com.google.common.collect.Iterables;

public class AvgOutDegree {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> input = env.readTextFile(Config.pathToSmallArcs());

		/* Convert the input to edges, consisting of (source, target) */
		DataSet<Tuple2<Long, Long>> edges = input
				.flatMap(new ArcReader());

		DataSet<Long> numVertices = edges.project(0)
				.types(Long.class)
				.union(edges.project(1).types(Long.class))// target
				.distinct().reduceGroup(new CountVertices());		

		DataSet<Tuple2<Long, Long>> verticesWithDegree = edges
				.project(0).types(Long.class)// source node
				.groupBy(0).reduceGroup(new DegreeOfVertex());

		// Emit (Degree, counts)
		DataSet<Tuple2<Long, Double>> degreeDistribution = verticesWithDegree
				.groupBy(1).reduceGroup(new AmountDegreeDistribution());

		// Emit (1, SumDegree)
		DataSet<Tuple2<Long, Double>> sumDegree = degreeDistribution
				.flatMap(new SumDegreeMapper());

		// Average(friend ratio)
		DataSet<Double> avgDegree = sumDegree.groupBy(0)
				.reduceGroup(new AvgRatioReducer())
				.withBroadcastSet(numVertices, "numVertices");

		avgDegree.print();
		
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

	public static class CountVertices implements
			GroupReduceFunction<Tuple1<Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple1<Long>> edges,
				Collector<Long> collector) throws Exception {
			collector.collect(new Long(Iterables.size(edges)));
		}
	}
	

	public static class AmountDegreeDistribution implements
			GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> verticesWithDegree,
				Collector<Tuple2<Long, Double>> collector) throws Exception {

			Iterator<Tuple2<Long, Long>> iterator = verticesWithDegree
					.iterator();
			Long degree = iterator.next().f1;

			double count = 1L;
			while (iterator.hasNext()) {
				iterator.next();
				count++;
			}

			collector.collect(new Tuple2<Long, Double>(degree, count));
		}
	}

	// Degree * times
	public static class SumDegreeMapper implements
			FlatMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {
		@Override
		public void flatMap(Tuple2<Long, Double> value,
				Collector<Tuple2<Long, Double>> out) throws Exception {
			out.collect(new Tuple2<Long, Double>((long) 1, value.f0 * value.f1));

		}
	}

	public static class AvgRatioReducer extends
			RichGroupReduceFunction<Tuple2<Long, Double>, Double> {

		private long numVertices;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			numVertices = getRuntimeContext().<Long> getBroadcastVariable(
					"numVertices").get(0);
		}

		@Override
		public void reduce(Iterable<Tuple2<Long, Double>> verticesWithDegree,
				Collector<Double> collector) throws Exception {

			Iterator<Tuple2<Long, Double>> iterator = verticesWithDegree
					.iterator();

			double sum = 0;
			while (iterator.hasNext()) {
				double degree = iterator.next().f1;
				sum += degree;
			}

			collector.collect(sum / numVertices);
		}
	}
}
