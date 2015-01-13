package degree;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;
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
import org.apache.flink.util.Collector;

/*TopK: 
 * 1.map:output (1, nodeId, degree)
 * 2.filter degree > avgDegree + 0.5
 * 3.reduce: firstN by Flink
 * 4.join nodeID with name
 * 
 * Gives reasonable results:
 * 1.amazon.com,25
 2.blogspot.com,23
 3.youtube.com,16
 */

public class TopKOutDegree {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputArc = env
				.readTextFile(Config.pathToSmallArcs());

		DataSource<String> inputIndex = env.readTextFile(Config
				.pathToSmallIndex());

		DataSet<Tuple2<String, Long>> nodes = inputIndex
				.flatMap(new NodeReader());

		/* Convert the input to edges, consisting of (source, target) */
		DataSet<Tuple2<Long, Long>> arcs = inputArc.flatMap(new ArcReader());

		/* Compute the degree of every vertex */
		DataSet<Tuple2<Long, Long>> verticesWithDegree = arcs.project(0)
				.types(Long.class).groupBy(0).reduceGroup(new DegreeOfVertex());

		DataSet<Tuple2<Long, Long>> highOutDegree = verticesWithDegree
				.filter(new DegreeFilter());

		DataSet<Tuple3<Long, Long, Long>> topKMapper = verticesWithDegree
				.flatMap(new TopKMapper());

		DataSet<Tuple3<Long, Long, Long>> topKReducer = topKMapper.groupBy(0)
				.sortGroup(2, Order.DESCENDING).first(10);
		// .reduceGroup(new TopKReducer());

		topKReducer.print();

		DataSet<Tuple2<String, Long>> topKwithName = topKReducer.join(nodes)
				.where(1).equalTo(1).flatMap(new ProjectNodeWithName());

		topKwithName.print();
		// degreeDistribution.writeAsText(Config.pathToOutDegreeDistritbuion(),
		// FileSystem.WriteMode.OVERWRITE);

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

	/*
	 * public static class FirstNMapper extends RichFlatMapFunction<Tuple2<Long,
	 * Long>, Tuple3<Long, Long, Long>> {
	 * 
	 * private TreeMap<Long, Long> recordMap = new TreeMap<Long, Long>(
	 * Collections.reverseOrder());
	 * 
	 * @Override public void flatMap(Tuple2<Long, Long> tuple,
	 * Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
	 * 
	 * recordMap.put(tuple.f0, tuple.f1); if (recordMap.size() > Config.topK())
	 * recordMap.remove(recordMap.firstKey()); }
	 * 
	 * @Override public void close() throws Exception { Iterator<Entry<Long,
	 * Long>> iterator = recordMap.entrySet() .iterator(); while
	 * (iterator.hasNext()) { Entry<Long, Long> thisEntry = iterator.next();
	 * Long key = thisEntry.getKey(); Long value = thisEntry.getValue();
	 * collector.collect(new Tuple3<Long, Long, Long>((long) 1, key, value)); }
	 * } }
	 */
	public static class TopKMapper implements
			FlatMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Long>> {

		private TreeMap<Long, Long> recordMap = new TreeMap<Long, Long>(
				Collections.reverseOrder());

		@Override
		public void flatMap(Tuple2<Long, Long> tuple,
				Collector<Tuple3<Long, Long, Long>> collector) throws Exception {
			collector.collect(new Tuple3<Long, Long, Long>((long) 1, tuple.f0,
					tuple.f1));
		}
	}

	/*
	 * public static class TopKReducer implements
	 * GroupReduceFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>> {
	 * 
	 * private TreeMap<Long, Long> recordMap = new TreeMap<Long, Long>(
	 * Collections.reverseOrder());
	 * 
	 * @Override public void reduce(Iterable<Tuple3<Long, Long, Long>> tuples,
	 * Collector<Tuple2<Long, Long>> collector) throws Exception {
	 * 
	 * System.out.println("----"); Iterator<Tuple3<Long, Long, Long>> iterator =
	 * tuples.iterator();
	 * 
	 * while (iterator.hasNext()) { Tuple3<Long, Long, Long> thisTuple =
	 * iterator.next(); recordMap.put(thisTuple.f1, thisTuple.f2); if
	 * (recordMap.size() > Config.topK())
	 * recordMap.remove(recordMap.firstKey()); }
	 * 
	 * Iterator<Entry<Long, Long>> mapIterator = recordMap.entrySet()
	 * .iterator(); while (mapIterator.hasNext()) { Entry<Long, Long> thisEntry
	 * = mapIterator.next(); Long key = thisEntry.getKey(); Long value =
	 * thisEntry.getValue(); collector.collect(new Tuple2<Long, Long>(key,
	 * value)); } } }
	 */

	public static class TopKReducer implements
			GroupReduceFunction<Tuple3<Long, Long, Long>, Tuple2<Long, Long>> {
		private TreeMap<Long, Long> recordMap = new TreeMap<Long, Long>(
				Collections.reverseOrder());

		@Override
		public void reduce(Iterable<Tuple3<Long, Long, Long>> tuples,
				Collector<Tuple2<Long, Long>> collector) throws Exception {

			Iterator<Tuple3<Long, Long, Long>> iterator = tuples.iterator();

			while (iterator.hasNext()) {
				Tuple3<Long, Long, Long> thisTuple = iterator.next();
				recordMap.put(thisTuple.f1, thisTuple.f2);
				if (recordMap.size() > Config.topK())
					recordMap.remove(recordMap.firstKey());
			}

			Iterator<Entry<Long, Long>> mapIterator = recordMap.entrySet()
					.iterator();
			while (mapIterator.hasNext()) {
				Entry<Long, Long> thisEntry = mapIterator.next();
				Long key = thisEntry.getKey();
				Long value = thisEntry.getValue();
				collector.collect(new Tuple2<Long, Long>(key, value));
			}
		}
	}

	public static class DegreeFilter implements
			FilterFunction<Tuple2<Long, Long>> {

		@Override
		public boolean filter(Tuple2<Long, Long> value) throws Exception {
			return value.f1 > Config.avgOutDegree();
		}
	}
}
