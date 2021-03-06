package connectivity;

import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import com.google.common.collect.Iterables;

/*
 * An implementation of the connected components algorithm, using a delta
 * iteration.
 * 
 * Initially, the algorithm assigns each vertex an unique ID. In each step, a
 * vertex picks the minimum of its own ID and its neighbors' IDs, as its new ID
 * and tells its neighbors about its new ID. After the algorithm has completed,
 * all vertices in the same component will have the same ID.
 * 
 * A vertex whose component ID did not change needs not propagate its
 * information in the next step. Because of that, the algorithm is easily
 * expressible via a delta iteration. We here model the solution set as the
 * vertices with their current component ids, and the workset as the changed
 * vertices. Because we see all vertices initially as changed, the initial
 * workset and the initial solution set are identical. Also, the delta to the
 * solution set is consequently also the next workset.
 */
@SuppressWarnings("serial")
public class WeakConnectedComponents implements ProgramDescription {

	public static void main(String... args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up execution environment
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputArc = env
				.readTextFile(edgesPath);

		DataSource<String> inputIndex = env.readTextFile(verticesPath);

		DataSet<Long> vertices = inputIndex.flatMap(new NodeReader());

		/* Convert the input to edges, consisting of (source, target) */
		DataSet<Tuple2<Long, Long>> arcs = inputArc.flatMap(new ArcReader());

		// Undirected graph (arc becomes edge)
		DataSet<Tuple2<Long, Long>> edges = arcs.flatMap(new UndirectEdge())
				.distinct();

		// assign the initial components (equal to the vertex id)
		DataSet<Tuple2<Long, Long>> verticesWithInitialId = vertices
				.map(new DuplicateValue<Long>());

		// Open a delta iteration
		DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration = verticesWithInitialId
				.iterateDelta(verticesWithInitialId, maxIterations, 0);

		// Apply the step logic: join with the edges, select the minimum
		// neighbor, update if the component of the candidate is smaller
		DataSet<Tuple2<Long, Long>> changes = iteration.getWorkset()
				.join(edges).where(0).equalTo(0)
				.with(new NeighborWithComponentIDJoin()).groupBy(0)
				.aggregate(Aggregations.MIN, 1)
				.join(iteration.getSolutionSet()).where(0).equalTo(0)
				.with(new ComponentIdFilter());

		// close the delta iteration (delta and new workset are identical)
		DataSet<Tuple2<Long, Long>> vertexWithComponentID = iteration
				.closeWith(changes, changes);
		
		// Size of Component
		DataSet<Long> numComponent = vertexWithComponentID.<Tuple1<Long>>project(1).distinct().reduceGroup(new CountComponent());

		/* Compute the size of every component, emit (Component size, 1) */
		DataSet<Tuple2<Long, Long>> ComponentCount = vertexWithComponentID
				.<Tuple1<Long>>project(1).groupBy(0)
				.reduceGroup(new ComponentCount()).flatMap(new ComponentMap());

		DataSet<Tuple2<Long, Long>> ComponentDistribution = ComponentCount
				.groupBy(0).aggregate(Aggregations.SUM, 1);

		// Emit result
		if (fileOutput) {
			ComponentDistribution.writeAsCsv(outputPath, "\n", " ",
					FileSystem.WriteMode.OVERWRITE);
			//numComponent.print();
		} else {
			numComponent.print();
			ComponentDistribution.print();
		}
		
		env.execute("Weakly Connected Components");
	}

	public static class CountComponent implements
			GroupReduceFunction<Tuple1<Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple1<Long>> vertices,
				Collector<Long> collector) throws Exception {
			collector.collect(new Long(Iterables.size(vertices)));
		}
	}

	public static class ComponentMap implements
			FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public void flatMap(Tuple2<Long, Long> value,
				Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(new Tuple2<Long, Long>(value.f1, new Long(1)));
		}
	}

	public static class ComponentCount implements
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

	public static class NodeReader implements FlatMapFunction<String, Long> {

		private static final Pattern SEPARATOR = Pattern.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Long> collector)
				throws Exception {
			if (!s.startsWith("%")) {
				String[] tokens = SEPARATOR.split(s);

				// String node = tokens[0];
				long nodeIndex = Long.parseLong(tokens[1]);

				collector.collect(nodeIndex);
			}
		}
	}

	/*Function that turns a value into a 2-tuple where both fields are that
	 * value.*/
	@ConstantFields("0 -> 0,1")
	public static final class DuplicateValue<T> implements
			MapFunction<T, Tuple2<T, T>> {

		@Override
		public Tuple2<T, T> map(T vertex) {
			return new Tuple2<T, T>(vertex, vertex);
		}
	}

	/*
	 * Undirected edges by emitting for each input edge the input edges itself
	 * and an inverted version.
	 */
	public static final class UndirectEdge implements
			FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		Tuple2<Long, Long> invertedEdge = new Tuple2<Long, Long>();

		@Override
		public void flatMap(Tuple2<Long, Long> edge,
				Collector<Tuple2<Long, Long>> out) {
			invertedEdge.f0 = edge.f1;
			invertedEdge.f1 = edge.f0;
			out.collect(edge);
			out.collect(invertedEdge);
		}
	}

	/*
	 * UDF that joins a (Vertex-ID, Component-ID) pair that represents the
	 * current component that a vertex is associated with, with a
	 * (Source-Vertex-ID, Target-VertexID) edge. The function produces a
	 * (Target-vertex-ID, Component-ID) pair.
	 */
	@ConstantFieldsFirst("1 -> 0")
	@ConstantFieldsSecond("1 -> 1")
	public static final class NeighborWithComponentIDJoin
			implements
			JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> join(Tuple2<Long, Long> vertexWithComponent,
				Tuple2<Long, Long> edge) {
			return new Tuple2<Long, Long>(edge.f1, vertexWithComponent.f1);
		}
	}

	@ConstantFieldsFirst("0")
	public static final class ComponentIdFilter
			implements
			FlatJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		@Override
		public void join(Tuple2<Long, Long> candidate, Tuple2<Long, Long> old,
				Collector<Tuple2<Long, Long>> out) {
			if (candidate.f1 < old.f1) {
				out.collect(candidate);
			}
		}
	}

	@Override
	public String getDescription() {
		return "Parameters: <vertices-path> <edges-path> <result-path> <max-number-of-iterations>";
	}
	
	private static boolean fileOutput = false;
	private static String verticesPath = null;
	private static String edgesPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] programArguments) {

		if (programArguments.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (programArguments.length == 4) {
				verticesPath = programArguments[0];
				edgesPath = programArguments[1];
				outputPath = programArguments[2];
				maxIterations = Integer.parseInt(programArguments[3]);
			} else {
				System.err
						.println("Usage: ConnectedComponents <vertices path> <edges path> <result path> <max number of iterations>");
				return false;
			}
		} else {
			System.out
					.println("Executing Connected Components example with default parameters and built-in default data.");
			System.out
					.println("  Provide parameters to read input data from files.");
			System.out
					.println("  See the documentation for the correct format of input files.");
			System.out
					.println("  Usage: ConnectedComponents <vertices path> <edges path> <result path> <max number of iterations>");
		}
		return true;
	}

}
