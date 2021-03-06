package closenessCentrality;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

import com.google.common.collect.Iterables;

/*
 * Efficient closeness computation from "Centralities in Large Networks:
 * Algorithms and Observations". 
 * 
 * Closeness is based on the length of the average
 * shortest path between a node and all other nodes in the network
 * Assumption:view graph as undirected (Hierarchical closeness(2014) solves this limitation)
 * 
 * To compute a node's closeness, in each step, it sums #neighbors
 * the distance = step i * #neighbors, 1 step represent 1 hop from the start node  
 * 
 * Main benefit is that it estimates #neighbors by Flajolet-Martin Sketch and replaces distance by step i * # neighbors 
 * 
 * A concrete execution is as follows:
 * 1. Sum distance for each node iteratively and output <vertexId, bit[], sum>
 * bit[] records the neighbors of node 
 * 
 * 2. Avg((n-1)/sum) for each node and (larger sum(distance) means this node needs more hop to other nodes)
 * (in paper it's sum/n-1 which the lower the value of node is, the more central of node is)
 * 
 * output <nodeID, closeness>
 */

@SuppressWarnings("serial")
public class Closeness {

	private static int maxIterations = 10;
	private static int nodePosition = 0;

	private static String argPathToIndex = "";
	private static String argPathToArc = "";
	private static String argPathOut = "";
	private static int topK = 10;

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputNode = env.readTextFile(argPathToIndex);

		DataSet<Tuple2<String, Long>> nodes = inputNode
				.flatMap(new NodeReader());

		DataSet<Tuple3<Long, CountDistinctElements, Double>> initialSolutionSet = nodes
				.flatMap(new AssignBitArrayToVertices()).name(
						"Assign BitArray to Vertex");

		// Initial vertices is both workset and solutionset
		DataSet<Tuple3<Long, CountDistinctElements, Double>> initialworkingSet = initialSolutionSet;

		DataSource<String> inputArc = env.readTextFile(argPathToArc);

		/*
		 * Convert the input to edges, consisting of (source, target),
		 * (target,source)
		 */
		DataSet<Tuple2<Long, Long>> arcs = inputArc.flatMap(new ArcReader());

		DataSet<Tuple2<Long, Long>> edges = arcs.flatMap(new UndirectEdge())
				.distinct();

		// Step Function: SendMsg and BitwiseOR, Update Function:
		// FilterCovergeNodes
		DeltaIteration<Tuple3<Long, CountDistinctElements, Double>, Tuple3<Long, CountDistinctElements, Double>> deltaIteration = initialSolutionSet
				.iterateDelta(initialworkingSet, maxIterations, nodePosition);

		// Send message to neighbors and record in bit_string
		DataSet<Tuple3<Long, CountDistinctElements, Double>> neighbors = deltaIteration
				.getWorkset().join(edges).where(0).equalTo(1)
				.with(new SendingMessageToNeighbors())
				.name("Sending Message To Neighbors");

		// Collect sent neighbors by bitwiseOr bit_string
		GroupReduceOperator<Tuple3<Long, CountDistinctElements, Double>, Tuple3<Long, CountDistinctElements, Double>> bitwiseOR = neighbors
				.groupBy(0).reduceGroup(new PartialBitwiseOR())
				.name("BitwiseOR");

		// Get next WorkSet
		DataSet<Tuple3<Long, CountDistinctElements, Double>> candidateUpdates = bitwiseOR
				.join(deltaIteration.getSolutionSet()).where(0).equalTo(0)
				.with(new FilterConvergedNodes())
				.name("Filters Converged Vertices");

		// Termination: no change after iterative computation
		DataSet<Tuple3<Long, CountDistinctElements, Double>> finalSolnSet = deltaIteration
				.closeWith(candidateUpdates, candidateUpdates);

		// Create a dataset of all node ids and count them
		DataSet<Long> numVertices = edges.<Tuple1<Long>> project(0)
				.union(edges.<Tuple1<Long>> project(1)).distinct()
				.reduceGroup(new CountVertices());

		// Computation of average with a single map which reads the
		// output<vertexId,sum> of the iteration
		DataSet<Tuple2<Long, Double>> closeness = finalSolnSet
				.map(new AverageComputation())
				.withBroadcastSet(numVertices, "numVertices")
				.name("Average Computation");

		// Focus on top K closeness
		DataSet<Tuple2<Long, Double>> filterCloseness = closeness
				.filter(new TopKFilter());

		DataSet<Tuple3<Long, Long, Double>> mapCloseness = filterCloseness
				.flatMap(new TopKMapper());

		DataSet<Tuple2<Long, Double>> topCloseness = mapCloseness.groupBy(0)
				.sortGroup(2, Order.DESCENDING).first(topK)
				.<Tuple2<Long, Double>> project(1, 2);

		topCloseness.writeAsCsv(argPathOut, WriteMode.OVERWRITE);

		env.execute();
	}

	public static class TopKMapper implements
			FlatMapFunction<Tuple2<Long, Double>, Tuple3<Long, Long, Double>> {

		@Override
		public void flatMap(Tuple2<Long, Double> tuple,
				Collector<Tuple3<Long, Long, Double>> collector)
				throws Exception {
			collector.collect(new Tuple3<Long, Long, Double>((long) 1,
					tuple.f0, tuple.f1));
		}
	}

	public static class TopKFilter implements
			FilterFunction<Tuple2<Long, Double>> {

		@Override
		public boolean filter(Tuple2<Long, Double> value) throws Exception {
			// a learning param to improve efficiency
			return value.f1 > 0.3;
		}
	}

	@ConstantFields("0")
	public static final class AverageComputation
			extends
			RichMapFunction<Tuple3<Long, CountDistinctElements, Double>, Tuple2<Long, Double>> {
		private long numVertices;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			numVertices = getRuntimeContext().<Long> getBroadcastVariable(
					"numVertices").get(0);
		}

		Double closeness = 0.0;

		@Override
		public Tuple2<Long, Double> map(
				Tuple3<Long, CountDistinctElements, Double> value)
				throws Exception {
			if (value.f2 > 0) {
				// difference than the paper : closeness = value.f2 /
				// (numVertices - 1), lower value -> more central
				closeness = (numVertices - 1) / value.f2;// higher->more central
			}
			Tuple2<Long, Double> emitcloseness = new Tuple2<Long, Double>();
			emitcloseness.f0 = value.f0;
			emitcloseness.f1 = closeness;
			return emitcloseness;
		}

	}

	@ConstantFields("0")
	public static final class AssignBitArrayToVertices
			implements
			FlatMapFunction<Tuple2<String, Long>, Tuple3<Long, CountDistinctElements, Double>> {

		@Override
		public void flatMap(Tuple2<String, Long> value,
				Collector<Tuple3<Long, CountDistinctElements, Double>> out)
				throws Exception {
			CountDistinctElements counter = new CountDistinctElements();
			counter.addNode(value.f1.intValue());
			Tuple3<Long, CountDistinctElements, Double> result = new Tuple3<Long, CountDistinctElements, Double>();
			result.f0 = value.f1;
			result.f1 = counter;
			result.f2 = 0.0;
			out.collect(result);

		}
	}

	@ConstantFieldsFirst("1->1;2 -> 2")
	@ConstantFieldsSecond("0 -> 0")
	public static final class SendingMessageToNeighbors
			extends
			RichJoinFunction<Tuple3<Long, CountDistinctElements, Double>, Tuple2<Long, Long>, Tuple3<Long, CountDistinctElements, Double>> {
		public static final String ACCUM_LOCAL_ITERATIONS = "accum.local.iterations";
		private IntCounter localIterations = new IntCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(ACCUM_LOCAL_ITERATIONS,
					localIterations);
			localIterations.add(1);
		}

		@Override
		public Tuple3<Long, CountDistinctElements, Double> join(
				Tuple3<Long, CountDistinctElements, Double> vertex_workset,
				Tuple2<Long, Long> neighbors) throws Exception {

			Tuple3<Long, CountDistinctElements, Double> output = new Tuple3<Long, CountDistinctElements, Double>();
			output.f0 = neighbors.f0;
			output.f1 = vertex_workset.f1;
			output.f2 = vertex_workset.f2;
			// System.out.println("Joining at destination id -->"+neighbors.f1+" distributes to only needed vertices which becomes partial bitstring of --->"+neighbors.f0);
			return output;
		}
	}

	public static final class PartialBitwiseOR
			extends
			RichGroupReduceFunction<Tuple3<Long, CountDistinctElements, Double>, Tuple3<Long, CountDistinctElements, Double>> {

		@Override
		public void reduce(
				Iterable<Tuple3<Long, CountDistinctElements, Double>> values,
				Collector<Tuple3<Long, CountDistinctElements, Double>> out)
				throws Exception {

			Iterator<Tuple3<Long, CountDistinctElements, Double>> iterator = values
					.iterator();
			Tuple3<Long, CountDistinctElements, Double> first;

			first = iterator.next();
			Long ver = first.f0;
			Double sum = first.f2;
			CountDistinctElements counter = first.f1.copy();

			while (iterator.hasNext()) {
				counter.merge(iterator.next().f1);
			}
			// System.out.println("Getting all bitstrings of adjacent vertices of "+ver+" and its count now --->"+counter.getCount()+"   at step "+getIterationRuntimeContext().getSuperstepNumber());

			Tuple3<Long, CountDistinctElements, Double> result = new Tuple3<Long, CountDistinctElements, Double>();
			result.f0 = ver;
			result.f1 = counter;
			result.f2 = sum;
			out.collect(result);
		}

		@Override
		public void combine(
				Iterable<Tuple3<Long, CountDistinctElements, Double>> values,
				Collector<Tuple3<Long, CountDistinctElements, Double>> out)
				throws Exception {

			Iterator<Tuple3<Long, CountDistinctElements, Double>> iterator = values
					.iterator();

			Tuple3<Long, CountDistinctElements, Double> first = iterator.next();
			Long ver = first.f0;
			Double sum = first.f2;
			CountDistinctElements counter = first.f1.copy();

			while (iterator.hasNext()) {
				counter.merge(iterator.next().f1);
			}
			// System.out.println("Getting all bitstrings of adjacent vertices of "+ver+" and its count now --->"+counter.getCount()+"   at step "+getIterationRuntimeContext().getSuperstepNumber());

			Tuple3<Long, CountDistinctElements, Double> result = new Tuple3<Long, CountDistinctElements, Double>();
			result.f0 = ver;
			result.f1 = counter;
			result.f2 = sum;
			out.collect(result);
		}

	}

	public static final class FilterConvergedNodes
			extends
			RichJoinFunction<Tuple3<Long, CountDistinctElements, Double>, Tuple3<Long, CountDistinctElements, Double>, Tuple3<Long, CountDistinctElements, Double>> {
		long NumNode_before;
		long NumNode_after;
		long diff;
		Double sum;
		long iterationNumber;

		List<Long> worksetSize = new ArrayList<Long>();

		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() != 1L) {
				/*
				 * System.out.println("Current Workset size--->" +
				 * worksetSize.size() + " at iteration " +
				 * getIterationRuntimeContext().getSuperstepNumber());
				 */
				worksetSize.clear();
			}
		}

		@Override
		public Tuple3<Long, CountDistinctElements, Double> join(
				Tuple3<Long, CountDistinctElements, Double> current,
				Tuple3<Long, CountDistinctElements, Double> previous) {

			iterationNumber = getIterationRuntimeContext().getSuperstepNumber();

			CountDistinctElements prevFm = previous.f1;
			CountDistinctElements currFm = current.f1;

			NumNode_before = ((closenessCentrality.CountDistinctElements) prevFm)
					.getCount();
			(currFm).merge(prevFm);
			NumNode_after = ((closenessCentrality.CountDistinctElements) currFm)
					.getCount();

			if (NumNode_before != NumNode_after) {
				sum = (java.lang.Double) previous.f2;
				diff = NumNode_after - NumNode_before;
				sum = sum + iterationNumber * (diff);
				current.f2 = (Double) sum;
				worksetSize.add((java.lang.Long) current.f0);
				/*
				 * System.out.println("not converged " + current.f0 + "  at  " +
				 * iterationNumber);
				 */
				return current;
			} else {
				/*
				 * System.out.println("converged " + current.f0 + "  at  " +
				 * iterationNumber);
				 */
			}
			return previous;
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

	public static class CountVertices implements
			GroupReduceFunction<Tuple1<Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple1<Long>> vertices,
				Collector<Long> collector) throws Exception {
			collector.collect(new Long(Iterables.size(vertices)));
		}
	}

	public static class ProjectNodeWithName
			implements
			FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<String, Long>>, Tuple3<String, Long, Double>> {

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Double>, Tuple2<String, Long>> value,
				Collector<Tuple3<String, Long, Double>> collector)
				throws Exception {

			collector.collect(new Tuple3<String, Long, Double>(value.f1.f0,
					value.f1.f1, value.f0.f1));

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

	public static boolean parseParameters(String[] args) {

		if (args.length < 6 || args.length > 6) {
			System.err
					.println("Usage:[path to index file] [path to arc file] [output path] [Max iterations] [Node position] [topK]");
			return false;
		}

		argPathToIndex = args[0];
		argPathToArc = args[1];
		argPathOut = args[2];

		maxIterations = Integer.parseInt(args[3]);
		nodePosition = Integer.parseInt(args[4]);
		topK = Integer.parseInt(args[5]);
		return true;
	}
}
