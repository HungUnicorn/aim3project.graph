package closenessCentrality;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.google.common.collect.Iterables;

/**
 * Efficient closeness computation from Centralities in Large Networks:
 * Algorithms and Observations Closeness is based on the length of the average
 * shortest path between a node and all other nodes in the network
 * 
 * 1. Sum distance for each vertex iteratively and output <vertexId, bit[], sum>
 * 2. Avg((n-1)/sum) for each vertex and output <vertexId, closeness> <br>
 * </ul>
 * 
 * 
 */

@SuppressWarnings("serial")
public class Closeness {

	public static void main(String[] args) throws Exception {

		/*
		 * if (!parseParameters(args)) { return; }
		 */
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSource<String> inputNode = env.readTextFile(Config
				.pathToSmallIndex());

		DataSet<Tuple2<String, Long>> nodes = inputNode
				.flatMap(new NodeReader());

		DataSet<Tuple3<Long, FMCounter, Double>> initialSolutionSet = nodes
				.flatMap(new AssignBitArrayToVertices()).name(
						"Assign BitArray to Vertex");

		// Initial vertices is both workset and solutionset
		DataSet<Tuple3<Long, FMCounter, Double>> initialworkingSet = initialSolutionSet;

		DataSource<String> inputArc = env
				.readTextFile(Config.pathToSmallArcs());

		/* Convert the input to edges, consisting of (source, target) */
		DataSet<Tuple2<Long, Long>> arcs = inputArc.flatMap(new ArcReader());

		int maxIterations = 3;
		int keyPosition = 0;

		DeltaIteration<Tuple3<Long, FMCounter, Double>, Tuple3<Long, FMCounter, Double>> deltaIteration = initialSolutionSet
				.iterateDelta(initialworkingSet, maxIterations, keyPosition);
		// deltaIteration.name("Effective Closeness Iteration");

		DataSet<Tuple3<Long, FMCounter, Double>> candidateUpdates = deltaIteration
				.getWorkset().join(arcs).where(0).equalTo(1)
				.with(new SendingMessageToNeighbors())
				.name("Sending Message To Neighbors").groupBy(0)
				.reduceGroup(new PartialBitwiseOR()).name("BitwiseOR")
				.join(deltaIteration.getSolutionSet()).where(0).equalTo(0)
				.with(new FilterConvergedNodes())
				.name("Filters Converged Vertices");

		// Termination: no change after iterative computation
		DataSet<Tuple3<Long, FMCounter, Double>> finalSolnSet = deltaIteration
				.closeWith(candidateUpdates, candidateUpdates);

		/* Create a dataset of all vertex ids and count them */
		DataSet<Long> numVertices = arcs.project(0).types(Long.class)
				.union(arcs.project(1).types(Long.class)).distinct()
				.reduceGroup(new CountVertices());

		// Computation of average with a single map which reads the
		// output<vertexId,sum> of the iteration
		DataSet<Tuple2<Long, Double>> closeness = finalSolnSet
				.map(new AverageComputation())
				.withBroadcastSet(numVertices, "numVertices")
				.name("Average Computation");

		closeness.print();
		/*
		 * closeness.writeAsCsv(Config.outputPath(), CentralityUtil.NEWLINE,
		 * CentralityUtil.TAB_DELIM, WriteMode.OVERWRITE);
		 */

		JobExecutionResult job = env.execute();

		System.out
				.println("Total number of iterations-->"
						+ ((job.getIntCounterResult(SendingMessageToNeighbors.ACCUM_LOCAL_ITERATIONS) / 4 // #subTasks
						) + 1));
		System.out.println("RunTime-->" + (job.getNetRuntime()));
	}

	@ConstantFields("0")
	public static final class AverageComputation
			extends
			RichMapFunction<Tuple3<Long, FMCounter, Double>, Tuple2<Long, Double>> {
		private long numVertices;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			numVertices = getRuntimeContext().<Long> getBroadcastVariable(
					"numVertices").get(0);
		}

		Double closeness = 0.0;

		@Override
		public Tuple2<Long, Double> map(Tuple3<Long, FMCounter, Double> value)
				throws Exception {
			if (value.f2 > 0) {
				closeness = value.f2 / (numVertices - 1);
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
			FlatMapFunction<Tuple2<String, Long>, Tuple3<Long, FMCounter, Double>> {

		@Override
		public void flatMap(Tuple2<String, Long> value,
				Collector<Tuple3<Long, FMCounter, Double>> out)
				throws Exception {
			FMCounter counter = new FMCounter();
			counter.addNode(value.f1.intValue());
			Tuple3<Long, FMCounter, Double> result = new Tuple3<Long, FMCounter, Double>();
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
			RichJoinFunction<Tuple3<Long, FMCounter, Double>, Tuple2<Long, Long>, Tuple3<Long, FMCounter, Double>> {
		public static final String ACCUM_LOCAL_ITERATIONS = "accum.local.iterations";
		private IntCounter localIterations = new IntCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(ACCUM_LOCAL_ITERATIONS,
					localIterations);
			localIterations.add(1);
		}

		@Override
		public Tuple3<Long, FMCounter, Double> join(
				Tuple3<Long, FMCounter, Double> vertex_workset,
				Tuple2<Long, Long> neighbors) throws Exception {

			Tuple3<Long, FMCounter, Double> output = new Tuple3<Long, FMCounter, Double>();
			output.f0 = neighbors.f0;
			output.f1 = vertex_workset.f1;
			output.f2 = vertex_workset.f2;
			// System.out.println("Joining at des id -->"+neighbors.f1+" distributes to only needed vertices which becomes partial bitstring of --->"+neighbors.f0);
			return output;
		}
	}

	public static final class PartialBitwiseOR
			extends
			RichGroupReduceFunction<Tuple3<Long, FMCounter, Double>, Tuple3<Long, FMCounter, Double>> {

		@Override
		public void reduce(Iterable<Tuple3<Long, FMCounter, Double>> values,
				Collector<Tuple3<Long, FMCounter, Double>> out)
				throws Exception {

			Iterator<Tuple3<Long, FMCounter, Double>> iterator = values
					.iterator();
			Tuple3<Long, FMCounter, Double> first;

			first = iterator.next();
			Long ver = first.f0;
			Double sum = first.f2;
			FMCounter counter = first.f1.copy();

			while (iterator.hasNext()) {
				counter.merge(iterator.next().f1);
			}
			// System.out.println("Getting all bitstrings of adjacent vertices of "+ver+" and its count now --->"+counter.getCount()+"   at step "+getIterationRuntimeContext().getSuperstepNumber());

			Tuple3<Long, FMCounter, Double> result = new Tuple3<Long, FMCounter, Double>();
			result.f0 = ver;
			result.f1 = counter;
			result.f2 = sum;
			out.collect(result);
		}

		@Override
		public void combine(Iterable<Tuple3<Long, FMCounter, Double>> values,
				Collector<Tuple3<Long, FMCounter, Double>> out)
				throws Exception {

			Iterator<Tuple3<Long, FMCounter, Double>> iterator = values
					.iterator();

			Tuple3<Long, FMCounter, Double> first = iterator.next();
			Long ver = first.f0;
			Double sum = first.f2;
			FMCounter counter = first.f1.copy();

			while (iterator.hasNext()) {
				counter.merge(iterator.next().f1);
			}
			// System.out.println("Getting all bitstrings of adjacent vertices of "+ver+" and its count now --->"+counter.getCount()+"   at step "+getIterationRuntimeContext().getSuperstepNumber());

			Tuple3<Long, FMCounter, Double> result = new Tuple3<Long, FMCounter, Double>();
			result.f0 = ver;
			result.f1 = counter;
			result.f2 = sum;
			out.collect(result);
		}

	}

	public static final class FilterConvergedNodes
			extends
			RichJoinFunction<Tuple3<Long, FMCounter, Double>, Tuple3<Long, FMCounter, Double>, Tuple3<Long, FMCounter, Double>> {
		long NumNode_before;
		long NumNode_after;
		long diff;
		Double sum;
		long iterationNumber;

		List<Long> worksetSize = new ArrayList<Long>();

		@Override
		public void open(Configuration parameters) throws Exception {
			if (getIterationRuntimeContext().getSuperstepNumber() != 1L) {
				System.out.println("Current Workset size--->"
						+ worksetSize.size() + " at iteration "
						+ getIterationRuntimeContext().getSuperstepNumber());
				worksetSize.clear();
			}
		}

		public void join(Tuple3<Long, FMCounter, Double> current,
				Tuple3<Long, FMCounter, Double> previous,
				Collector<Tuple3<Long, FMCounter, Double>> out) {

			iterationNumber = getIterationRuntimeContext().getSuperstepNumber();

			FMCounter prevFm = previous.f1;
			FMCounter currFm = current.f1;

			NumNode_before = prevFm.getCount();
			currFm.merge(prevFm);
			NumNode_after = currFm.getCount();
			if (NumNode_before != NumNode_after) {
				sum = previous.f2;
				diff = NumNode_after - NumNode_before;
				sum = sum + iterationNumber * (diff);
				current.f2 = sum;
				out.collect(current);
				worksetSize.add(current.f0);
				System.out.println("not converged " + current.f0 + "  at  "
						+ iterationNumber);
			} else {
				System.out.println("converged " + current.f0 + "  at  "
						+ iterationNumber);
			}
		}

		@Override
		public Tuple3<Long, FMCounter, Double> join(
				Tuple3<Long, FMCounter, Double> current,
				Tuple3<Long, FMCounter, Double> previous) throws Exception {

			return null;
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

	/*@Parameters [Degree of parallelism],[vertices input-path],[edge input-path],
	 *             [out-put],[Max-Num of iterations],[Number of vertices]*/
	
	private static boolean fileOutput = false;
	private static String textPath;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {
		String fieldDelimiter = CentralityUtil.TAB_DELIM;
		int keyPosition = 0;

		if (args.length < 6) {
			System.err
					.println("Usage:[Degree of parallelism],[vertices input-path],"
							+ "[edge input-path],[out-put],[Max-Num of iterations],[Number of vertices]"
							+ ",[Delimiter]");
		}

		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0])
				: 1);
		final String verticesInput = (args.length > 1 ? args[1] : "");
		final String edgeInput = (args.length > 2 ? args[2] : "");
		final String output = (args.length > 3 ? args[3] : "");
		final int maxIterations = (args.length > 4 ? Integer.parseInt(args[4])
				: 5);
		final String numVertices = (args.length > 5 ? (args[5])
				: CentralityUtil.ZERO);

		if (args.length > 6) {
			fieldDelimiter = (args[6]);
		}

		char delim = CentralityUtil.checkDelim(fieldDelimiter);
		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err
						.println("Usage: Closeness <input path> <output path>");
				return false;
			}
		} else {
			System.out
					.println(" Provide parameters to read input data from a file.");
			System.out.println("Usage: Closeness <input path> <output path>");
			return false;
		}
		return true;
	}

	private static DataSource<String> getTextDataSource(ExecutionEnvironment env) {
		// read the text file from given input path
		return env.readTextFile(textPath);
	}

}