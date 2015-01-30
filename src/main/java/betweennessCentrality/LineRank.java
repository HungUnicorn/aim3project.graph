package betweennessCentrality;

import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.DoubleSumAggregator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsFirst;
import org.apache.flink.api.java.functions.FunctionAnnotation.ConstantFieldsSecond;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.util.Collector;

import betweennessCentrality.SourceIncidence.ArcReader;

import com.google.common.collect.Iterables;

/* Generates the importance of arcs and determine which nodes are more important based on the importance of arcs those node have.
Computing the importance of arcs are similar to PageRank
1. Generate Line Graph
2. Compute eigenvector of line Graph
3. Collect the importance of node by the importance of edges connects to that node
*/

public class LineRank {
	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {
		/*
		 * if (args.length < 4) { System.err .println(
		 * "Usage: LineRank <DOP> <edgeInputPath> <outputPath> <numIterations> <numOfEdges> <delimiter>"
		 * ); return; }
		 * 
		 * final int dop = Integer.parseInt(args[0]);
		 * 
		 * final String srcIncidencePath = args[1]; final String targetInputPath
		 * = args[2]; final String outputPath = args[3]; final int maxIterations
		 * = Integer.parseInt(args[4]); final double numEdges = (args.length > 5
		 * ? (Integer.parseInt(args[5])) : 1); Double c = 0.85; String
		 * fieldDelimiter = CentralityUtil.TAB_DELIM; if (args.length > 6) {
		 * fieldDelimiter = (args[6]); } char delim =
		 * CentralityUtil.checkDelim(fieldDelimiter);
		 */
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		// env.setDegreeOfParallelism(dop);

		// Read inArcs and outArcs
		DataSource<String> inputInArc = env.readTextFile(Config.inArcs());

		DataSet<Tuple2<Long, Long>> srcIncMat = inputInArc
				.flatMap(new IncidenceArcReader());

		DataSource<String> inputOutArc = env.readTextFile(Config.outArcs());

		DataSet<Tuple2<Long, Long>> tarIncMat = inputOutArc
				.flatMap(new IncidenceArcReader());

		/****************************************************
		 * Computing normalization factors
		 ****************************************************/
		// d1 <- S(G)T*1 which results in d1 of dimensions vxm X mxv => vx1
		DataSet<Tuple2<Long, Double>> d1 = srcIncMat.groupBy(1)
				.reduceGroup(new MatrixToVector()).name("D1");
		// d2 <- T(G)d1; which results in d2 of dimensions mxv X vx1 => mx1
		DataSet<Tuple2<Long, Double>> d2 = d1.join(tarIncMat).where(0)
				.equalTo(1).with(new MatrixVectorMul()).name("D2");

		// d <- 1./d2 an element-wise divide operation, so no dimension change=>
		// mx1
		DataSet<Tuple2<Long, Double>> d = d2.map(
				new MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Double> map(Tuple2<Long, Double> value)
							throws Exception {
						Tuple2<Long, Double> elementwiseInverse = new Tuple2<Long, Double>();
						elementwiseInverse.f0 = value.f0;
						elementwiseInverse.f1 = 1 / value.f1;
						// System.out.println("d-->"+elementwiseInverse.f0+"  "+elementwiseInverse.f1);
						return elementwiseInverse;
					}

				}).name("D");

		// Count arcs
		DataSource<String> inputArc = env
				.readTextFile(Config.pathToSmallArcs());

		DataSet<Tuple2<Long, Long>> arcs = inputArc.flatMap(new ArcReader());

		DataSet<Long> numArc = arcs.distinct().reduceGroup(new CountArcs());

		// Initialize random vector with mx1
		DataSet<Tuple2<Long, Double>> edgeScores = d
				.map(new InitializeRandomVector()).name("V")
				.withBroadcastSet(numArc, "numArc");

		/********************************************************
		 * Power Method for computing the stationary probabilities of edges
		 * using Bulk Iteration
		 ********************************************************/

		int maxIterations = Config.maxIterations();

		DoubleSumAggregator agg = new DoubleSumAggregator();

		IterativeDataSet<Tuple2<Long, Double>> iteration = edgeScores
				.iterate(maxIterations)
				.registerAggregationConvergenceCriterion(
						L1_NormDiff.AGGREGATOR_NAME, agg,
						new L1_NormConvergence())
				.name("EdgeScoreVector_BulkIteration");

		DataSet<Tuple2<Long, Double>> new_edgeScores = iteration
				.join(d)
				.where(0)
				.equalTo(0)
				.with(new V1_HadamardProduct())
				.name("V1")
				// Hadamard product of v1 <- d * v
				.join(srcIncMat)
				.where(0)
				.equalTo(0)
				.with(new V2_SrcIncWithV1())
				.name("V2")
				// S(G) i.e. mxv becomes => vxm and then vxm X mx1 => vx1
				.groupBy(0)
				.aggregate(Aggregations.SUM, 1)
				// Sum followed by product in matrix vector multiplication would
				// result vx1
				// .map(new PrintMapper("v2"))
				.join(tarIncMat).where(0)
				.equalTo(1)
				// c is damping factor
				.with(new V3_TarIncWithV2())
				.withBroadcastSet(numArc, "numArc")
				.name("V3")
				// .map(new DampingMapper(c, numEdges)) // mxv X vx1 => mx1
				// .map(new PrintMapper("v3"))
				.join(iteration).where(0).equalTo(0).with(new L1_NormDiff())
				.name("L1_NORM");
		DataSet<Tuple2<Long, Double>> convergedVector = iteration
				.closeWith(new_edgeScores);

		/********************************************************
		 * Aggregating edge scores for each vertex to get betweenness score
		 ********************************************************/
		// (S(G) + T(G))^T * V => S(G)^T *V + T(G)^T *V

		DataSet<Tuple2<Long, Double>> partialAggregation_1 = srcIncMat
				.join(convergedVector).where(0).equalTo(0)
				.with(new AddSrcWithTar()).name("Par1").groupBy(0)
				.aggregate(Aggregations.SUM, 1);
		DataSet<Tuple2<Long, Double>> partialAggregation_2 = tarIncMat
				.join(convergedVector).where(0).equalTo(0)
				.with(new AddSrcWithTar()).name("Part2").groupBy(0)
				.aggregate(Aggregations.SUM, 1);
		DataSet<Tuple2<Long, Double>> lineRank = partialAggregation_1
				.join(partialAggregation_2).where(0).equalTo(0)
				.with(new EdgeScoreAggregation()).name("EdgeScore_Agg");

		lineRank.writeAsCsv(Config.outputPath(), WriteMode.OVERWRITE).name(
				"Writing Results");

		JobExecutionResult job = env.execute();
		/*
		 * System.out
		 * .println("Total number of iterations in UnweightedLineRank-->" +
		 * ((job.getIntCounterResult(V2_SrcIncWithV1.ACCUM_LOCAL_ITERATIONS) /
		 * dop) - 1)); System.out.println("RunTime-->" + ((job.getNetRuntime()))
		 * + "sec");
		 */
	}

	/**
	 * Convergence criterion to check the sum of the differences of edge scores
	 * is less than a threshold at the end of each iteration
	 */
	public static final class L1_NormConvergence implements
			ConvergenceCriterion<DoubleValue> {
		@Override
		public boolean isConverged(int iteration, DoubleValue value) {
			double diff = value.getValue();
			// System.out.println("inside check");
			return diff < Config.episolon();
		}
	}

	/**
	 * Joins the current edge score vector v with previous iteration's vector v
	 * to find the differences
	 */
	public static final class L1_NormDiff
			extends
			RichJoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {
		public static final String AGGREGATOR_NAME = "linerank.aggregator";
		private DoubleSumAggregator agg;

		public void open(Configuration parameters) {
			agg = getIterationRuntimeContext().getIterationAggregator(
					AGGREGATOR_NAME);
		}

		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> current,
				Tuple2<Long, Double> prev) throws Exception {
			// System.out.println(current.f0+"---prev-->"+prev.f1+"   -   "+current.f1+"  at step-->"+getIterationRuntimeContext().getSuperstepNumber());
			agg.aggregate(Math.abs(prev.f1 - current.f1));
			return current;
		}
	}

	/**
	 * An intermediate join operation in the iteration between V1 vector and
	 * S(G) on vector index of V1 and edgeId of S(G)
	 */
	@ConstantFieldsFirst("1 -> 1")
	@ConstantFieldsSecond("1 -> 0")
	public static final class V2_SrcIncWithV1
			extends
			RichJoinFunction<Tuple2<Long, Double>, Tuple2<Long, Long>, Tuple2<Long, Double>> {
		public static final String ACCUM_LOCAL_ITERATIONS = "accum.local.iterations";
		private IntCounter localIterations = new IntCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(ACCUM_LOCAL_ITERATIONS,
					localIterations);
			localIterations.add(1);
		}

		// v2 <- S(G)T v1;

		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> iteration_v1,
				Tuple2<Long, Long> srcIncMat) throws Exception {
			Tuple2<Long, Double> result = new Tuple2<Long, Double>();
			result.f0 = srcIncMat.f1;
			result.f1 = (iteration_v1.f1);
			// System.out.println("-srcinc->"+result.f0+"---->"+iteration_v1.f1+"  *  "+srcIncMat.f2);
			return result;
		}
	}

	/**
	 * A join operation to get the product of two vectors (d and v)
	 */
	@ConstantFieldsFirst("0->0")
	public static final class V1_HadamardProduct
			implements
			JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

		// v1 <- dv Hadamard product
		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> v,
				Tuple2<Long, Double> d) throws Exception {
			Tuple2<Long, Double> result = new Tuple2<Long, Double>();
			result.f0 = v.f0;
			result.f1 = (d.f1) * v.f1;
			// System.err.println("Iteration "+getIterationRuntimeContext().getSuperstepNumber());
			// System.out.println("prod -->"+d.f1+"   *   "+v.f1);
			return result;
		}
	}

	/**
	 * A map function to randomly initialize edge score vector. An initial value
	 * to all the edges in the graph
	 */
	@ConstantFields("0")
	public static final class InitializeRandomVector extends
			RichMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

		private long numArc;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			numArc = getRuntimeContext().<Long> getBroadcastVariable("numArc")
					.get(0);
		}

		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, Double> value)
				throws Exception {
			// random initial vector of size m
			final double fracNumEdges = 1 / numArc;

			Tuple2<Long, Double> v = new Tuple2<Long, Double>();
			v.f0 = value.f0;
			v.f1 = fracNumEdges;
			// System.out.println("random vector -->"+v.f0+"    "+v.f1);
			return v;
		}
	}

	/**
	 * Reused Join function for getting the product in the matrix vector
	 * multiplication (the sum followed by this product is achieved by using
	 * group by aggregate)
	 */
	@ConstantFieldsFirst("1->1")
	@ConstantFieldsSecond("0->0")
	public static final class MatrixVectorMul
			implements
			JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Long>, Tuple2<Long, Double>> {
		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> first,
				Tuple2<Long, Long> second) throws Exception {
			Tuple2<Long, Double> result = new Tuple2<Long, Double>();

			result.f0 = second.f0;
			result.f1 = first.f1;

			return result;
		}
	}

	@ConstantFieldsSecond("0 -> 0")
	public static final class V3_TarIncWithV2
			extends
			RichJoinFunction<Tuple2<Long, Double>, Tuple2<Long, Long>, Tuple2<Long, Double>> {
		private long numArc;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			numArc = getRuntimeContext().<Long> getBroadcastVariable("numArc")
					.get(0);
		}

		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> first,
				Tuple2<Long, Long> second) throws Exception {
			double randomJump = (1 - Config.dampingFactor()) / numArc;
			Tuple2<Long, Double> result = new Tuple2<Long, Double>();

			result.f0 = second.f0;
			result.f1 = (first.f1) * Config.dampingFactor() + randomJump;
			return result;

		}
	}

	public static final class DampingMapper extends
			RichMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

		private final double dampening;
		private final double randomJump;

		public DampingMapper(double dampening, double numEdges) {
			this.dampening = dampening;
			this.randomJump = (1 - dampening) / numEdges;
		}

		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
			value.f1 = (value.f1 * dampening) + randomJump;
			return value;
		}
	}

	/*	*//**
	 * A reduce operation used as a part of normalization
	 */
	/*
	 * public static final class MatrixToVector extends
	 * GroupReduceFunction<Tuple3<Long, Long, Double>, Tuple2<Long, Double>> {
	 * 
	 * @Override public void reduce(Iterator<Tuple3<Long, Long, Double>> values,
	 * Collector<Tuple2<Long, Double>> out) throws Exception { Tuple2<Long,
	 * Double> toVector = new Tuple2<Long, Double>(); Double sum = 0.0; boolean
	 * flag = false; Long key = null; while (values.hasNext()) { Tuple3<Long,
	 * Long, Double> sameRowValues = values.next(); if (!flag) { key =
	 * sameRowValues.f1; flag = true; } sum = sum + sameRowValues.f2; }
	 * 
	 * toVector.f0 = key; toVector.f1 = sum; //
	 * System.out.println("d1 -->"+toVector.f0+"  "+toVector.f1);
	 * out.collect(toVector); }
	 * 
	 * }
	 */

	/**
	 * A reduce operation used as a part of normalization
	 */
	public static final class MatrixToVector implements
			GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values,
				Collector<Tuple2<Long, Double>> out) throws Exception {
			Tuple2<Long, Double> toVector = new Tuple2<Long, Double>();
			Double sum = 0.0;
			boolean flag = false;
			Iterator<Tuple2<Long, Long>> iterator = values.iterator();
			Long key = null;

			while (iterator.hasNext()) {
				Tuple2<Long, Long> sameRowValues = iterator.next();
				if (!flag) {
					key = sameRowValues.f1;
					flag = true;
				}
				sum = sum + 1;
			}

			toVector.f0 = key;
			toVector.f1 = sum;
			// System.out.println("d1 -->"+toVector.f0+"  "+toVector.f1);
			out.collect(toVector);

		}

	}

	/**
	 * A join function for to compute the partial (product part in matrix vector
	 * multiplication) aggregation
	 */
	@ConstantFieldsFirst("1 -> 0")
	@ConstantFieldsSecond("1 -> 1")
	public static final class AddSrcWithTar
			implements
			JoinFunction<Tuple2<Long, Long>, Tuple2<Long, Double>, Tuple2<Long, Double>> {
		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Long> matrix,
				Tuple2<Long, Double> vector) throws Exception {
			Tuple2<Long, Double> transposed = new Tuple2<Long, Double>();
			transposed.f0 = matrix.f1;
			transposed.f1 = vector.f1;
			return transposed;
		}
	}

	/**
	 * A final join to compute full aggregation
	 */
	@ConstantFieldsFirst("0")
	public static final class EdgeScoreAggregation
			implements
			JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> first,
				Tuple2<Long, Double> second) throws Exception {
			Tuple2<Long, Double> res = new Tuple2<Long, Double>();
			res.f0 = first.f0;
			res.f1 = first.f1 + second.f1;
			// System.out.println("Line Rank of " + res.f0 + " -> " + res.f1);
			return res;
		}

	}

	public static class CountArcs implements
			GroupReduceFunction<Tuple2<Long, Long>, Long> {
		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> arcs,
				Collector<Long> collector) throws Exception {
			collector.collect(new Long(Iterables.size(arcs)));
		}
	}

	public static class IncidenceArcReader implements
			FlatMapFunction<String, Tuple2<Long, Long>> {

		private static final Pattern SEPARATOR = Pattern
				.compile("[ \t,]");

		@Override
		public void flatMap(String s, Collector<Tuple2<Long, Long>> collector)
				throws Exception {
			if (!s.startsWith("%")) {

				String[] tokens = SEPARATOR.split(s);
				long arcId = Long.parseLong(tokens[0]);
				long node = Long.parseLong(tokens[1]);
				// long weight = Long.parseLong(tokens[2]);
				collector.collect(new Tuple2<Long, Long>(arcId, node));

			}
		}
	}
}
