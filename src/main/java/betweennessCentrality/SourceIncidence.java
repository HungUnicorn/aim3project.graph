package betweennessCentrality;

import java.util.regex.Pattern;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.Collector;

/* Line Graph Decomposition
 * Generate the in-arcs file for sparse Line Graph 
 1->2, 1->3 will generate
 edge1, 1
 edge2, 2 
 */

public class SourceIncidence {

	private static String argPathToArc = "";
	private static String argPathOut = "";

	public static void main(String[] args) throws Exception {
		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		
		DataSource<String> inputArc = env
				.readTextFile(argPathToArc);

		/* Convert the input to arcs, consisting of (source, target) */
		DataSet<Tuple2<Long, Long>> arcs = inputArc.flatMap(new ArcReader());

		DataSet<Tuple3<Long, Long, Double>> srcIncMat = arcs.map(
				new SourceIncMatrix()).name("S(G)");

		srcIncMat.writeAsCsv(argPathOut, "\n", "\t",
				FileSystem.WriteMode.OVERWRITE);

		env.execute();		
	}

	/*
	 * Reads input edge file <srcId,tarId,weight>. Generates edgeId using
	 * accumulators and emits <edgeId, srcId, weight>
	 */
	public static final class SourceIncMatrix extends
			RichMapFunction<Tuple2<Long, Long>, Tuple3<Long, Long, Double>> {
		private static final long serialVersionUID = 1L;
		public static final String ACCUM_NUM_LINES = "accumulator.num-lines";
		private LongCounter num_vertices = new LongCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(ACCUM_NUM_LINES,
					this.num_vertices);
		}

		@Override
		public Tuple3<Long, Long, Double> map(Tuple2<Long, Long> value)
				throws Exception {
			num_vertices.add(1L);
			Tuple3<Long, Long, Double> srcInc = new Tuple3<Long, Long, Double>();
			srcInc.f0 = num_vertices.getLocalValue().longValue();
			srcInc.f1 = value.f0;
			srcInc.f2 = 1.0;
			// System.out.println("Src-->"+srcInc.f0+" "+srcInc.f1+" "+srcInc.f2);
			return srcInc;
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
