package betweennessCentrality;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import betweennessCentrality.SourceIncidence.ArcReader;

/* Line Graph Decomposition
 * Generate out-arcs for sparse Line Graph
 * 
 * 1->2, 1->3 will generate
edge1, 2
edge2, 3 
 */

public class TargetIncidence {

	public static void main(String[] args) throws Exception {
		
		/* * if (args.length < 4) { System.err .println(
		 * "Usage: LineRank <DOP> <edgeInputPath> <tar outputPath> <delimiter>"
		 * ); return; }
		 

		final int dop = Integer.parseInt(args[0]);
		final String edgeInputPath = args[1];
		final String outputPath = args[2];
		String fieldDelimiter = CentralityUtil.TAB_DELIM;
		if (args.length > 3) {
			fieldDelimiter = (args[3]);
		}

		char delim = CentralityUtil.checkDelim(fieldDelimiter);
*/
		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();
		//env.setDegreeOfParallelism(dop);
		
		DataSource<String> inputArc = env
				.readTextFile(Config.pathToSmallArcs());

		/* Convert the input to edges, consisting of (source, target) */
		DataSet<Tuple2<Long, Long>> arcs = inputArc.flatMap(new ArcReader());
		
		DataSet<Tuple3<Long, Long, Double>> tarIncMat = arcs.map(new TargetIncMatrix())
				.name("T(G)");

		tarIncMat.writeAsCsv(Config.outArcs(), "\n", "\t",
				FileSystem.WriteMode.OVERWRITE);
		JobExecutionResult job = env.execute();
		/*System.out.println("RunTime-->" + ((job.getNetRuntime() / 1000))
				+ "sec");*/
	}

	/**
	 * Reads input edge file <srcId,tarId,weight>. Generates edgeId using
	 * accumulators and emits <edgeId, tarId, weight>
	 */
	public static final class TargetIncMatrix extends
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
			Tuple3<Long, Long, Double> tarInc = new Tuple3<Long, Long, Double>();
			tarInc.f0 = num_vertices.getLocalValue().longValue();
			tarInc.f1 = value.f1;
			tarInc.f2 = 1.0;
			// System.out.println("Tar-->"+tarInc.f0+" "+tarInc.f1+" "+tarInc.f2);
			return tarInc;
		}
	}

}
