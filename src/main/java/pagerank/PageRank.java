package pagerank;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.*;

/**
 * Created by philip on 13.01.15.
 */
public class PageRank {

    private static final double beta = 0.85;
    private static final double epsilon = 0.0001;
    private static final int maxIterations = 10;

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Read the input files - pages and links
        DataSource<String> inputPages = env.readTextFile(Config.pathToSmallPages());
        DataSet<Tuple1<Long>> pages = inputPages.flatMap(new PageReader());

        DataSource<String> inputLinks = env.readTextFile(Config.pathToSmallLinks());
        DataSet<Tuple2<Long, Long>> links = inputLinks.flatMap(new LinkReader());

        // Get the total count of pages
        DataSet<Long> numPages = pages.reduceGroup(new CountPages());

        // Find sinks
        DataSet<Tuple1<Long>> noOutgoingLinks = pages.flatMap(new FindSinks()).withBroadcastSet(links.project(0).types(Long.class).distinct(), "pages");

        // Point sinks to all other nodes
        DataSet<Tuple2<Long, Long>> sinksToAll = noOutgoingLinks.flatMap(new PointToAllOther()).withBroadcastSet(pages, "pages");

        // Assign the initial rank to every page - 1 / numPages
        DataSet<Tuple2<Long, Double>> pagesRanked = pages.map(new InitialRanking()).withBroadcastSet(numPages, "numPages");

        // Encode sparse adjacency matrix to a list
        DataSet<Tuple2<Long, Long[]>> sparseMatrix = links.union(sinksToAll).groupBy(0).reduceGroup(new BuildList());

        // Start iteration - Not using DeltaIteration since the whole DataSet is recomputed
        IterativeDataSet<Tuple2<Long, Double>> iterationSet = pagesRanked.iterate(maxIterations);

        DataSet<Tuple2<Long, Double>> pageRank = iterationSet.

                // Iteratively join the iterationSet with the sparseMatrix
                join(sparseMatrix).where(0).equalTo(0).flatMap(new DistributePageRank()).groupBy(0).sum(1).

                // To implement the random teleport behaviour we recompute the pageRank
                // and applying a function on each PageRank which is given by
                // beta * pageRank + ((1 - beta) / numPages)
                map(new RandomTeleport()).withBroadcastSet(numPages, "numPages");

        DataSet<Tuple2<Long, Double>> results = iterationSet.closeWith(
                pageRank,
                pageRank.join(iterationSet).where(0).equalTo(0).filter(new ConvergenceCondition()));

        // results.writeAsText(Config.pathToPageRank(), FileSystem.WriteMode.OVERWRITE);

        results.sum(1).print();

        env.execute();
    }

    // The Reader classes
    public static class PageReader implements FlatMapFunction<String, Tuple1<Long>> {

        @Override
        public void flatMap(String s, Collector<Tuple1<Long>> collector) throws Exception {
            String[] token = s.split("\t");
            collector.collect(new Tuple1<Long>(Long.parseLong(token[1])));
        }
    }

    public static class LinkReader implements FlatMapFunction<String, Tuple2<Long, Long>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<Long, Long>> collector) throws Exception {
            String[] token = s.split("\t");
            long source = Long.parseLong(token[0]);
            long target = Long.parseLong(token[1]);
            collector.collect(new Tuple2<Long, Long>(source, target));
        }
    }

    // Class to count the entries within the pages set in order to retrieve the pages count numPages
    public static class CountPages implements GroupReduceFunction<Tuple1<Long>, Long> {
        @Override
        public void reduce(Iterable<Tuple1<Long>> pages, Collector<Long> collector) throws Exception {
            collector.collect(new Long(Iterables.size(pages)));
        }
    }

    // Class to find sinks within the graph
    public static class FindSinks extends RichFlatMapFunction<Tuple1<Long>, Tuple1<Long>> {
        private HashSet<Long> withOutgoingLinks = new HashSet<Long>();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            Iterator pages = getRuntimeContext().getBroadcastVariable("pages").iterator();
            while(pages.hasNext()) {
                Tuple1<Long> tmp = (Tuple1<Long>) pages.next();
                withOutgoingLinks.add(tmp.f0);
            }
        }

        @Override
        public void flatMap(Tuple1<Long> page, Collector<Tuple1<Long>> collector) throws Exception {
            if(!withOutgoingLinks.contains(page.f0)) {
                collector.collect(page);
            }
        }
    }

    // Class that points all sinks to all other nodes
    public static class PointToAllOther extends RichFlatMapFunction<Tuple1<Long>, Tuple2<Long, Long>> {
        private ArrayList<Long> allPages = new ArrayList<Long>();

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            Iterator pages = getRuntimeContext().getBroadcastVariable("pages").iterator();
            while(pages.hasNext()) {
                Tuple1<Long> tmp = (Tuple1<Long>) pages.next();
                allPages.add(tmp.f0);
            }
        }

        @Override
        public void flatMap(Tuple1<Long> sink, Collector<Tuple2<Long, Long>> collector) throws Exception {
            for (int i=0; i < allPages.size(); i++) {
                collector.collect(new Tuple2<Long, Long>(sink.f0, allPages.get(i)));
            }
        }
    }

    // Class that initially assigns ranks to the pages
    public static class InitialRanking extends RichMapFunction<Tuple1<Long>, Tuple2<Long, Double>> {
        private long numPages = 0L;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            numPages = getRuntimeContext().<Long> getBroadcastVariable("numPages").get(0);
        }

        @Override
        public Tuple2<Long, Double> map(Tuple1<Long> page) throws Exception {
            return new Tuple2<Long, Double>(page.f0, 1.0d / numPages);
        }
    }

    // Class to encode the sparse adjacency matrix into a list
    public static class BuildList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

        @Override
        public void reduce(Iterable<Tuple2<Long, Long>> iterable, Collector<Tuple2<Long, Long[]>> collector) throws Exception {
            long id = 0L;
            ArrayList<Long> outgoingEdges = new ArrayList<Long>();

            Iterator<Tuple2<Long, Long>> i = iterable.iterator();
            while(i.hasNext()) {
                Tuple2<Long, Long> entry = i.next();
                id = entry.f0;
                outgoingEdges.add(entry.f1);
            }
            collector.collect(new Tuple2<Long, Long[]>(id, outgoingEdges.toArray(new Long[outgoingEdges.size()])));
        }
    }

    // Class to distribute the rank of a page to the linked pages, a so called "vote" is computed as rank / #(outgoing edges)
    public static class DistributePageRank implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {

        @Override
        public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple2<Long, Double>> collector) throws Exception {
            Long[] linkedPages = value.f1.f1;
            double newRank = value.f0.f1 / linkedPages.length;
            for(int i=0; i < linkedPages.length; i++)
                collector.collect(new Tuple2<Long, Double>(linkedPages[i], newRank));
        }
    }

    // Class to implement the random teleport behaviour
    public static class RandomTeleport extends RichMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {
        private long numPages = 0L;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            numPages = getRuntimeContext().<Long> getBroadcastVariable("numPages").get(0);
        }

        @Override
        public Tuple2<Long, Double> map(Tuple2<Long, Double> value) throws Exception {
            value.f1 = (beta * value.f1) + ((1 - beta) / numPages);
            return value;
        }
    }

    // Implemenation of the convergence condition
    public static class ConvergenceCondition implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

        @Override
        public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) throws Exception {
            return Math.abs(value.f0.f1 - value.f1.f1) > epsilon;
        }
    }
}
