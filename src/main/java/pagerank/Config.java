package pagerank;

/**
 * Created by philip on 20.01.15.
 */
public class Config {

    private Config() { }

    private static final String INPUT_PATH = "/home/philip/Dokumente/aim3project.graph/src/test/resources/";
    private static final String OUTPUT_PATH = "/home/philip/Dokumente/aim3project.graph/src/test/resources/analysis/";

    public static String pathToSmallPages() {
        return INPUT_PATH + "smallGraph/example_index";
    }
    public static String pathToSmallLinks() { return INPUT_PATH + "smallGraph/example_arcs"; }

    public static String pathToPageRank() {
        return OUTPUT_PATH + "PageRank";
    }
}
