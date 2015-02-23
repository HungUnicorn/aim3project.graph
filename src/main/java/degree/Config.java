package degree;

public class Config {

	private Config() {
	}

	private static final String INPUT_PATH = "/home/hung/aim3project.graph/src/test/resources/";
	private static final String OUTPUT_PATH = "/home/hung/aim3project.graph/src/test/resources/analysis/";

	// Example files contain a graph with 106 nodes and 141 arcs
	public static String pathToSmallArcs() {
		return INPUT_PATH + "smallGraph/example_arcs";
	}

	// Example nodes
	public static String pathToSmallIndex() {
		return INPUT_PATH + "smallGraph/example_index";
	}

	public static String pathToBigArcs() {
		return "/home/hung/Downloads/sd-arc";
	}

	public static String pathToBigIndex() {
		return "/home/hung/Downloads/index-00000";
	}

	
	public static String outputPath() {
		return OUTPUT_PATH + "/tmp/statistics/";
	}

	public static String pathToOutDegreeDistritbuion() {
		return OUTPUT_PATH + "outDegreeDistribution";
	}

	public static long topK() {
		return 5;
	}

	public static long avgOutDegree() {
		return (long) 20;
	}
		
}
