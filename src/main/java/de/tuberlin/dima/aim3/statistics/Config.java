package de.tuberlin.dima.aim3.statistics;

public class Config {

  private Config() {}

  private static final String INPUT_PATH = "/home/hung/aim3project.graph/src/test/resources/";
  private static final String OUTPUT_PATH = "/home/hung/aim3project.graph/src/test/resources/analysis/";
  
  // Example files contain a graph with 106 nodes and 141 arcs
  public static String pathToSmallArcs() {
		return INPUT_PATH + "smallGraph/example_arcs";
	}
  // Example nodes
  public static String pathToSmallIndex() {
		return INPUT_PATH + "example_index";
	}
  
	public static String pathToBigGraph() {
		return INPUT_PATH + "big.tab";
	}
	
  public static String pathToWebCommon() {
    return "/home/Downloads/";
  }

  public static String outputPath() {
    return OUTPUT_PATH+ "/tmp/statistics/";
  }

  public static String pathToOutDegreeDistritbuion() {
		return OUTPUT_PATH + "outDegreeDistribution";
	}
  
  public static long randomSeed() {
    return 0xdeadbeef;
  }
}
