package de.tuberlin.dima.aim3.statistics;

public class Config {

  private Config() {}

  private static final String INPUT_PATH = "/home/hung/aim3project.graph/src/test/resources/";
  private static final String OUTPUT_PATH = "/home/hung/aim3project.graph/src/test/resources/analysis/";
  
  public static String pathToSmallGraph() {
		return INPUT_PATH + "small.tab";
	}

	public static String pathToBigGraph() {
		return INPUT_PATH + "big.tab";
	}
	
  public static String pathToWebCommon() {
    return "/home/Downloads/";
  }

  public static String outputPath() {
    return "/tmp/statistics/";
  }

  public static String pathToDegreeDistritbuion() {
		return OUTPUT_PATH + "degreeDistribution";
	}
  
  public static long randomSeed() {
    return 0xdeadbeef;
  }
}
