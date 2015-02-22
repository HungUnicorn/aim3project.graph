package connectivity;

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
			return INPUT_PATH + "smallGraph/example_index";
		}

	  public static String pathToTestArcs() {
			return INPUT_PATH + "smallGraph/arcs";
		}
	  
	  public static String pathToTestNodes() {
			return INPUT_PATH + "smallGraph/index";
		}	 

	  public static String pathToWeakConnectedComponents() {
			return OUTPUT_PATH + "WeakConnectedComponents";
		}    	  	  
	}