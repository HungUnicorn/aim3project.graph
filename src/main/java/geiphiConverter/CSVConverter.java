package geiphiConverter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Stack;

// Convert Tab to CSV for importing into Geiphi
// Used Geiphi's calculation to check the correctness of implemented algorithms

public class CSVConverter{

	static String line; // to read in each line
	static Stack<String> stack; // to store each line of data
	static File outputFile; // output file to be returned
	static BufferedReader br; // used to read in bytes from file
	static FileWriter writer; // used to write bytes to file
	static String fileName = "example_arcs";
	
	public static void main(String args[]){
		String inputFilePath = "/home/hung/"+ fileName;
		File inputFile = new File(inputFilePath);
		CSVConverter c = new CSVConverter();		
		c.convertFromTabToCSV(inputFile);
		System.out.print("Converted end");
		
	}
	
	private static File convert(File inputFile, char from, char to) {
		outputFile = new File("/home/hung/" + fileName +".csv");

		try {
			stack = new Stack<String>();
			br = new BufferedReader(new FileReader(inputFile));
			writer = (new FileWriter(outputFile));
			while ((line = br.readLine()) != null)
				stack.push(line.replace(from, to));
			while (!stack.isEmpty()) {
				writer.write((String) stack.pop());
				if (!stack.empty())
					writer.write("\n");
			}

		} catch (FileNotFoundException a) {
			System.out.println("Could not open file");
			a.printStackTrace();
		} catch (IOException b) {
			System.out.println("IOException occured");
			b.printStackTrace();
		} catch (Exception c) {
			c.printStackTrace();
		} finally {
			try {
				br.close();
				writer.close();
			} catch (IOException d) {
				System.out.println("IOException, unable to close streams");
				d.printStackTrace();
			}
		}

		return outputFile;
	}

	public void convertFromCSVToTab(File inputFile) {
		convert(inputFile, ',', '\t');
	}

	public void convertFromTabToCSV(File inputFile) {
		convert(inputFile, '\t', ',');
	}
}
