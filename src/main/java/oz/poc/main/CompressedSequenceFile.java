package oz.poc.main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;

import org.springframework.util.StringUtils;

public class CompressedSequenceFile {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		System.out.println("######## Starting task #########");
		System.out.println("Arguments: " + Arrays.asList(args) + " " + args.length);
		String arguments = Arrays.asList(args).get(0);
		String[] argumentsParsed = StringUtils.delimitedListToStringArray(arguments, ",");
		
		String methodName = argumentsParsed[0];
		
		CompressedSequenceFile testHarness = new CompressedSequenceFile();
		if (methodName.equalsIgnoreCase("prepareFile")){
			int value = Integer.parseInt(argumentsParsed[1]);
			testHarness.prepareFile(value);
		}
		
		
		
	}
	
	public void prepareFile(int value) throws Exception{
		BufferedWriter bw = new BufferedWriter(new FileWriter("source/source.txt"));
		for (int i = 0; i < value; i++) {
			BufferedReader br = new BufferedReader(new FileReader("source/small-source.txt"));
			for (int j = 0; j < 80000; j++) {
				bw.write(br.readLine() + "\n");		
			}
			br.close();
		}
		System.out.println("Generated " + (80000*value) + " records file: ");
		bw.close();
	}

}
