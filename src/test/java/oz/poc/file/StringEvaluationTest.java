package oz.poc.file;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.reflect.Method;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringEvaluationTest {
	
	Pattern p = Pattern.compile("ge-14/0/0.0:14.0.0.40:40 -> 14.0.0.41:41");

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		final BufferedReader br = new BufferedReader(new FileReader("/Users/oleg/Documents/workspace/hadoop-stuff/source/small-source.txt"));
		StringBuffer buffer = new StringBuffer();
		for (int i = 0; i < 10000; i++) {
			buffer.append(br.readLine() + "\n");
		}
		String batch = buffer.toString();
		br.close();
		
		StringEvaluationTest test = new StringEvaluationTest();
		System.out.println("### Testing one line at the time");
		test.testLine(batch);
		test.testLine(batch);
		test.testLine(batch);
		test.testLine(batch);
		test.testLine(batch);
		test.testLine(batch);
		test.testLine(batch);
		test.testLine(batch);
		test.testLine(batch);
		test.testLine(batch);
		System.out.println("### Done\n");
		System.out.println("### Testing batch at the time");
		test.testBatch(batch);
		test.testBatch(batch);
		test.testBatch(batch);
		test.testBatch(batch);
		test.testBatch(batch);
		test.testBatch(batch);
		test.testBatch(batch);
		test.testBatch(batch);
		test.testBatch(batch);
		test.testBatch(batch);
		System.out.println("### Done\n");
	}
	
	public void testLine(String batch) throws Exception {
		Method method = StringEvaluationTest.class.getDeclaredMethod("evaluateString", String.class);
		
		StringTokenizer tokenizer = new StringTokenizer(batch, "\n");
		long start = System.nanoTime();
		
		while (tokenizer.hasMoreTokens()) {		
			method.invoke(this, tokenizer.nextToken());
		}
		long stop = System.nanoTime();
		System.out.println(stop - start);
	}
	
	public void testBatch(String batch) throws Exception {
		Method method = StringEvaluationTest.class.getDeclaredMethod("evaluateString", String.class);
		
		long start = System.nanoTime();
		method.invoke(this, batch);
		long stop = System.nanoTime();
		System.out.println(stop - start);
	}

	public void evaluateString(String line){
		Matcher m = p.matcher(line);
		while (m.find()) {
			//System.out.println("found");
		}
	}
}
