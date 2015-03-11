import com.google.common.base.Splitter;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * 1. Read documents from local filesystem.
 * 2. Create shingle for each document.
 * 3. Take sum of shingle, map each unique shingle to id.
 * 4. Map documents shingle to out ids.
 * 5. Store shingle ids to file.
 * <p/>
 * Created by m on 11.03.15.
 */
public class NoMapReduceResolver {
	private static final Logger logger = Logger.getLogger(NoMapReduceResolver.class);

	static String readFile(String path) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, Charset.defaultCharset());
	}

	public static void main(String[] args) throws Exception {
		//String dirPath = "sample_input";
		String dirPath = "../input2";
		int shingleSize = 3;
		String shingleResultFilePath = "shingle-output.json";
		if ( args.length == 2 ) {
			dirPath = args[0];
			shingleSize = Integer.parseInt(args[1]);
		} else {
			logger.warn("Invalid arguments number. Set default.");
		}

		Shingler shingler = new Shingler(shingleSize);
		Set<String> shingleSum = new HashSet<String>();

		// READ FILES
		File[] files = new File(dirPath).listFiles();
		Map<File, List<String>> fileToShingles = new HashMap<File, List<String>>(files.length);

		for (File file : files) {
			logger.info("Reading file: " + file.getAbsolutePath());
			if (!file.canRead()) {
				logger.warn("Can't read file.");
			}
			String document = readFile(file.getAbsolutePath());
			List<String> shingle = shingler.create(document);
			if (shingle == null || shingle.isEmpty()) {
				logger.error("Cannot create shingle for file: " + file.getName());
				continue;
			}
			fileToShingles.put(file, shingle);
			shingleSum.addAll(shingle);
		}

		// MAP EACH SHINGLE TO OUR INTERNAL ID
		List<String> sortedShinglesSum = new ArrayList<String>(shingleSum);
		Collections.sort(sortedShinglesSum);
		Map<String, List<Long>> fileNameToShingleIds = new HashMap<String, List<Long>>(files.length);
		for (Map.Entry<File, List<String>> entry : fileToShingles.entrySet()) {
			String fileName = entry.getKey().getName();
			List<String> shingle = entry.getValue();
			List<Long> shingleIds = new ArrayList<Long>(shingle.size());
			for(String sh : shingle) {
				long shingleId = Collections.binarySearch(sortedShinglesSum, sh) + 1;
				shingleIds.add(shingleId);
			}
			fileNameToShingleIds.put(fileName, shingleIds);
		}

		// SERIALIZE RESULT
		/*FileOutputStream fos = new FileOutputStream(shingleResultFilePath);
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(sortedShinglesSum.size());
		oos.writeObject(fileNameToShingleIds);
		oos.close();
		fos.close();*/
		/*rintWriter writer = new PrintWriter(shingleResultFilePath, Charset.defaultCharset().name());
		// store total number of shingles
		writer.println(sortedShinglesSum.size());
		writer.println(fileNameToShingleIds);
		writer.close();*/

		// STORE RESULTS
		ShingleOutput shingleOutput = new ShingleOutput(sortedShinglesSum.size(), fileNameToShingleIds);
		FileOutputStream fos = new FileOutputStream(shingleResultFilePath);	
		new ObjectMapper().writeValue(fos, shingleOutput);
	}
}