import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import pl.edu.mimuw.students.mw336071.commons.MinhashingOutput;
import pl.edu.mimuw.students.mw336071.commons.ShingleOutput;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by m on 11.03.15.
 */
public class NoMapReduceResolver {
	private static final Logger logger = Logger.getLogger(NoMapReduceResolver.class);

	public static void main(String[] args) throws Exception {
		int hashFunctionNumber = 100; // equal to output signature size for each document
		if (args.length == 1) {
			hashFunctionNumber = Integer.valueOf(args[0]);
		}else {
			logger.warn("Invalid arguments number. Set default.");
		}

		String shingleResultFilePath = "shingle-output.json";
		String minhashResultFilePath = "minhash-output.json";

		FileInputStream fis = new FileInputStream(shingleResultFilePath);
		ShingleOutput input = new ObjectMapper().readValue(fis, ShingleOutput.class);
		long mod = input.getShingleSumNumber();

		Minhashing minhashing = new Minhashing(hashFunctionNumber);
		MinhashingOutput output = new MinhashingOutput();

		// iterate through all documents
		Map<String, List<Long>> docNameToSignature =
				new HashMap<String, List<Long>>(input.getFileNameToShingleIds().size());
		for (Map.Entry<String, List<Long>> entry : input.getFileNameToShingleIds().entrySet()) {
			String docName = entry.getKey();
			List<Long> shingleIds = entry.getValue();
			List<Long> signature = minhashing.createSignature(shingleIds, mod);
			docNameToSignature.put(docName, signature);
		}
		output.setDocNameToSignature(docNameToSignature);

		// STORE RESULTS
		FileOutputStream fos = new FileOutputStream(minhashResultFilePath);
		new ObjectMapper().writeValue(fos, output);
	}
}
